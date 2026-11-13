"""Shared helpers for the NOAA GoMOFS virtual-Icechunk builders.

Source: s3://noaa-nos-ofs-pds/gomofs/netcdf/ (us-east-1, anonymous). Four cycles a day (00, 06,
12, 18 UTC), each a 72 h forecast, plus two flavours of file used here:

  3d  ``fields``  3-D + 2-D fields, 3-hourly:  n003, n006, f003 ... f072   (26 files/cycle)
  2d  ``2ds``     surface fields, hourly:      n001 ... n006, f001 ... f072 (78 files/cycle)

``n006`` is valid at the cycle time, ``n003`` 3 h before it, ``f0NN`` NN hours after it.
Only the files named ``gomofs.tHHz.YYYYMMDD.<family>.<n|f>NNN.nc`` are used (2024-11-07 on);
the older ``nos.gomofs.*`` names (2024-07 to 2024-10) are deliberately ignored.
"""

import datetime as dt
import re
from concurrent.futures import ThreadPoolExecutor

import fsspec
import icechunk
import numpy as np
import pandas as pd
import xarray as xr
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import S3Store
from virtualizarr import open_virtual_dataset
from virtualizarr.parsers import HDFParser

SRC_BUCKET, SRC_REGION = "noaa-nos-ofs-pds", "us-east-1"
SRC = f"s3://{SRC_BUCKET}"
FIRST_DAY = dt.date(2024, 12, 10)          # series start: ~30 days after the oldest surviving file (NOAA prunes from the front)
TREE_FROM = dt.date(2024, 11, 19)          # from here the YYYY/MM/DD tree exists too
TIME_DIM = "ocean_time"

FAMILIES = {
    "3d": {"family": "fields", "interval_h": 3, "nowcast": ("n003", "n006"),
           "per_cycle": 26},
    "2d": {"family": "2ds", "interval_h": 1,
           "nowcast": tuple(f"n{i:03d}" for i in range(1, 7)), "per_cycle": 78},
}

_NAME = re.compile(r"gomofs\.t(\d\d)z\.(\d{8})\.(fields|2ds)\.([nf])(\d{3})\.nc$")
_fs = fsspec.filesystem("s3", anon=True)


def _lead(kind, n):
    """Hours from the cycle time to the valid time: n006 -> 0, n003 -> -3, f003 -> +3."""
    return n - 6 if kind == "n" else n


def list_cycles(variant, start, end, need_forecast=True):
    """Complete cycles between two dates (inclusive), as {cycle_time: [(lead_h, kind, key)]}.

    A cycle is keyed by the date in its file names and its ``tHHz``. Where a file exists both in
    the YYYY/MM/DD tree and in the flat YYYYMM folder the tree copy wins. Cycles missing any
    file are returned in the second value instead, so callers can report rather than guess.
    With ``need_forecast=False`` a cycle is complete once all its nowcast files are present
    (between 2024-11-07 and 2025-12-19 the archive holds nowcast files only).
    """
    spec = FAMILIES[variant]
    found = {}
    day = start
    while day <= end:
        dirs = [f"{SRC_BUCKET}/gomofs/netcdf/{day:%Y/%m/%d}"]
        if day <= TREE_FROM:     # the tree starts at the 18Z cycle of TREE_FROM; the rest is flat
            dirs.insert(0, f"{SRC_BUCKET}/gomofs/netcdf/{day:%Y%m}")   # tree copy, listed last, wins
        for d in dirs:
            for key in _fs.glob(f"{d}/gomofs.t??z.{day:%Y%m%d}.{spec['family']}.*.nc"):
                m = _NAME.search(key)
                h, ymd, _fam, kind, n = m.groups()
                cyc = dt.datetime.strptime(ymd + h, "%Y%m%d%H")
                found.setdefault(cyc, {})[(kind, int(n))] = key
        day += dt.timedelta(days=1)

    complete, partial = {}, {}
    for cyc, files in sorted(found.items()):
        rows = sorted((_lead(k, n), k, key) for (k, n), key in files.items())
        have_nowcast = {k for _, k, _ in rows if k == "n"} and \
            sum(1 for _, k, _ in rows if k == "n") == len(spec["nowcast"])
        ok = len(rows) == spec["per_cycle"] or (not need_forecast and have_nowcast)
        (complete if ok else partial)[cyc] = rows
    return complete, partial


def make_registry():
    return ObjectStoreRegistry({SRC: S3Store(bucket=SRC_BUCKET, region=SRC_REGION, skip_signature=True)})


def open_one(key, registry, parser=HDFParser()):
    ds = open_virtual_dataset(f"s3://{key}", registry=registry, parser=parser,
                              loadable_variables=[TIME_DIM], decode_times=True)
    return ds.drop_vars("ntimes", errors="ignore")      # per-file scalar; misleading once merged


def open_many(keys, registry, workers=16):
    with ThreadPoolExecutor(workers) as ex:
        return list(ex.map(lambda k: open_one(k, registry), keys))


def concat_time(parts):
    parts = sorted(parts, key=lambda d: d[TIME_DIM].values[0])
    return xr.concat(parts, dim=TIME_DIM, data_vars="minimal", coords="minimal",
                     compat="override", combine_attrs="override")


def fmrc_dataset(cycle_time, rows, registry):
    """One cycle as ``(time=1, step, ...)``: scalar forecast reference time + forecast period."""
    ds = concat_time(open_many([key for _, _, key in rows], registry))
    valid = pd.DatetimeIndex(ds[TIME_DIM].values)
    ref = pd.Timestamp(cycle_time)
    # the file's own valid times must agree with its name, or the step axis would lie
    expected = pd.DatetimeIndex([ref + pd.Timedelta(hours=lead) for lead, _, _ in rows])
    if not (valid == expected).all():
        raise ValueError(f"cycle {cycle_time}: valid times do not match file names")
    ds = ds.rename_dims({TIME_DIM: "step"}).drop_vars(TIME_DIM)
    ds = ds.assign_coords(step=("step", (valid - ref).values, {"standard_name": "forecast_period"}))
    # ``time`` goes only on the variables that vary with step; grid, masks and ROMS parameters
    # stay static (expand_dims on the whole dataset would give every one of them a time axis)
    varying = [v for v in ds.data_vars if "step" in ds[v].dims]
    static = ds.drop_vars(varying)
    ds = xr.merge([ds[varying].expand_dims(time=[np.datetime64(ref)]), static], compat="override",
                  combine_attrs="override")
    ds["time"].attrs.update(standard_name="forecast_reference_time")
    # Pin the encodings: left to xarray they depend on each cycle's own values ("days since" vs
    # "hours since"), and an appended cycle then decodes to the wrong time.
    ds["time"].encoding = {"units": "hours since 2024-01-01", "dtype": "int64"}
    ds["step"].encoding = {"units": "hours", "dtype": "int64"}
    return ds


def for_append(ds, dim):
    """Only the variables that have ``dim``: appending the static ones again rewrites their
    metadata, and virtualizarr refuses that for the scalar parameters."""
    drop = [v for v in ds.variables if dim not in ds[v].dims]
    return ds.drop_vars(drop)


def timeseries_dataset(rows, registry):
    """Files (already in valid-time order) as one flat ``time`` dimension."""
    ds = concat_time(open_many([key for _, _, key in rows], registry))
    ds = ds.rename({TIME_DIM: "time"})
    ds["time"].attrs.update(standard_name="time")
    ds["time"].encoding = {"units": "hours since 2024-01-01", "dtype": "int64"}   # see fmrc_dataset
    return ds


def timeseries_rows(variant, cycles, newest_forecast=True):
    """Files for the monotonic series: every cycle's nowcast files, plus the newest cycle's
    forecast.  Nowcasts of consecutive cycles tile the timeline with no overlap."""
    spec = FAMILIES[variant]
    rows = []
    last = max(cycles)
    for cyc, files in sorted(cycles.items()):
        full = len(files) == spec["per_cycle"]          # a half-uploaded forecast is not used
        for lead, kind, key in files:
            if kind == "n" or (newest_forecast and cyc == last and full):
                rows.append((cyc + dt.timedelta(hours=lead), kind, key))
    rows.sort()
    return rows


def repo_config():
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            url_prefix=f"{SRC}/",                      # trailing '/' is required
            store=icechunk.s3_store(region=SRC_REGION, anonymous=True),
        )
    )
    return config


AUTH = icechunk.containers_credentials({f"{SRC}/": icechunk.s3_anonymous_credentials()})


def storage(name, store_path=None):
    """Where store ``name`` (``timeseries-3d`` ...) lives: a local directory, or an S3 bucket
    named by ``GOMOFS_S3_*`` environment variables (Source Cooperative through its STS proxy in
    CI, any S3 bucket otherwise).  Credentials come from the environment (``from_env``)."""
    import os
    if store_path:
        return icechunk.local_filesystem_storage(f"{store_path}/{name}")
    bucket = os.environ.get("GOMOFS_S3_BUCKET")
    if not bucket:
        raise SystemExit("set GOMOFS_S3_BUCKET (or pass --store-path)")
    endpoint = os.environ.get("GOMOFS_S3_ENDPOINT") or None
    acl = os.environ.get("GOMOFS_S3_ACL", "")
    return icechunk.s3_storage(
        bucket=bucket, prefix=f"{os.environ.get('GOMOFS_S3_PREFIX', 'noaa-gomofs')}/{name}",
        region=os.environ.get("GOMOFS_S3_REGION", "us-west-2"), endpoint_url=endpoint,
        from_env=True, allow_http=(endpoint or "").startswith("http://"),
        force_path_style=bool(endpoint),
        write_headers={"x-amz-acl": acl} if acl and acl != "none" else None)


def open_repo(name, store_path=None, create=False):
    config = repo_config()
    st = storage(name, store_path)
    if create:
        repo = icechunk.Repository.open_or_create(st, config=config, authorize_virtual_chunk_access=AUTH)
    elif icechunk.Repository.exists(st):
        repo = icechunk.Repository.open(st, config=config, authorize_virtual_chunk_access=AUTH)
    else:
        raise SystemExit(f"no Icechunk repository at {name}: build it first (this tool only updates)")
    return repo
