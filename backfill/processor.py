"""NOAA GoMOFS nowcast time series (monotonic valid-time axis) as a virtual Icechunk store.

Source: s3://noaa-nos-ofs-pds/gomofs/netcdf/ (us-east-1, anonymous).  Four forecast cycles a day
(00/06/12/18 UTC).  Only each cycle's *nowcast* files are used, because they tile the timeline
with no overlap:

  GOMOFS_VARIANT=3d  ``fields`` files  n003, n006            -> 3-hourly, 3-D + 2-D fields
  GOMOFS_VARIANT=2d  ``2ds`` files     n001 ... n006         -> hourly, surface fields

``n006`` is valid at the cycle time and ``n00k`` is ``6 - k`` hours before it.  The store has one
``time`` dimension of valid times.  Backfill declares the whole axis up front (it follows from the
cycle times, no listing needed) and the workers fill it with ``region="auto"``; a cycle that is
missing from the archive would leave an empty row, but none are missing from 2024-11-07 to
2026-11-12.  Forward updates (the newest cycle's forecast tail, trimming and appending) are not
done here: they run from ``build_timeseries.py`` in the icechunks repository.

Environment: GOMOFS_VARIANT (3d|2d), GOMOFS_END_CYCLE (ISO, last cycle of the backfill, UTC),
ICECHUNK_BUCKET / ICECHUNK_PREFIX / ICECHUNK_REGION or ICECHUNK_LOCAL_PATH for the repository.
"""

import datetime as dt
import logging
import os
import re
from datetime import datetime
from itertools import islice

import icechunk
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from icechunk import ForkSession, Repository, Session
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import S3Store
from virtualizarr import open_virtual_dataset
from virtualizarr.parsers import HDFParser

logger = logging.getLogger(__name__)

SRC_BUCKET, SRC_REGION = "noaa-nos-ofs-pds", "us-east-1"
SRC = f"s3://{SRC_BUCKET}/"
APPEND_DIMENSION = "time"
SOURCE_TIME = "ocean_time"
TIME_UNITS = "hours since 2024-01-01"
# NOAA prunes the bucket from the front, about a day a day (2026-11-13: oldest file 2024-11-09 00Z),
# so the series starts ~30 days after the oldest surviving cycle and is rebuilt now and then.
FIRST_CYCLE = dt.datetime.fromisoformat(os.environ.get("GOMOFS_FIRST_CYCLE", "2024-12-10T00:00:00"))
TREE_FROM = dt.datetime(2024, 11, 19, 18)       # first cycle present under YYYY/MM/DD/

VARIANTS = {
    "3d": {"family": "fields", "nowcast_leads": (-3, 0)},
    "2d": {"family": "2ds", "nowcast_leads": tuple(range(-5, 1))},
}
_NAME = re.compile(r"gomofs\.t(\d\d)z\.(\d{8})\.(fields|2ds)\.n(\d{3})\.nc$")


def variant() -> str:
    v = os.environ.get("GOMOFS_VARIANT", "3d")
    if v not in VARIANTS:
        raise ValueError(f"GOMOFS_VARIANT must be one of {sorted(VARIANTS)}, got {v!r}")
    return v


def end_cycle() -> dt.datetime:
    return dt.datetime.fromisoformat(os.environ["GOMOFS_END_CYCLE"])


def valid_axis(v: str, last: dt.datetime) -> pd.DatetimeIndex:
    """Every nowcast valid time from FIRST_CYCLE to the ``last`` cycle, in order."""
    cycles = pd.date_range(FIRST_CYCLE, last, freq="6h")
    times = sorted({c + pd.Timedelta(hours=h) for c in cycles for h in VARIANTS[v]["nowcast_leads"]})
    return pd.DatetimeIndex(times)


def valid_time(file_key: str) -> np.datetime64:
    """The valid time a nowcast file's *name* promises."""
    m = _NAME.search(file_key)
    if not m:
        raise ValueError(f"not a GoMOFS nowcast file: {file_key}")
    hh, ymd, _fam, n = m.groups()
    cycle = dt.datetime.strptime(ymd + hh, "%Y%m%d%H")
    return np.datetime64(cycle + dt.timedelta(hours=int(n) - 6), "ns")


def nowcast_keys(v: str, last: dt.datetime) -> list[str]:
    """Bucket-relative keys of every nowcast file the axis needs, in time order.

    From the 2024-11-19 18Z cycle the files are in ``YYYY/MM/DD/`` (NOAA's current layout, where
    the same files also exist in the flat ``YYYYMM/`` folder); before that only the flat folder
    has them.
    """
    fam = VARIANTS[v]["family"]
    keys = []
    for c in pd.date_range(FIRST_CYCLE, last, freq="6h"):
        c = c.to_pydatetime()
        folder = c.strftime("%Y%m") if c < TREE_FROM else c.strftime("%Y/%m/%d")
        for lead in VARIANTS[v]["nowcast_leads"]:
            keys.append(f"gomofs/netcdf/{folder}/gomofs.t{c:%H}z.{c:%Y%m%d}.{fam}.n{lead + 6:03d}.nc")
    return keys


def first_file_key(v: str) -> str:
    """The file for the first time on the axis (it sits in the flat YYYYMM folder)."""
    fam, lead = VARIANTS[v]["family"], VARIANTS[v]["nowcast_leads"][0]
    c = FIRST_CYCLE
    return f"gomofs/netcdf/{c:%Y%m}/gomofs.t{c:%H}z.{c:%Y%m%d}.{fam}.n{lead + 6:03d}.nc"


def _registry() -> ObjectStoreRegistry:
    return ObjectStoreRegistry(
        {SRC: S3Store(bucket=SRC_BUCKET, region=SRC_REGION, skip_signature=True)}
    )


def virtual_slice(file_key: str, registry: ObjectStoreRegistry | None = None) -> xr.Dataset:
    """One nowcast file as a one-step virtual dataset on ``time``.

    The file's own valid time must agree with its name, or the axis position would be wrong.
    """
    ds = open_virtual_dataset(
        f"s3://{SRC_BUCKET}/{file_key.removeprefix(SRC_BUCKET + '/')}",
        registry=registry or _registry(), parser=HDFParser(),
        loadable_variables=[SOURCE_TIME], decode_times=True,
    )
    ds = ds.drop_vars("ntimes", errors="ignore").rename({SOURCE_TIME: APPEND_DIMENSION})
    if ds[APPEND_DIMENSION].values[0] != valid_time(file_key):
        raise ValueError(f"{file_key}: file valid time {ds[APPEND_DIMENSION].values[0]} "
                         f"does not match its name")
    ds[APPEND_DIMENSION].attrs.update(standard_name="time")
    # pinned: xarray otherwise picks "days since" or "hours since" per file's own values
    ds[APPEND_DIMENSION].encoding = {"units": TIME_UNITS, "dtype": "int64"}
    return ds


def time_dependent(ds: xr.Dataset) -> xr.Dataset:
    """Only the variables carrying ``time``; ``region="auto"`` has no slice for the others."""
    ds = ds.drop_vars([n for n in ds.variables if APPEND_DIMENSION not in ds[n].dims])
    # A region write also rewrites the root group attributes.  The file's global attributes differ
    # from file to file (creation time, history, ...), so two workers would then modify the root
    # group differently and the merge would fail.  The first file's attributes are already stored.
    ds.attrs = {}
    return ds


def repo_config() -> icechunk.RepositoryConfig:
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(SRC, icechunk.s3_store(region=SRC_REGION, anonymous=True))
    )
    return config


def authorize() -> dict:
    return icechunk.containers_credentials({SRC: icechunk.s3_anonymous_credentials()})


def _storage() -> icechunk.Storage:
    bucket = os.environ.get("ICECHUNK_BUCKET")
    if bucket:
        return icechunk.s3_storage(
            bucket=bucket, prefix=os.environ.get("ICECHUNK_PREFIX"),
            region=os.environ.get("ICECHUNK_REGION"), from_env=True,
        )
    return icechunk.local_filesystem_storage(os.environ["ICECHUNK_LOCAL_PATH"])


class Processor:
    def initialize_repo(self) -> Repository:
        repo = Repository.open_or_create(
            storage=_storage(), config=repo_config(), authorize_virtual_chunk_access=authorize()
        )
        # a reader opening anonymously needs the virtual chunk container in the saved config
        repo.save_config()
        return repo

    def open_backfill_repo(self) -> Repository:
        return self.initialize_repo()

    def initialize_session(self, repo: Repository) -> Session:
        return repo.writable_session("main")

    def process_file(self, file_key: str, session: Session) -> bool:
        # Forward updates (including the newest cycle's forecast tail) run from GitHub Actions.
        logger.error("forward processing is not implemented here: %s", file_key)
        return False

    def commit_processed_files(self, session: Session) -> str:
        return str(session.commit(message=f"Update {session.snapshot_id}"))

    def initialize_backfill_store(self, repo: Repository) -> str:
        """Create every array at full length, write the whole ``time`` axis, commit."""
        v, last = variant(), end_cycle()
        axis = valid_axis(v, last)

        repo.create_branch("backfill", repo.lookup_branch("main"))
        session = repo.writable_session("backfill")
        # The NEWEST nowcast file creates every array and supplies the static grid variables and
        # attributes: the oldest file is the one NOAA deletes first, and the statics would then
        # dangle.  Its time-dependent references land at index 0 and are overwritten there by the
        # region write of the first file (the first file is always in the inventory).
        sample = virtual_slice(nowcast_keys(v, last)[-1])
        sample.vz.to_icechunk(session.store)

        root = zarr.open_group(session.store, mode="r+")
        for _name, arr in root.arrays():
            dims = arr.metadata.dimension_names or ()
            if APPEND_DIMENSION in dims:
                shape = list(arr.shape)
                shape[dims.index(APPEND_DIMENSION)] = len(axis)
                arr.resize(tuple(shape))
        hours = ((axis - pd.Timestamp("2024-01-01")) / pd.Timedelta(hours=1)).astype("int64")
        root[APPEND_DIMENSION][:] = np.asarray(hours)
        # Every region write also sets the root ``coordinates`` attribute from the coordinates of
        # the dataset it writes (just ``time``, the static ones being dropped).  If the stored value
        # differs, each worker modifies the root group and the merge fails with "Multiple writers
        # updated the same groups".  xarray still finds lat/lon via the per-variable attributes.
        root.attrs["coordinates"] = " ".join(time_dependent(sample).coords)
        root.attrs["last_cycle"] = last.isoformat()
        root.attrs["gomofs_variant"] = v
        return str(session.commit("Initialize backfill shape"))

    def process_backfill_file(self, file_key: str, fork: ForkSession) -> bool:
        try:
            time_dependent(virtual_slice(file_key)).vz.to_icechunk(fork.store, region="auto")
            return True
        except Exception:
            logger.exception("process_backfill_file failed for %s", file_key)
            return False

    def garbage_collect(self, expiry_time: datetime) -> icechunk.GCSummary:
        repo = self.initialize_repo()
        repo.expire_snapshots(older_than=expiry_time)
        return repo.garbage_collect(delete_object_older_than=expiry_time)
