"""Keep the GoMOFS monotonic-time Icechunk stores up to date.

    gomofs update 3d            # append new cycles + replace the forecast tail
    gomofs status 3d --format markdown

Every command reads the store's state, diffs it against the NOAA bucket and does only the
missing work, so a failed or skipped run needs no retry: the next one catches up, and a run with
nothing to do commits nothing.  One writer at a time is the caller's job (the workflow's
``concurrency`` group).

Every cycle contributes its nowcast files (they tile the timeline without overlap).  The newest
cycle also contributes its forecast.  An update trims the previous forecast tail (everything
after the previous newest cycle time) and appends the new cycles, so the store always ends with
the latest forecast.  The newest cycle time is kept in the root group attribute ``last_cycle``.
"""
import argparse
import datetime as dt
import sys

import numpy as np
import pandas as pd
import xarray as xr
import zarr

from . import common as g

FORECAST_WAIT = dt.timedelta(hours=12)      # an incomplete newest cycle this young is still uploading
RETENTION_DAYS = 735                        # NOAA deleted the 2024-11-07 18Z files on 2026-11-12
WARN_DAYS = 715                             # warn when the oldest step is this old (~20 d before deletions)


def _store_name(variant):
    return f"timeseries-{variant}"


def _now():
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def update(args):
    name = _store_name(args.variant)
    spec = g.FAMILIES[args.variant]
    repo = g.open_repo(name, args.store_path)
    root = zarr.open_group(repo.readonly_session("main").store, mode="r")
    last = dt.datetime.fromisoformat(root.attrs["last_cycle"])
    end = args.end or _now().date()

    cycles, partial = g.list_cycles(args.variant, last.date(), end, need_forecast=False)
    cycles = {c: r for c, r in cycles.items() if c > last}
    for c, r in partial.items():
        if c > last:
            print(f"skipping incomplete cycle {c:%Y-%m-%d %Hz}: {len(r)}/{spec['per_cycle']} files")
    if cycles:
        newest = max(cycles)
        # A young cycle whose forecast is still uploading must wait: once it is stored as the
        # newest, its forecast tail would never be added (the next run starts after it).
        if len(cycles[newest]) != spec["per_cycle"] and _now() - newest < FORECAST_WAIT:
            print(f"cycle {newest:%Y-%m-%d %Hz} has {len(cycles[newest])}/{spec['per_cycle']} files "
                  f"and is still arriving; waiting")
            del cycles[newest]
    if not cycles:
        print(f"{name}: nothing to do (last cycle {last:%Y-%m-%d %Hz})")
        return 0
    newest = max(cycles)
    if min(cycles) - last != dt.timedelta(hours=6):
        print(f"::warning::gap between stored cycle {last} and {min(cycles)}: the series has a hole")

    registry = g.make_registry()
    rows = g.timeseries_rows(args.variant, cycles)
    print(f"{name}: {len(cycles)} new cycles through {newest:%Y-%m-%d %Hz}, {len(rows)} files")

    session = repo.writable_session("main")
    times = xr.open_zarr(session.store, consolidated=False, chunks=None).time.values
    keep = int(np.searchsorted(times, np.datetime64(last), side="right"))
    if keep < len(times):                       # drop the old forecast tail
        grp = zarr.open_group(session.store, mode="r+")
        for _, arr in grp.arrays():
            dims = arr.metadata.dimension_names or ()
            if "time" in dims:
                shape = list(arr.shape)
                shape[dims.index("time")] = keep
                arr.resize(tuple(shape))
        print(f"trimmed {len(times) - keep} forecast steps")

    for i in range(0, len(rows), args.batch):
        part = rows[i:i + args.batch]
        ds = g.timeseries_dataset(part, registry)
        g.for_append(ds, "time").vz.to_icechunk(session.store, append_dim="time")
        if i + args.batch < len(rows):
            # last_cycle is still the old value, so an interruption here is repaired by the
            # next run, which trims everything after it before appending again
            session.commit(f"series {args.variant}: files up to {part[-1][0]:%Y-%m-%d %H}Z")
            session = repo.writable_session("main")
        print(f"  {min(i + args.batch, len(rows))}/{len(rows)} files", flush=True)

    zarr.open_group(session.store, mode="r+").attrs["last_cycle"] = newest.isoformat()
    snap = session.commit(f"series {args.variant}: cycles through {newest:%Y-%m-%d %Hz}")
    print("committed", snap)
    return 0


def _probe(ds, index):
    """True if the first science array can be read at ``index`` (its source file still exists)."""
    try:
        ds.zeta.isel(time=index).load()
        return True
    except Exception as e:       # a deleted source file surfaces as a storage error
        return f"{type(e).__name__}: {str(e).splitlines()[0][:120]}"


def status(args):
    name = _store_name(args.variant)
    repo = g.open_repo(name, args.store_path)
    session = repo.readonly_session("main")
    root = zarr.open_group(session.store, mode="r")
    last = dt.datetime.fromisoformat(root.attrs["last_cycle"])
    ds = xr.open_zarr(session.store, consolidated=False, chunks=None)
    t = pd.DatetimeIndex(ds.time.values)
    tail = int((t > pd.Timestamp(last)).sum())
    oldest_age = (_now() - t[0].to_pydatetime()).days
    first_ok, last_ok = _probe(ds, 0), _probe(ds, len(t) - 1)
    lag = (_now() - last).total_seconds() / 3600
    rows = [
        ("steps", f"{len(t):,}"),
        ("first valid time", f"{t[0]:%Y-%m-%d %H}Z (age {oldest_age} d; NOAA has deleted files at ~{RETENTION_DAYS} d)"),
        ("last valid time", f"{t[-1]:%Y-%m-%d %H}Z ({tail} forecast steps after the newest cycle)"),
        ("newest cycle", f"{last:%Y-%m-%d %H}Z, {lag:.0f} h ago"),
        ("oldest step readable", "yes" if first_ok is True else f"**NO** — {first_ok}"),
        ("newest step readable", "yes" if last_ok is True else f"**NO** — {last_ok}"),
    ]
    problems = []
    if first_ok is not True:
        problems.append("the oldest step's source file is gone: rebuild the store with a later start")
    elif oldest_age >= WARN_DAYS:
        problems.append(f"the oldest step is {oldest_age} d old: NOAA's deletions are close; rebuild soon")
    if lag > 24:
        problems.append(f"no new cycle stored for {lag:.0f} h")
    if args.format == "markdown":
        print(f"### {name}\n\n| | |\n|---|---|")
        for k, v in rows:
            print(f"| {k} | {v} |")
        for p in problems:
            print(f"\n> :warning: {p}")
    else:
        for k, v in rows:
            print(f"{k:22} {v}")
        for p in problems:
            print("WARNING:", p)
    return 1 if first_ok is not True or last_ok is not True else 0


def main(argv=None):
    ap = argparse.ArgumentParser(prog="gomofs", description=__doc__,
                                 formatter_class=argparse.RawTextHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for cmd, fn in (("update", update), ("status", status)):
        p = sub.add_parser(cmd)
        p.add_argument("variant", choices=g.FAMILIES)
        p.add_argument("--store-path", help="local directory holding the stores (default: S3 from GOMOFS_S3_*)")
        p.set_defaults(fn=fn)
        if cmd == "update":
            p.add_argument("--end", type=dt.date.fromisoformat, default=None, help="last day to look at (default today, UTC)")
            p.add_argument("--batch", type=int, default=400, help="files per commit")
        else:
            p.add_argument("--format", choices=("text", "markdown"), default="text")
    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
