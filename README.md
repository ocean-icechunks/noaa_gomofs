# noaa-gomofs-icechunk

Keeps the NOAA GoMOFS (Gulf of Maine ROMS) virtual Icechunk stores on Source Cooperative up to date,
the way [noaa_oisst](https://github.com/ocean-icechunks/noaa_oisst) keeps OISST up to date.

| Store | Files | Step |
|---|---|---|
| `ocean-icechunks/noaa-gomofs/timeseries-3d` | `fields` (3-D) | 3 h |
| `ocean-icechunks/noaa-gomofs/timeseries-2d` | `2ds` (surface) | 1 h |

Both have one `time` dimension (valid time). Every cycle contributes its nowcast files, which tile the
timeline; the newest cycle also contributes its 72 h forecast, which each update replaces.
The stores were first built by a parallel AWS backfill (see [How the stores were initialized](#how-the-stores-were-initialized));
this tool only keeps them up to date.

```bash
pip install .
gomofs status 3d --store-path /some/dir     # or set GOMOFS_S3_BUCKET etc. for S3
gomofs update 3d --store-path /some/dir
```

[`notebooks/noaa-gomofs-quickstart.ipynb`](notebooks/noaa-gomofs-quickstart.ipynb) opens `timeseries-3d`
anonymously and plots the latest surface temperature (hvplot) and a month of surface temperature at NERACOOS
buoy B01, ending with the newest forecast. It is committed without outputs.

`.github/workflows/update.yml` runs `gomofs update` and `gomofs status` for both stores every three
hours. See [docs/operations.md](docs/operations.md) for setup and the rolling-archive caveat.

## How the stores were initialized

The history was computed in parallel on AWS with Development Seed's
[`virtualizarr-data-pipelines`](https://github.com/developmentseed/virtualizarr-data-pipelines), copied to
Source Cooperative, and is kept current by the workflow in this repository. The full story (the design, the
backfill, the copy, the upkeep, how rebuilds and backfills will work, and what went wrong) is in the stores'
[README](https://github.com/ocean-icechunks/icechunks/blob/main/noaa-gomofs/README.md#how-this-was-built).

What is here is the code for that backfill, in [`backfill/`](backfill/):

- `processor.py`: the GoMOFS processor for the pipeline
- `make_inventory.py`: writes the list of source files to process
- `upstream-changes.patch`: the changes to the upstream pipeline (applies to `bc834d7`)
- [`backfill/README.md`](backfill/README.md): the recipe and the settings, for rebuilding a store

`docs/operations.md` covers running and repairing the update workflow.
