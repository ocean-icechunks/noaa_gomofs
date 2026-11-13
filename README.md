# noaa-gomofs-icechunk

Keeps the NOAA GoMOFS (Gulf of Maine ROMS) virtual Icechunk stores on Source Cooperative up to date,
the way [noaa_oisst](https://github.com/ocean-icechunks/noaa_oisst) keeps OISST up to date.

| Store | Files | Step |
|---|---|---|
| `ocean-icechunks/noaa-gomofs/timeseries-3d` | `fields` (3-D) | 3 h |
| `ocean-icechunks/noaa-gomofs/timeseries-2d` | `2ds` (surface) | 1 h |

Both have one `time` dimension (valid time). Every cycle contributes its nowcast files, which tile the
timeline; the newest cycle also contributes its 72 h forecast, which each update replaces.
The first build is made elsewhere (see `docs/operations.md`); this tool only updates.

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
