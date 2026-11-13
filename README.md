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

Each store is a **virtual** Icechunk repository: it holds Zarr metadata and byte-range references into NOAA's
`s3://noaa-nos-ofs-pds` bucket and no array data. Computing the references means opening every source NetCDF file
(5,622 `fields` files for 3d, 16,866 `2ds` files for 2d), which is slow on one machine, so the initial build ran
in parallel on AWS using a fork of
[developmentseed/virtualizarr-data-pipelines](https://github.com/developmentseed/virtualizarr-data-pipelines)
(AWS CDK: Step Functions plus Lambda). The code is in [`backfill/`](backfill/).

1. **Pick the files.** Only the nowcast files are used for the history: a cycle's `n003`/`n006` (3d, `fields`) or
   `n001`...`n006` (2d, `2ds`), which tile the timeline with no overlap. The inventory is a JSON list of bucket keys
   computed from the cycle times (no bucket listing), then checked to exist (`backfill/make_inventory.py`).
   The series starts about 30 days after the oldest file NOAA still has, because NOAA deletes the oldest files
   about a day a day (see [docs/operations.md](docs/operations.md)).
2. **Initialize the repository** (one Lambda). It creates a `backfill` branch, writes the virtual references of the
   **newest** nowcast file (which creates every array and supplies the static grid variables and the attributes),
   resizes every array that has a `time` dimension to the full length of the series, writes the whole `time` axis,
   and commits. The statics come from the newest file because the oldest files are the first to be deleted.
3. **Fan out.** The inventory is split into partitions of 500 files and each partition into batches of 10, with up to
   50 workers running at once. A worker opens its files with VirtualiZarr (`HDFParser`) and writes each file's
   one-step virtual dataset into its own slot of the `time` axis with `region="auto"`, on a fork of the session.
4. **Merge and promote.** The forks are merged back into `backfill`, which is then promoted to `main`.
5. **Copy and verify.** The finished repositories were built in an S3 bucket in the author's AWS account, copied to
   `ocean-icechunks/noaa-gomofs/<store>` on Source Cooperative (the `repo` file last, so a half-copied store is never
   visible), and opened anonymously to check the time axis, the static variables and a few data reads.

From then on `gomofs update` (this repository's workflow) is the only writer: it appends new cycles and replaces
the forecast tail, so no AWS stack is needed for upkeep.

Things that went wrong and are handled in `backfill/processor.py`:

- **Per-file global attributes.** VirtualiZarr rewrites the root group's attributes on every region write, and they
  differ file to file. Left alone, every worker modifies the root group differently and the merge fails with
  "Multiple writers updated the same groups". The processor clears them for region writes and stores the root
  `coordinates` attribute once, equal to what the workers would write.
- **Time encoding.** xarray picks `days since` or `hours since` per file from the values, so an appended file can
  decode to the wrong time. The processor pins `hours since 2024-01-01` as `int64` for `time`.
- **Static variables.** Only variables that have a `time` dimension are written by region writes; statics are written
  once, at initialization.
- **A failed first run leaves a `backfill` branch behind**, which makes a restart fail with `AlreadyExistsError`;
  clear the repository prefix before restarting.

Cost and time, measured: 3d took about 14 minutes and 2d about 73 minutes (about 2 hours when both ran at once),
2 GB Lambdas; the 3d run's cost is estimated at about one dollar.

Rebuilding is the same procedure with a later start. It is not automated; `gomofs status` warns when the oldest
step is close to NOAA's deletion age.
