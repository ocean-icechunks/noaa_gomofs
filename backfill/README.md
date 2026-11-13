# Initial build (AWS backfill)

What built the first version of each store. It is a fork of
[developmentseed/virtualizarr-data-pipelines](https://github.com/developmentseed/virtualizarr-data-pipelines)
at `bc834d7`, so it is kept here as the pieces that differ from upstream, not as a copy of the whole project.

| File | What it is |
|---|---|
| `processor.py` | The GoMOFS processor, replacing upstream's `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`: axis, initialization, region writes |
| `make_inventory.py` | Writes the backfill inventory (replaces nothing; goes in `scripts/`) |
| `upstream-changes.patch` | Changes to `cdk/settings.py`, `cdk/stack.py`, `cdk/stack_constructs/backfill_pipeline.py` and the Lambda `pyproject.toml` |

## Recipe

```bash
git clone https://github.com/developmentseed/virtualizarr-data-pipelines && cd virtualizarr-data-pipelines
git checkout bc834d7
git apply /path/to/noaa_gomofs/backfill/upstream-changes.patch
cp /path/to/noaa_gomofs/backfill/processor.py lambda/virtualizarr-processor/virtualizarr_processor/processor.py
cp /path/to/noaa_gomofs/backfill/make_inventory.py scripts/
```

Settings go in `.env` (one deployment per store, so a different `STACK_NAME`, `BACKFILL_BUCKET_NAME` and
`ICECHUNK_PREFIX` for each):

| Variable | Value |
|---|---|
| `GOMOFS_VARIANT` | `3d` (`fields`, 3-hourly) or `2d` (`2ds`, hourly) |
| `GOMOFS_FIRST_CYCLE` | first cycle of the series, about 30 days after NOAA's oldest file |
| `GOMOFS_END_CYCLE` | last nowcast cycle to include |
| `ICECHUNK_BUCKET`, `ICECHUNK_PREFIX`, `ICECHUNK_REGION` | where the repository is written |
| `DATA_BUCKET_NAME` | `noaa-nos-ofs-pds` |
| `BACKFILL_ENABLED` | `true` |
| `BACKFILL_BUCKET_NAME` | a new bucket for the inventory |
| `BACKFILL_PARTITION_SIZE`, `BACKFILL_MAX_ITEMS_PER_BATCH`, `BACKFILL_MAX_CONCURRENCY` | `500`, `10`, `50` |

```bash
python scripts/make_inventory.py 3d 2026-11-12T12:00:00 inventory-3d.json     # then check every key exists
npx aws-cdk deploy
aws s3 cp inventory-3d.json s3://<BackfillBucketName>/inventory-3d.json
./scripts/start_backfill.sh gomofs-3d-1 s3://<BackfillBucketName>/inventory-3d.json
```

When the Step Functions execution succeeds, open the repository and check the `time` axis (uniform spacing), a static
variable such as `h`, and a few data reads; then copy it to its destination with the `repo` file last.
