# Operations

## The idea

Every command reads the store's state, diffs it against NOAA's bucket and does only the missing work.
A killed or skipped run needs no retry, and a run with nothing to do commits nothing. Exactly one writer
per store: the workflow's `concurrency` group is the only guard, so do not run a second writer against
production while a run is active.

`update`: lists cycles after the stored `last_cycle` (root attribute), appends their nowcast files,
trims the previous forecast tail and appends the newest cycle's forecast. A newest cycle whose forecast
is still arriving (fewer files than expected and younger than 12 h) waits for the next run, otherwise its
forecast tail would never be added. A missing cycle is reported as a warning (a hole), not an error.
`status`: steps, time range, forecast-tail length, whether the oldest and newest steps can still be read.

## Rolling archive (read this)

NOAA deletes the oldest GoMOFS files about a day a day (the 2024-11-07 18Z files vanished on
2026-11-12, ~735 days). The stores reference those files, so the front of each store dies. Mitigation:

- each build starts about 30 days after the oldest surviving cycle;
- `status` fails when the oldest step can no longer be read and warns from 715 days;
- the static grid variables are taken from the newest file at build time, not the oldest;
- the stores must be rebuilt (about 14 min for 3d, 1-2 h for 2d, with the AWS Lambda backfill in
  `noaa-gomofs-virtualizarr-data-pipelines`) before the warning becomes a failure. **A rebuild is
  not automated yet.**

## Credentials (one-time setup)

Same as noaa_oisst: no secrets, GitHub OIDC into a Source Cooperative service account.

1. `source-coop service-account create --github ocean-icechunks/noaa_gomofs --branch main --access ocean-icechunks/noaa-gomofs=write`
2. Create the `source-coop` GitHub environment, deployment branches limited to `main`.
3. Set repository variables `GOMOFS_S3_BUCKET=ocean-icechunks` and `SOURCE_COOP_SERVICE_ACCOUNT` (the
   account id the CLI prints). The workflow skips itself until both are set.

Proxy credentials last 1 h by default; the workflow asks for 2 h because Icechunk keeps the first
credentials it reads.

## Not yet covered

The FMRC stores (`fmrc-3d`, `fmrc-2d`: forecast cycle x lead step) are not built, so there is no
update step for them here.
