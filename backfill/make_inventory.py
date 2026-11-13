#!/usr/bin/env python
"""Write the backfill inventory (a JSON array of bucket-relative file keys) for a GoMOFS store.

    python scripts/make_inventory.py 3d 2026-11-12T12:00:00 inventory-3d.json
    aws s3 cp inventory-3d.json s3://<BackfillBucketName>/inventory-3d.json

Set the same GOMOFS_VARIANT and GOMOFS_END_CYCLE in .env before deploying.  The keys follow from
the cycle times, so no bucket listing is needed; run ``--check`` to confirm every key exists.
"""
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambda" / "virtualizarr-processor"))
from virtualizarr_processor import processor as p  # noqa: E402

variant, end, out = sys.argv[1], dt.datetime.fromisoformat(sys.argv[2]), sys.argv[3]
keys = p.nowcast_keys(variant, end)
Path(out).write_text(json.dumps(keys))
print(f"{len(keys)} keys, {keys[0]} .. {keys[-1]}")
if "--check" in sys.argv:
    import fsspec
    fs = fsspec.filesystem("s3", anon=True)
    missing = [k for k in keys if not fs.exists(f"{p.SRC_BUCKET}/{k}")]
    print("missing:", len(missing), missing[:5])
