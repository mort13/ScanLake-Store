#!/usr/bin/env python3
"""
Migration script: extract OCR confidence columns from existing scans parquet files
into separate *confidences.parquet files in R2.

The script splits every  scans_batch{N}.parquet  into:
  - scans_batch{N}.parquet          (columns WITHOUT confidence data – rewritten in place)
  - confidences_batch{N}.parquet    (confidence columns + the row key column for joining)

Prerequisites:
    pip install boto3 pyarrow

Cloudflare R2 credentials must be provided via environment variables or CLI args:
    R2_ACCOUNT_ID     – Cloudflare account ID
    R2_ACCESS_KEY     – R2 API access key ID
    R2_SECRET_KEY     – R2 API secret access key
    R2_BUCKET         – bucket name (default: scanlake-data)

Usage:
    # Dry-run (preview what would happen, no writes)
    python migrate_confidences.py --dry-run

    # Live run (reads existing scans files, splits them, writes confidences files)
    python migrate_confidences.py

    # Skip rewriting the scans files (only create confidences files, keep originals)
    python migrate_confidences.py --no-rewrite-source

Options:
    --dry-run           Print actions without writing anything to R2
    --no-rewrite-source Do not overwrite the source scans parquet after splitting off confidences
    --confidence-cols   Comma-separated explicit column names to treat as confidence data.
                        If omitted, any column whose name contains 'confidence' or 'conf_'
                        (case-insensitive) is treated as a confidence column.
    --join-col          Column name used as the row-level join key copied into the
                        confidences file (default: auto-detect first column or 'id').
    --account-id        R2 account ID (overrides R2_ACCOUNT_ID env var)
    --access-key        R2 access key (overrides R2_ACCESS_KEY env var)
    --secret-key        R2 secret key (overrides R2_SECRET_KEY env var)
    --bucket            Bucket name (overrides R2_BUCKET env var, default: scanlake-data)
"""

import argparse
import io
import os
import re
import sys

import boto3
import pyarrow.parquet as pq
import pyarrow as pa


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_r2_client(account_id: str, access_key: str, secret_key: str):
    endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    )


def list_source_keys(s3, bucket: str) -> list[str]:
    """Return all keys matching the scans_batch or compositions_batch pattern."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            if re.search(r"/(scans|compositions)_batch\d+\.parquet$", key):
                keys.append(key)
    return keys


def confidences_key_for(source_key: str) -> str:
    """Derive the confidences parquet key from a scans or compositions key.
    e.g. 2025/01/15/user1/sess1/scans_batch001.parquet
      -> 2025/01/15/user1/sess1/confidences_batch001.parquet
         2025/01/15/user1/sess1/compositions_batch001.parquet
      -> 2025/01/15/user1/sess1/confidences_batch001.parquet
    """
    return re.sub(r"/(scans|compositions)_batch(\d+)\.parquet$", r"/confidences_batch\2.parquet", source_key)


def detect_confidence_columns(schema: pa.Schema) -> list[str]:
    """Return column names that look like confidence data.
    Matches columns ending with '_conf' or containing 'confidence' (case-insensitive).
    e.g. amount_mat_index_conf, word_confidence
    """
    return [
        field.name
        for field in schema
        if re.search(r"_conf$|confidence", field.name, re.IGNORECASE)
    ]


def detect_join_column(schema: pa.Schema) -> str | None:
    """Heuristic: return first column that looks like a row identifier.
    Recognises mat_index, capture_id, scan_id, id, key, row_key, etc.
    """
    candidates = [
        field.name
        for field in schema
        if re.search(r"^mat_?index$|^capture_?id$|\bid\b|_id|^key$|^row_?key$|^scan_?id$", field.name, re.IGNORECASE)
    ]
    return candidates[0] if candidates else schema[0].name if len(schema) > 0 else None


# ---------------------------------------------------------------------------
# Core migration logic
# ---------------------------------------------------------------------------

def migrate_file(
    s3,
    bucket: str,
    source_key: str,
    conf_cols: list[str] | None,
    join_col: str | None,
    rewrite_source: bool,
    dry_run: bool,
) -> None:
    conf_key = confidences_key_for(source_key)

    # Check if confidences file already exists – skip to avoid duplicates
    try:
        s3.head_object(Bucket=bucket, Key=conf_key)
        print(f"  [SKIP] {conf_key} already exists")
        return
    except s3.exceptions.ClientError:
        pass  # Does not exist – proceed
    except Exception:
        pass

    print(f"  Downloading {source_key} ...")
    response = s3.get_object(Bucket=bucket, Key=source_key)
    raw = response["Body"].read()

    table: pa.Table = pq.read_table(io.BytesIO(raw))
    schema = table.schema

    # Resolve confidence columns
    resolved_conf_cols = conf_cols if conf_cols else detect_confidence_columns(schema)
    if not resolved_conf_cols:
        print(f"  [WARN] No confidence columns found in {source_key}, skipping")
        return

    # Resolve join column
    resolved_join_col = join_col if join_col else detect_join_column(schema)

    print(f"  Confidence columns : {resolved_conf_cols}")
    print(f"  Join column        : {resolved_join_col}")

    # Build confidences table: join_col + confidence columns
    conf_col_set = set(resolved_conf_cols)
    conf_table_cols = []
    if resolved_join_col and resolved_join_col not in conf_col_set:
        conf_table_cols.append(resolved_join_col)
    conf_table_cols.extend(resolved_conf_cols)

    # Validate all required columns exist
    missing = [c for c in conf_table_cols if c not in schema.names]
    if missing:
        print(f"  [ERROR] Columns not found in parquet schema: {missing}. Skipping.")
        return

    confidences_table = table.select(conf_table_cols)

    # Serialize to parquet in memory
    conf_buf = io.BytesIO()
    pq.write_table(confidences_table, conf_buf)
    conf_buf.seek(0)

    if dry_run:
        print(f"  [DRY-RUN] Would write {conf_key} ({conf_buf.getbuffer().nbytes} bytes)")
    else:
        print(f"  Writing {conf_key} ...")
        s3.put_object(
            Bucket=bucket,
            Key=conf_key,
            Body=conf_buf.read(),
            ContentType="application/octet-stream",
        )

    if rewrite_source:
        # Remove confidence columns from source table and rewrite
        remaining_cols = [c for c in schema.names if c not in conf_col_set]
        if len(remaining_cols) == len(schema.names):
            print(f"  [INFO] No columns removed from source; skipping rewrite of {source_key}")
        else:
            stripped_table = table.select(remaining_cols)
            src_buf = io.BytesIO()
            pq.write_table(stripped_table, src_buf)
            src_buf.seek(0)

            if dry_run:
                print(f"  [DRY-RUN] Would rewrite {source_key} without confidence columns")
            else:
                print(f"  Rewriting {source_key} without confidence columns ...")
                s3.put_object(
                    Bucket=bucket,
                    Key=source_key,
                    Body=src_buf.read(),
                    ContentType="application/octet-stream",
                )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split OCR confidence columns from scans parquets into separate confidences parquets in R2."
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no writes")
    parser.add_argument(
        "--no-rewrite-source",
        action="store_true",
        help="Keep confidence columns in the source scans file (don't strip them)",
    )
    parser.add_argument(
        "--confidence-cols",
        help="Comma-separated column names to extract as confidence data",
    )
    parser.add_argument(
        "--join-col",
        help="Column name copied into the confidences file as a join key",
    )
    parser.add_argument("--account-id", help="R2 account ID")
    parser.add_argument("--access-key", help="R2 access key ID")
    parser.add_argument("--secret-key", help="R2 secret access key")
    parser.add_argument("--bucket", help="R2 bucket name", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    account_id = args.account_id or os.environ.get("R2_ACCOUNT_ID")
    access_key = args.access_key or os.environ.get("R2_ACCESS_KEY")
    secret_key = args.secret_key or os.environ.get("R2_SECRET_KEY")
    bucket = args.bucket or os.environ.get("R2_BUCKET", "scanlake-data")

    missing_creds = [n for n, v in [("account-id", account_id), ("access-key", access_key), ("secret-key", secret_key)] if not v]
    if missing_creds:
        print(f"ERROR: Missing credentials: {', '.join(missing_creds)}")
        print("Set R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY env vars or pass them as CLI args.")
        sys.exit(1)

    conf_cols = [c.strip() for c in args.confidence_cols.split(",")] if args.confidence_cols else None
    join_col = args.join_col or None
    rewrite_source = not args.no_rewrite_source

    s3 = get_r2_client(account_id, access_key, secret_key)

    print(f"Bucket       : {bucket}")
    print(f"Dry-run      : {args.dry_run}")
    print(f"Rewrite src  : {rewrite_source}")
    print()

    keys = list_source_keys(s3, bucket)
    if not keys:
        print("No scans or compositions parquet files found.")
        return

    print(f"Found {len(keys)} source parquet file(s).\n")

    for key in keys:
        print(f"Processing: {key}")
        try:
            migrate_file(
                s3=s3,
                bucket=bucket,
                source_key=key,
                conf_cols=conf_cols,
                join_col=join_col,
                rewrite_source=rewrite_source,
                dry_run=args.dry_run,
            )
        except Exception as exc:
            print(f"  [ERROR] Failed to process {key}: {exc}")
        print()

    print("Done.")


if __name__ == "__main__":
    main()
