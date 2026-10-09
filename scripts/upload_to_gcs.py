"""
Upload local users/addresses CSVs to GCS as an immutable ingestion batch.

Each run copies data/users.csv and data/addresses.csv to:
  gs://{bucket}/raw/{table}/ingestion_date=YYYY-MM-DD/{table}_YYYYMMDDTHHMMSS.csv

Edit the local CSVs in place and re-run to simulate continuous ingestion.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import storage

# --- Defaults (project, bucket and location can be overridden from the command line) ---
PROJECT_ID = "deloitte-ha"
BUCKET_NAME = "deloitte-ha-raw-data"
LOCATION = "us-central1"
RAW_PREFIX = "raw"

# Local CSVs live in <repo>/data, wherever the script is run from.
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"

TABLES = ("users", "addresses")


def ensure_bucket(client: storage.Client, bucket_name: str, location: str) -> storage.Bucket:
    """Return the bucket, creating it on the first run."""
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket exists: gs://{bucket_name}")
        return bucket
    except NotFound:
        pass

    # Private bucket: IAM-only access (no per-object ACLs) and no public access.
    bucket = storage.Bucket(client, name=bucket_name)
    bucket.storage_class = "STANDARD"
    bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    bucket.iam_configuration.public_access_prevention = "enforced"

    try:
        bucket = client.create_bucket(bucket, location=location)
        print(f"Created bucket: gs://{bucket_name} ({location})")
    except Conflict:
        # Created by someone else between the check and the create.
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket exists: gs://{bucket_name}")

    return bucket


def object_path(table: str, ingestion_ts: datetime) -> str:
    """
    Build the object name, e.g.
    raw/users/ingestion_date=2026-10-09/users_20261009T095830.csv

    The date folder lets the loader scan only one day; the timestamp in the
    file name makes every batch a new object, so nothing is overwritten.
    """
    date_part = ingestion_ts.strftime("%Y-%m-%d")
    ts_part = ingestion_ts.strftime("%Y%m%dT%H%M%S")
    return f"{RAW_PREFIX}/{table}/ingestion_date={date_part}/{table}_{ts_part}.csv"


def upload_table(
    bucket: storage.Bucket,
    table: str,
    ingestion_ts: datetime,
) -> str:
    """Upload data/{table}.csv as a new object and return its gs:// URI."""
    local_path = DATA_DIR / f"{table}.csv"
    if not local_path.is_file():
        raise FileNotFoundError(f"Missing local file: {local_path}")

    blob_name = object_path(table, ingestion_ts)
    uri = f"gs://{bucket.name}/{blob_name}"

    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path), content_type="text/csv")
    print(f"Uploaded {local_path.name} -> {uri}")
    return uri


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload users/addresses CSVs to GCS as a new ingestion batch."
    )
    parser.add_argument("--project", default=PROJECT_ID, help="GCP project ID")
    parser.add_argument("--bucket", default=BUCKET_NAME, help="GCS bucket name")
    parser.add_argument("--location", default=LOCATION, help="Bucket region")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    # One UTC timestamp for the whole batch, so both files share the same date folder and suffix.
    ingestion_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    print(f"Project: {args.project}")
    print(f"Bucket:  gs://{args.bucket}")
    print(f"Batch:   {ingestion_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print()

    client = storage.Client(project=args.project)
    bucket = ensure_bucket(client, args.bucket, args.location)

    for table in TABLES:
        upload_table(bucket, table, ingestion_ts)

    print()
    print("Done. Re-run after editing data/*.csv to land the next batch.")


if __name__ == "__main__":
    main()
