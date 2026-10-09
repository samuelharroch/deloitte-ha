"""
Load today's GCS ingestion batches into BigQuery bronze tables (APPEND).

Default scope: only objects under
  gs://{bucket}/raw/{table}/ingestion_date={today_UTC}/

ingestion_time is a TIMESTAMP set to the load moment (when rows enter BigQuery).
Batch identity / idempotency comes from object_uri in management.load_manifest.

Bronze tables (bronze.users, bronze.addresses) keep every batch; Dataform
upserts them into silver.
Re-runs skip already loaded files. Use --date YYYY-MM-DD for another day.
"""

from __future__ import annotations

import argparse
import re
import uuid
from datetime import date, datetime, timezone

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

# --- Defaults (each can be overridden from the command line) ---
PROJECT_ID = "deloitte-ha"
BUCKET_NAME = "deloitte-ha-raw-data"
LOCATION = "us-central1"
BRONZE_DATASET = "bronze"
MANAGEMENT_DATASET = "management"
RAW_PREFIX = "raw"
MANIFEST_TABLE = "load_manifest"

# Source folder in GCS -> target bronze table.
TABLES = {
    "users": "users",
    "addresses": "addresses",
}

# --- Bronze schemas: the CSV columns plus ingestion_time (added at load) ---
USERS_SCHEMA = [
    bigquery.SchemaField("user_id", "INT64"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("phone", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("ingestion_time", "TIMESTAMP"),
]

ADDRESSES_SCHEMA = [
    bigquery.SchemaField("address_id", "INT64"),
    bigquery.SchemaField("user_id", "INT64"),
    bigquery.SchemaField("street", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("postal_code", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("ingestion_time", "TIMESTAMP"),
]

SCHEMAS = {
    "users": USERS_SCHEMA,
    "addresses": ADDRESSES_SCHEMA,
}

# One row per loaded file; object_uri is what makes reruns skip it.
MANIFEST_SCHEMA = [
    bigquery.SchemaField("object_uri", "STRING"),
    bigquery.SchemaField("table_name", "STRING"),
    bigquery.SchemaField("ingestion_time", "TIMESTAMP"),
    bigquery.SchemaField("job_id", "STRING"),
]

CSV_FILENAME_RE = re.compile(r".+\.csv$", re.IGNORECASE)


# --- Setup: create datasets / tables on first run, no-op afterwards ---

def ensure_dataset(bq: bigquery.Client, dataset_id: str, location: str) -> None:
    dataset_ref = bigquery.Dataset(f"{bq.project}.{dataset_id}")
    dataset_ref.location = location
    bq.create_dataset(dataset_ref, exists_ok=True)
    print(f"Dataset ready: {bq.project}.{dataset_id} ({location})")


def ensure_table(
    bq: bigquery.Client,
    dataset_id: str,
    table_id: str,
    schema: list[bigquery.SchemaField],
    partition_field: str | None = "ingestion_time",
) -> None:
    table_ref = f"{bq.project}.{dataset_id}.{table_id}"
    try:
        bq.get_table(table_ref)
        print(f"Table exists: {table_ref}")
        return
    except NotFound:
        pass

    table = bigquery.Table(table_ref, schema=schema)
    # Daily partitions on ingestion_time, so queries on recent batches scan less.
    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
    bq.create_table(table)
    print(f"Created table: {table_ref}")


# --- Find files to load ---

def list_day_objects(
    gcs: storage.Client,
    bucket_name: str,
    table: str,
    day: date,
) -> list[str]:
    """List CSVs for a calendar day folder (GCS scan scope only)."""
    prefix = f"{RAW_PREFIX}/{table}/ingestion_date={day.isoformat()}/"
    uris: list[str] = []
    for blob in gcs.list_blobs(bucket_name, prefix=prefix):
        # Skip folder placeholders and anything that isn't a CSV.
        if blob.name.endswith("/") or not CSV_FILENAME_RE.match(blob.name):
            continue
        if blob.name.endswith(".loaded"):
            continue
        uris.append(f"gs://{bucket_name}/{blob.name}")
    return sorted(uris)


def already_loaded(
    bq: bigquery.Client,
    management_dataset: str,
    object_uris: list[str],
) -> set[str]:
    """Return the URIs from object_uris that are already in the manifest."""
    if not object_uris:
        return set()

    # Query in chunks to keep the array parameter small.
    manifest = f"`{bq.project}.{management_dataset}.{MANIFEST_TABLE}`"
    loaded: set[str] = set()
    chunk_size = 100
    for i in range(0, len(object_uris), chunk_size):
        chunk = object_uris[i : i + chunk_size]
        job = bq.query(
            f"""
            SELECT object_uri
            FROM {manifest}
            WHERE object_uri IN UNNEST(@uris)
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("uris", "STRING", chunk),
                ]
            ),
        )
        loaded.update(row["object_uri"] for row in job.result())
    return loaded


# --- Load one file and record it ---

def load_object(
    bq: bigquery.Client,
    bronze_dataset: str,
    target_table: str,
    object_uri: str,
    ingestion_time: datetime,
) -> str:
    """
    Load one CSV into a bronze table.

    CSV has no ingestion_time column; we load into a temp table, then INSERT
    into bronze with ingestion_time = load moment.
    """
    dest = f"{bq.project}.{bronze_dataset}.{target_table}"
    # The CSV has every bronze column except ingestion_time.
    csv_schema = [f for f in SCHEMAS[target_table] if f.name != "ingestion_time"]
    csv_columns = ", ".join(f.name for f in csv_schema)

    # Random suffix so concurrent runs never share a temp table.
    temp_table_id = f"_tmp_{target_table}_{uuid.uuid4().hex[:8]}"
    temp_ref = f"{bq.project}.{bronze_dataset}.{temp_table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=csv_schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        # 1. CSV -> temp table.
        load_job = bq.load_table_from_uri(object_uri, temp_ref, job_config=job_config)
        load_job.result()

        # 2. Temp table -> bronze, stamping every row with the same ingestion_time.
        insert_job = bq.query(
            f"""
            INSERT INTO `{dest}` ({csv_columns}, ingestion_time)
            SELECT {csv_columns}, @ingestion_time AS ingestion_time
            FROM `{temp_ref}`
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "ingestion_time", "TIMESTAMP", ingestion_time
                    ),
                ]
            ),
        )
        insert_job.result()
        return load_job.job_id
    finally:
        # Always drop the temp table, even if the load or insert failed.
        bq.delete_table(temp_ref, not_found_ok=True)


def record_manifest(
    bq: bigquery.Client,
    management_dataset: str,
    object_uri: str,
    table_name: str,
    ingestion_time: datetime,
    job_id: str,
) -> None:
    """Mark a file as loaded, so later runs skip it."""
    manifest = f"{bq.project}.{management_dataset}.{MANIFEST_TABLE}"
    rows = [
        {
            "object_uri": object_uri,
            "table_name": table_name,
            "ingestion_time": ingestion_time.isoformat(),
            "job_id": job_id,
        }
    ]
    errors = bq.insert_rows_json(manifest, rows)
    if errors:
        raise RuntimeError(f"Failed to write load_manifest for {object_uri}: {errors}")


# --- Entry point ---

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Append today's GCS CSV batches into BigQuery bronze tables."
    )
    parser.add_argument("--project", default=PROJECT_ID)
    parser.add_argument("--bucket", default=BUCKET_NAME)
    parser.add_argument("--location", default=LOCATION)
    parser.add_argument("--bronze-dataset", default=BRONZE_DATASET)
    parser.add_argument("--management-dataset", default=MANAGEMENT_DATASET)
    parser.add_argument(
        "--date",
        default=None,
        help="Calendar day folder to scan (YYYY-MM-DD, UTC). Default: today UTC.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    # Which day folder to scan: --date, or today (UTC, matching the upload paths).
    if args.date:
        day = date.fromisoformat(args.date)
    else:
        day = datetime.now(timezone.utc).date()

    print(f"Project:         {args.project}")
    print(f"Bucket:          gs://{args.bucket}")
    print(f"Bronze:          {args.project}.{args.bronze_dataset}")
    print(f"Management:      {args.project}.{args.management_dataset}")
    print(f"Scan day folder: ingestion_date={day.isoformat()} (UTC)")
    print()

    bq = bigquery.Client(project=args.project, location=args.location)
    gcs = storage.Client(project=args.project)

    # Make sure datasets and tables exist.
    ensure_dataset(bq, args.bronze_dataset, args.location)
    ensure_dataset(bq, args.management_dataset, args.location)
    ensure_table(
        bq,
        args.management_dataset,
        MANIFEST_TABLE,
        MANIFEST_SCHEMA,
        partition_field=None,
    )
    for target_table, schema in SCHEMAS.items():
        ensure_table(bq, args.bronze_dataset, target_table, schema)

    loaded_count = 0
    skipped_count = 0

    for source_table, target_table in TABLES.items():
        # All CSVs in today's folder for this table.
        uris = list_day_objects(gcs, args.bucket, source_table, day)
        print(f"{source_table}: found {len(uris)} object(s) for {day}")

        if not uris:
            continue

        # Load each new file, then record it in the manifest.
        already = already_loaded(bq, args.management_dataset, uris)
        for uri in uris:
            if uri in already:
                print(f"  skip  {uri}")
                skipped_count += 1
                continue

            # One timestamp per file: used in bronze and in the manifest.
            ingestion_time = datetime.now(timezone.utc)
            print(
                f"  load  {uri} -> {args.bronze_dataset}.{target_table} "
                f"(ingestion_time={ingestion_time.isoformat()})"
            )
            job_id = load_object(
                bq,
                args.bronze_dataset,
                target_table,
                uri,
                ingestion_time,
            )
            # Recorded only after a successful load, so a failed file is retried next run.
            record_manifest(
                bq,
                args.management_dataset,
                uri,
                target_table,
                ingestion_time,
                job_id,
            )
            print(f"        job {job_id}")
            loaded_count += 1

    print()
    print(f"Done. loaded={loaded_count} skipped={skipped_count}")


if __name__ == "__main__":
    main()
