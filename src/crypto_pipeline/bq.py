"""BigQuery client + raw-table schema and setup.

The raw `ticks` table is the landing zone for Kafka messages: one row per trade,
plus the Kafka coordinates (topic/partition/offset) for traceability and dedup.
dbt (Phase 3) reads from here to build the VWAP / volatility / imbalance marts.
"""

from __future__ import annotations

import json
import logging
import os

from google.cloud import bigquery
from google.oauth2 import service_account

from crypto_pipeline.config import settings

log = logging.getLogger("bq")

# Raw landing-zone schema. Timestamps are stored both as raw epoch-ms (exactly as
# Binance sent them) and as proper TIMESTAMP columns for partitioning/querying.
RAW_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("trade_id", "INT64"),
    bigquery.SchemaField("price", "FLOAT64"),
    bigquery.SchemaField("quantity", "FLOAT64"),
    bigquery.SchemaField("quote_quantity", "FLOAT64"),
    bigquery.SchemaField("is_buyer_maker", "BOOL"),
    bigquery.SchemaField("taker_side", "STRING"),
    bigquery.SchemaField("trade_time", "INT64"),  # epoch ms (raw)
    bigquery.SchemaField("event_time", "INT64"),  # epoch ms (raw)
    bigquery.SchemaField("ingest_time", "INT64"),  # epoch ms (raw)
    bigquery.SchemaField("trade_ts", "TIMESTAMP"),  # derived from trade_time
    bigquery.SchemaField("ingest_ts", "TIMESTAMP"),  # derived from ingest_time
    bigquery.SchemaField("kafka_topic", "STRING"),
    bigquery.SchemaField("kafka_partition", "INT64"),
    bigquery.SchemaField("kafka_offset", "INT64"),
]


def _credentials() -> service_account.Credentials:
    """Load SA credentials, preferring inline JSON (for hosted envs like Render).

    * GOOGLE_APPLICATION_CREDENTIALS_JSON — the key's JSON contents (Render
      secret env var; no file on disk).
    * GOOGLE_APPLICATION_CREDENTIALS — path to the key file (local dev).
    """
    inline = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if inline:
        return service_account.Credentials.from_service_account_info(json.loads(inline))
    if settings.google_credentials:
        return service_account.Credentials.from_service_account_file(
            settings.google_credentials
        )
    raise RuntimeError(
        "No credentials: set GOOGLE_APPLICATION_CREDENTIALS_JSON or "
        "GOOGLE_APPLICATION_CREDENTIALS"
    )


def get_client() -> bigquery.Client:
    """Build a BigQuery client from the service-account credentials."""
    return bigquery.Client(
        project=settings.gcp_project_id,
        credentials=_credentials(),
        location=settings.bq_location,
    )


def ensure_dataset_and_table(client: bigquery.Client) -> None:
    """Create the raw dataset and partitioned/clustered table if absent."""
    dataset_id = f"{settings.gcp_project_id}.{settings.bq_raw_dataset}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = settings.bq_location
    client.create_dataset(dataset, exists_ok=True)
    log.info("Dataset ready: %s (%s)", dataset_id, settings.bq_location)

    table = bigquery.Table(settings.bq_raw_table_ref, schema=RAW_SCHEMA)
    # Partition by trade day (prunes scans for time-windowed mart queries),
    # cluster by symbol (most filters/aggregations are per-symbol).
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="trade_ts"
    )
    table.clustering_fields = ["symbol"]
    client.create_table(table, exists_ok=True)
    log.info("Table ready: %s (partition=trade_ts day, cluster=symbol)", settings.bq_raw_table_ref)


if __name__ == "__main__":
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    ensure_dataset_and_table(get_client())
