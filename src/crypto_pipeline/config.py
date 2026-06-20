"""Centralised configuration loaded from environment / .env file.

Keeping every tunable in one dataclass makes the producer (and later the
consumer) trivial to test and to reason about — no scattered os.getenv calls.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

# Load .env if present; real environment variables always take precedence.
load_dotenv()


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    # --- Kafka (Confluent Cloud) ---
    bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
    )
    topic_prefix: str = os.getenv("KAFKA_TOPIC_PREFIX", "ticks")
    topic_partitions: int = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "3"))
    # Confluent Cloud enforces replication factor 3.
    topic_replication_factor: int = int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "3"))

    # --- Kafka auth (SASL_SSL with a Confluent API key/secret) ---
    kafka_security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    kafka_sasl_mechanism: str = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
    kafka_api_key: str = os.getenv("KAFKA_API_KEY", "")       # SASL username
    kafka_api_secret: str = os.getenv("KAFKA_API_SECRET", "")  # SASL password

    # --- Binance ---
    symbols: list[str] = field(
        default_factory=lambda: _split_csv(
            os.getenv("BINANCE_SYMBOLS", "btcusdt,ethusdt,solusdt")
        )
    )
    # data-stream.binance.vision = Binance's public market-data mirror. Same
    # symbols & @trade schema as stream.binance.com, but not geo-blocked (the
    # main endpoint returns HTTP 451 from US IPs).
    ws_base: str = os.getenv("BINANCE_WS_BASE", "wss://data-stream.binance.vision")

    # --- Producer behaviour ---
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()
    stats_interval_seconds: int = int(os.getenv("STATS_INTERVAL_SECONDS", "10"))

    # --- GCP / BigQuery (Phase 2 consumer) ---
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "crypto-microstructure")
    google_credentials: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    bq_location: str = os.getenv("BQ_LOCATION", "US")
    bq_raw_dataset: str = os.getenv("BQ_RAW_DATASET", "crypto_raw")
    bq_raw_table: str = os.getenv("BQ_RAW_TABLE", "ticks")
    bq_marts_dataset: str = os.getenv("BQ_MARTS_DATASET", "crypto_marts")

    # --- Consumer behaviour ---
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "crypto-bq-consumer")
    consumer_offset_reset: str = os.getenv("KAFKA_OFFSET_RESET", "earliest")
    # Flush a batch to BigQuery when it reaches this many rows...
    bq_batch_size: int = int(os.getenv("BQ_BATCH_SIZE", "500"))
    # ...or when this many seconds have elapsed since the last flush.
    bq_flush_seconds: float = float(os.getenv("BQ_FLUSH_SECONDS", "5"))

    def kafka_common(self) -> dict[str, object]:
        """Shared librdkafka config: bootstrap + auth.

        Adds SASL_SSL credentials unless the protocol is PLAINTEXT (kept as an
        escape hatch for a local broker). Producer/consumer/admin all build on
        this so auth is configured in exactly one place.
        """
        cfg: dict[str, object] = {"bootstrap.servers": self.bootstrap_servers}
        if self.kafka_security_protocol.upper() != "PLAINTEXT":
            if not (self.kafka_api_key and self.kafka_api_secret):
                raise RuntimeError(
                    "KAFKA_API_KEY / KAFKA_API_SECRET must be set for "
                    f"{self.kafka_security_protocol} authentication"
                )
            cfg.update(
                {
                    "security.protocol": self.kafka_security_protocol,
                    "sasl.mechanisms": self.kafka_sasl_mechanism,
                    "sasl.username": self.kafka_api_key,
                    "sasl.password": self.kafka_api_secret,
                }
            )
        return cfg

    def topic_for(self, symbol: str) -> str:
        """Map a symbol (e.g. 'btcusdt') to its Kafka topic ('ticks.btcusdt')."""
        return f"{self.topic_prefix}.{symbol.lower()}"

    @property
    def topics(self) -> list[str]:
        return [self.topic_for(s) for s in self.symbols]

    @property
    def bq_raw_table_ref(self) -> str:
        """Fully-qualified raw table id: project.dataset.table."""
        return f"{self.gcp_project_id}.{self.bq_raw_dataset}.{self.bq_raw_table}"

    @property
    def combined_stream_url(self) -> str:
        """Binance combined-stream URL for the @trade stream of every symbol."""
        streams = "/".join(f"{s.lower()}@trade" for s in self.symbols)
        return f"{self.ws_base}/stream?streams={streams}"


settings = Settings()
