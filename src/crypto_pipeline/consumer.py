"""Kafka -> BigQuery consumer.

Subscribes to the per-ticker `ticks.*` topics, batches messages, and streams
them into the BigQuery raw table via `insertAll` (tabledata.insertAll — not a
load job, so it lands in near-real-time).

Delivery semantics: at-least-once with best-effort dedup.
  * Offsets are committed ONLY after a batch is successfully inserted, so a crash
    mid-batch replays those messages (never loses them).
  * Each row carries an insertId of `topic:partition:offset`; BigQuery's
    streaming buffer dedups identical insertIds within its dedup window, so the
    common replay case collapses to exactly-once in practice.

Run:  python -m crypto_pipeline.consumer
"""

from __future__ import annotations

import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from crypto_pipeline.bq import ensure_dataset_and_table, get_client
from crypto_pipeline.config import settings

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)
log = logging.getLogger("consumer")


def _ms_to_iso(ms: int | None) -> str | None:
    """Epoch milliseconds -> ISO-8601 UTC string for a BigQuery TIMESTAMP."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


class BigQueryConsumer:
    def __init__(self) -> None:
        self._consumer = Consumer(
            {
                **settings.kafka_common(),  # bootstrap + SASL_SSL auth
                "group.id": settings.consumer_group,
                "auto.offset.reset": settings.consumer_offset_reset,
                # We commit explicitly after a successful BigQuery insert.
                "enable.auto.commit": False,
                "partition.assignment.strategy": "cooperative-sticky",
            }
        )
        self._bq = get_client()
        self._running = True
        self._batch: list[dict[str, Any]] = []
        self._row_ids: list[str] = []
        self._consumed = 0
        self._inserted = 0
        self._insert_errors = 0
        self._last_flush = time.monotonic()

    # --- Message -> BigQuery row ---------------------------------------
    @staticmethod
    def _to_row(tick: dict[str, Any], msg: Any) -> dict[str, Any]:
        return {
            "symbol": tick["symbol"],
            "trade_id": tick.get("trade_id"),
            "price": tick.get("price"),
            "quantity": tick.get("quantity"),
            "quote_quantity": tick.get("quote_quantity"),
            "is_buyer_maker": tick.get("is_buyer_maker"),
            "taker_side": tick.get("taker_side"),
            "trade_time": tick.get("trade_time"),
            "event_time": tick.get("event_time"),
            "ingest_time": tick.get("ingest_time"),
            "trade_ts": _ms_to_iso(tick.get("trade_time")),
            "ingest_ts": _ms_to_iso(tick.get("ingest_time")),
            "kafka_topic": msg.topic(),
            "kafka_partition": msg.partition(),
            "kafka_offset": msg.offset(),
        }

    # --- Batch flush ---------------------------------------------------
    def _flush(self) -> bool:
        """Insert the current batch into BigQuery; commit offsets on success."""
        if not self._batch:
            return True
        errors = self._bq.insert_rows_json(
            settings.bq_raw_table_ref, self._batch, row_ids=self._row_ids
        )
        if errors:
            # Streaming insert reports per-row errors; do NOT commit so the
            # batch is retried on the next poll cycle.
            self._insert_errors += len(errors)
            log.error("BigQuery insert returned %d row errors: %s", len(errors), errors[:3])
            return False

        self._inserted += len(self._batch)
        self._consumer.commit(asynchronous=False)
        log.info(
            "Flushed %d rows -> BigQuery | total inserted=%d consumed=%d",
            len(self._batch),
            self._inserted,
            self._consumed,
        )
        self._batch.clear()
        self._row_ids.clear()
        self._last_flush = time.monotonic()
        return True

    def _due_for_flush(self) -> bool:
        """Flush when the batch is full OR it has been buffering too long.

        The time-based trigger matters when trades arrive steadily but slowly:
        poll() keeps returning messages (never idle), so without it a small
        batch could sit unsent for a long time — bad for a live dashboard.
        """
        if not self._batch:
            return False
        if len(self._batch) >= settings.bq_batch_size:
            return True
        return (time.monotonic() - self._last_flush) >= settings.bq_flush_seconds

    # --- Main loop -----------------------------------------------------
    def run(self) -> None:
        ensure_dataset_and_table(self._bq)
        self._consumer.subscribe(settings.topics)
        log.info("Subscribed to: %s", ", ".join(settings.topics))

        try:
            while self._running:
                # Short poll so the time-based flush stays responsive even
                # while messages are arriving steadily.
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    if self._due_for_flush():
                        self._flush()
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    tick = json.loads(msg.value())
                    self._batch.append(self._to_row(tick, msg))
                    self._row_ids.append(
                        f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
                    )
                    self._consumed += 1
                except (json.JSONDecodeError, KeyError) as exc:
                    log.warning("Skipping bad message at offset %s: %s", msg.offset(), exc)
                    continue

                if self._due_for_flush():
                    self._flush()
        finally:
            self._shutdown()

    def stop(self, *_: Any) -> None:
        log.info("Shutdown requested; flushing final batch...")
        self._running = False

    def _shutdown(self) -> None:
        try:
            self._flush()
        finally:
            self._consumer.close()
        log.info(
            "Final | consumed=%d inserted=%d insert_errors=%d",
            self._consumed,
            self._inserted,
            self._insert_errors,
        )


def main() -> None:
    consumer = BigQueryConsumer()
    signal.signal(signal.SIGINT, consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)
    consumer.run()


if __name__ == "__main__":
    main()
