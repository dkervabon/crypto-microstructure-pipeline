"""Binance WebSocket -> Kafka producer.

Subscribes to the Binance combined `@trade` stream for the configured symbols,
normalises each trade into a flat tick record, and publishes it to a per-symbol
Kafka topic (keyed by symbol so all trades for a symbol keep order on one
partition).

Design notes
------------
* One WebSocket connection (Binance "combined stream") serves all symbols.
* confluent-kafka's Producer is driven from the same asyncio loop: after every
  produce() we call poll(0) to service delivery callbacks without blocking.
* Reconnects with capped exponential backoff; flushes Kafka on shutdown so no
  buffered messages are lost on SIGINT/SIGTERM.

Run:  python -m crypto_pipeline.producer
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from typing import Any

import websockets
from confluent_kafka import Producer

from crypto_pipeline.config import settings

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)
log = logging.getLogger("producer")


class TickProducer:
    def __init__(self) -> None:
        self._producer = Producer(
            {
                **settings.kafka_common(),  # bootstrap + SASL_SSL auth
                "client.id": "crypto-tick-producer",
                # Durability-friendly defaults.
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 50,
                "compression.type": "lz4",
                "retries": 10,
            }
        )
        self._running = True
        self._produced = 0
        self._delivered = 0
        self._failed = 0
        self._last_stats = time.monotonic()
        self._last_produced_at_stats = 0

    # --- Kafka delivery callback ---------------------------------------
    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self._failed += 1
            log.error("Delivery failed for %s: %s", msg.topic(), err)
        else:
            self._delivered += 1

    # --- Trade event normalisation -------------------------------------
    @staticmethod
    def _normalize(trade: dict[str, Any]) -> dict[str, Any]:
        """Convert a Binance @trade payload into a flat tick record.

        Binance `m` = "is the buyer the market maker?". If the buyer is the
        maker, the aggressor (taker) is the seller -> the trade lifts the bid =
        a SELL. This taker side is what drives trade-imbalance downstream.
        """
        price = float(trade["p"])
        qty = float(trade["q"])
        is_buyer_maker = bool(trade["m"])
        return {
            "symbol": trade["s"],
            "trade_id": trade["t"],
            "price": price,
            "quantity": qty,
            "quote_quantity": round(price * qty, 8),
            "is_buyer_maker": is_buyer_maker,
            "taker_side": "sell" if is_buyer_maker else "buy",
            "trade_time": trade["T"],  # ms, exchange trade timestamp
            "event_time": trade["E"],  # ms, exchange event timestamp
            "ingest_time": int(time.time() * 1000),  # ms, our wall clock
        }

    # --- Publishing ----------------------------------------------------
    def _publish(self, tick: dict[str, Any]) -> None:
        symbol = tick["symbol"].lower()
        topic = settings.topic_for(symbol)
        try:
            self._producer.produce(
                topic=topic,
                key=tick["symbol"],
                value=json.dumps(tick).encode("utf-8"),
                on_delivery=self._on_delivery,
            )
            self._produced += 1
        except BufferError:
            # Local queue full — let librdkafka drain, then retry once.
            log.warning("Producer queue full; flushing before retry")
            self._producer.poll(1)
            self._producer.produce(
                topic=topic,
                key=tick["symbol"],
                value=json.dumps(tick).encode("utf-8"),
                on_delivery=self._on_delivery,
            )
            self._produced += 1
        # Service delivery callbacks without blocking.
        self._producer.poll(0)

    def _maybe_log_stats(self) -> None:
        interval = settings.stats_interval_seconds
        if interval <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_stats
        if elapsed < interval:
            return
        rate = (self._produced - self._last_produced_at_stats) / elapsed
        log.info(
            "stats | produced=%d delivered=%d failed=%d in_flight=%d | %.1f msg/s",
            self._produced,
            self._delivered,
            self._failed,
            len(self._producer),
            rate,
        )
        self._last_stats = now
        self._last_produced_at_stats = self._produced

    # --- WebSocket loop ------------------------------------------------
    async def _consume_stream(self) -> None:
        """Connect once and pump messages until the connection drops."""
        url = settings.combined_stream_url
        log.info("Connecting to Binance: %s", url)
        async with websockets.connect(
            url, ping_interval=20, ping_timeout=20, max_queue=1024
        ) as ws:
            log.info("Connected. Streaming trades for: %s", ", ".join(settings.symbols))
            async for raw in ws:
                if not self._running:
                    break
                try:
                    envelope = json.loads(raw)
                    # Combined stream wraps payloads as {"stream": ..., "data": {...}}
                    trade = envelope.get("data", envelope)
                    if trade.get("e") != "trade":
                        continue
                    self._publish(self._normalize(trade))
                except (json.JSONDecodeError, KeyError, ValueError) as exc:
                    log.warning("Skipping malformed message: %s", exc)
                self._maybe_log_stats()

    async def run(self) -> None:
        backoff = 1.0
        max_backoff = 30.0
        while self._running:
            try:
                await self._consume_stream()
                backoff = 1.0  # clean exit (shutdown) — no need to back off
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — reconnect on any WS error
                if not self._running:
                    break
                log.warning("Stream error (%s); reconnecting in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
        self._shutdown()

    def stop(self) -> None:
        log.info("Shutdown requested; draining...")
        self._running = False

    def _shutdown(self) -> None:
        remaining = self._producer.flush(10)
        if remaining:
            log.warning("%d messages still in queue after flush timeout", remaining)
        log.info(
            "Final | produced=%d delivered=%d failed=%d",
            self._produced,
            self._delivered,
            self._failed,
        )


async def main() -> None:
    producer = TickProducer()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, producer.stop)
    await producer.run()


if __name__ == "__main__":
    asyncio.run(main())
