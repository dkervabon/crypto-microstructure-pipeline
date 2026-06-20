"""Kafka topic management against Confluent Cloud (SASL_SSL).

Replaces the old docker-exec `kafka-topics` script — the cloud broker is
reached over the network with the same credentials the producer/consumer use.

    python -m crypto_pipeline.admin create   # create per-ticker topics
    python -m crypto_pipeline.admin list     # list topics on the cluster
    python -m crypto_pipeline.admin peek ticks.btcusdt [N]   # print N messages
"""

from __future__ import annotations

import json
import logging
import sys

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from crypto_pipeline.config import settings

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)
log = logging.getLogger("admin")


def _admin() -> AdminClient:
    return AdminClient(settings.kafka_common())


def create_topics() -> None:
    admin = _admin()
    new = [
        NewTopic(
            t,
            num_partitions=settings.topic_partitions,
            replication_factor=settings.topic_replication_factor,
        )
        for t in settings.topics
    ]
    log.info(
        "Creating %d topics (partitions=%d, rf=%d) on %s",
        len(new), settings.topic_partitions, settings.topic_replication_factor,
        settings.bootstrap_servers,
    )
    for topic, fut in admin.create_topics(new).items():
        try:
            fut.result()
            log.info("  ✓ created %s", topic)
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc).lower():
                log.info("  • %s already exists", topic)
            else:
                log.error("  ✗ %s: %s", topic, exc)


def list_topics() -> None:
    md = _admin().list_topics(timeout=15)
    log.info("Topics on %s:", settings.bootstrap_servers)
    for name in sorted(md.topics):
        if not name.startswith("_"):  # hide internal topics
            log.info("  - %s (%d partitions)", name, len(md.topics[name].partitions))


def peek(topic: str, n: int = 5) -> None:
    """Consume up to n messages from a topic for a quick sanity check."""
    consumer = Consumer(
        {
            **settings.kafka_common(),
            "group.id": "crypto-admin-peek",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    log.info("Peeking %d message(s) from %s ...", n, topic)
    seen = 0
    try:
        while seen < n:
            msg = consumer.poll(timeout=10)
            if msg is None:
                log.info("(no more messages)")
                break
            if msg.error():
                continue
            print(json.dumps(json.loads(msg.value()), indent=2))
            seen += 1
    finally:
        consumer.close()


def main(argv: list[str]) -> None:
    cmd = argv[0] if argv else "create"
    if cmd == "create":
        create_topics()
    elif cmd == "list":
        list_topics()
    elif cmd == "peek":
        topic = argv[1] if len(argv) > 1 else settings.topics[0]
        n = int(argv[2]) if len(argv) > 2 else 5
        peek(topic, n)
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
