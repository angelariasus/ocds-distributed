"""Outbox relay: moves committed events from gold.outbox to Kafka, at least once."""

import json
import logging
import time

import psycopg
from confluent_kafka import Producer

log = logging.getLogger(__name__)


def relay_once(conn: psycopg.Connection, producer: Producer, topic: str, batch: int = 100) -> int:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "SELECT id, event_type, payload, created_at FROM gold.outbox WHERE published_at IS NULL ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED",
            (batch,),
        )
        rows = cur.fetchall()
        for event_id, event_type, payload, created_at in rows:
            value = json.dumps({"id": event_id, "type": event_type, "created_at": created_at.isoformat(), **payload}).encode()
            producer.produce(topic, key=str(event_id).encode(), value=value)
        if rows:
            if producer.flush(30) > 0:
                raise RuntimeError("kafka delivery timed out; outbox rows stay unpublished")
            cur.execute("UPDATE gold.outbox SET published_at = now() WHERE id = ANY(%s)", ([r[0] for r in rows],))
    return len(rows)


def run(dsn: str, brokers: str, topic: str, interval: float) -> None:
    producer = Producer({"bootstrap.servers": brokers, "enable.idempotence": True, "acks": "all"})
    while True:
        try:
            with psycopg.connect(dsn, connect_timeout=10) as conn:
                while True:
                    n = relay_once(conn, producer, topic)
                    if n:
                        log.info("relayed %d events", n)
                    time.sleep(interval)
        except psycopg.OperationalError as err:
            # The primary may be failing over; reconnect through HAProxy after a pause.
            log.warning("database unavailable (%s), retrying", err)
            time.sleep(interval)
