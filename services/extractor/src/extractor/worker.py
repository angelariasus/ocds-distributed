import json
import logging
import signal

from confluent_kafka import Consumer, KafkaException, Message, Producer
from opentelemetry import propagate, trace
from opentelemetry.trace import Status, StatusCode

from .config import Settings
from .extractor import ExhaustedError, Extractor
from .models import Task
from .telemetry import DOWNLOADED_BYTES, IN_PROGRESS, TASK_DURATION, TASKS

log = logging.getLogger(__name__)
tracer = trace.get_tracer("extractor")


def headers_to_carrier(msg: Message) -> dict[str, str]:
    return {key: value.decode() for key, value in (msg.headers() or []) if value is not None}


def carrier_to_headers(carrier: dict[str, str]) -> list[tuple[str, bytes]]:
    return [(key, value.encode()) for key, value in carrier.items()]


class Worker:
    def __init__(self, settings: Settings, extractor: Extractor):
        self.settings = settings
        self.extractor = extractor
        self.running = True
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.brokers,
                "group.id": settings.group_id,
                "client.id": settings.worker_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "partition.assignment.strategy": "cooperative-sticky",
            }
        )
        self.producer = Producer(
            {
                "bootstrap.servers": settings.brokers,
                "client.id": settings.worker_id,
                "enable.idempotence": True,
                "acks": "all",
            }
        )

    def stop(self, *_):
        self.running = False

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        self.consumer.subscribe(
            [self.settings.tasks_topic],
            on_assign=lambda _, parts: log.info("assigned %s", [p.partition for p in parts]),
            on_revoke=lambda _, parts: log.info("revoked %s", [p.partition for p in parts]),
        )
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log.error("consumer error: %s", msg.error())
                    continue
                self.process(msg)
                # Commit only after the outcome is durably published: at-least-once delivery.
                self.consumer.commit(message=msg, asynchronous=False)
        finally:
            self.consumer.close()
            self.producer.flush(10)

    def process(self, msg: Message) -> None:
        ctx = propagate.extract(headers_to_carrier(msg))
        with tracer.start_as_current_span("extract", context=ctx) as span, IN_PROGRESS.track_inprogress():
            span.set_attribute("messaging.kafka.partition", msg.partition())
            span.set_attribute("messaging.kafka.offset", msg.offset())
            try:
                task = Task.from_json(msg.value())
            except (ValueError, KeyError, TypeError) as err:
                TASKS.labels("invalid").inc()
                span.set_status(Status(StatusCode.ERROR, "invalid task"))
                self.publish(self.settings.dlq_topic, msg.key(), self.dead_letter(msg.value(), err, 0))
                return

            span.set_attribute("ocds.task_id", task.task_id)
            with TASK_DURATION.time():
                try:
                    result = self.extractor.handle(task)
                except ExhaustedError as err:
                    TASKS.labels("failed").inc()
                    span.record_exception(err.cause)
                    span.set_status(Status(StatusCode.ERROR, str(err)))
                    self.publish(self.settings.dlq_topic, msg.key(), self.dead_letter(msg.value(), err, err.attempts))
                    return

            TASKS.labels(result.status).inc()
            DOWNLOADED_BYTES.inc(result.size_bytes)
            span.set_attribute("ocds.status", result.status)
            log.info(
                "task %s %s (%d bytes, %d pages, %d records, %d attempts)",
                task.task_id,
                result.status,
                result.size_bytes,
                result.pages,
                result.records,
                result.attempts,
            )
            self.publish(self.settings.results_topic, msg.key(), result.to_json())

    def dead_letter(self, raw: bytes, err: Exception, attempts: int) -> bytes:
        return json.dumps(
            {
                "task": raw.decode(errors="replace"),
                "error": str(err),
                "attempts": attempts,
                "worker_id": self.settings.worker_id,
            }
        ).encode()

    def publish(self, topic: str, key: bytes | None, value: bytes) -> None:
        carrier: dict[str, str] = {}
        propagate.inject(carrier)
        errors = []
        self.producer.produce(
            topic,
            key=key,
            value=value,
            headers=carrier_to_headers(carrier),
            on_delivery=lambda err, _: err and errors.append(err),
        )
        if self.producer.flush(30) > 0 or errors:
            raise KafkaException(errors[0] if errors else "delivery timed out")
