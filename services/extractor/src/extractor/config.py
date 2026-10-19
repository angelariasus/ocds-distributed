import os
import socket
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    brokers: str
    group_id: str
    tasks_topic: str
    results_topic: str
    dlq_topic: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    max_attempts: int
    backoff_seconds: float
    metrics_port: int
    worker_id: str

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ.get
        return cls(
            brokers=env("KAFKA_BROKERS", "localhost:19092"),
            group_id=env("KAFKA_GROUP_ID", "extractor"),
            tasks_topic=env("TASKS_TOPIC", "extract.tasks"),
            results_topic=env("RESULTS_TOPIC", "extract.results"),
            dlq_topic=env("DLQ_TOPIC", "extract.tasks.dlq"),
            s3_endpoint=env("S3_ENDPOINT", "http://localhost:3900"),
            s3_access_key=os.environ["S3_ACCESS_KEY"],
            s3_secret_key=os.environ["S3_SECRET_KEY"],
            s3_bucket=env("S3_BUCKET", "bronze"),
            max_attempts=int(env("MAX_ATTEMPTS", "4")),
            backoff_seconds=float(env("BACKOFF_SECONDS", "2")),
            metrics_port=int(env("METRICS_PORT", "8000")),
            worker_id=env("WORKER_ID", socket.gethostname()),
        )
