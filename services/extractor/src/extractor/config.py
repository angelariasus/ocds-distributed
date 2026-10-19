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
    ocds_base_url: str
    limiter_addr: str
    limiter_fallback_rate: float
    page_size: int
    breaker_failures: int
    breaker_recovery_seconds: float
    catalog_addrs: list[str]
    observations_topic: str
    catalog_tls_ca: str
    catalog_tls_cert: str
    catalog_tls_key: str

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
            ocds_base_url=env("OCDS_BASE_URL", "https://contratacionesabiertas.oece.gob.pe/api/v1"),
            limiter_addr=env("LIMITER_ADDR", "localhost:7000"),
            limiter_fallback_rate=float(env("LIMITER_FALLBACK_RATE", "0.5")),
            page_size=int(env("PAGE_SIZE", "100")),
            breaker_failures=int(env("BREAKER_FAILURES", "3")),
            breaker_recovery_seconds=float(env("BREAKER_RECOVERY_SECONDS", "30")),
            catalog_addrs=[a.strip() for a in env("CATALOG_ADDRS", "").split(",") if a.strip()],
            observations_topic=env("OBSERVATIONS_TOPIC", "bronze.records"),
            catalog_tls_ca=env("CATALOG_TLS_CA", ""),
            catalog_tls_cert=env("CATALOG_TLS_CERT", ""),
            catalog_tls_key=env("CATALOG_TLS_KEY", ""),
        )
