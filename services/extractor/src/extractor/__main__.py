import logging

from . import telemetry
from .breaker import CircuitBreaker
from .catalog import Catalog, mtls_credentials
from .config import Settings
from .crawler import RangeCrawler
from .events import Emitter
from .extractor import Extractor
from .limiter import RateLimiter
from .ocds import OcdsHttp
from .storage import S3Store
from .worker import Worker


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    provider = telemetry.setup("extractor", settings.metrics_port)
    store = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_bucket)
    http = OcdsHttp()
    catalog = None
    if settings.catalog_addrs:
        creds = (
            mtls_credentials(settings.catalog_tls_ca, settings.catalog_tls_cert, settings.catalog_tls_key)
            if settings.catalog_tls_cert
            else None
        )
        catalog = Catalog(settings.catalog_addrs, credentials=creds)
    worker = Worker(settings)
    emitter = Emitter(worker.producer, settings.observations_topic, settings.worker_id)
    crawler = RangeCrawler(
        http,
        store,
        RateLimiter(settings.limiter_addr, "ocds-portal", settings.worker_id, settings.limiter_fallback_rate),
        CircuitBreaker("ocds-portal", settings.breaker_failures, settings.breaker_recovery_seconds),
        settings.ocds_base_url,
        page_size=settings.page_size,
        catalog=catalog,
        emitter=emitter,
    )
    extractor = Extractor(
        http,
        store,
        settings.worker_id,
        max_attempts=settings.max_attempts,
        backoff_seconds=settings.backoff_seconds,
        crawler=crawler,
        emitter=emitter,
    )
    try:
        worker.run(extractor)
    finally:
        provider.shutdown()


if __name__ == "__main__":
    main()
