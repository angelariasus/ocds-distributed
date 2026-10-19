import logging

from . import telemetry
from .config import Settings
from .extractor import Extractor
from .ocds import OcdsHttp
from .storage import S3Store
from .worker import Worker


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    provider = telemetry.setup("extractor", settings.metrics_port)
    store = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_bucket)
    extractor = Extractor(
        OcdsHttp(),
        store,
        settings.worker_id,
        max_attempts=settings.max_attempts,
        backoff_seconds=settings.backoff_seconds,
    )
    try:
        Worker(settings, extractor).run()
    finally:
        provider.shutdown()


if __name__ == "__main__":
    main()
