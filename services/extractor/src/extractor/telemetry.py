from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import Counter, Gauge, Histogram, start_http_server

TASKS = Counter("extractor_tasks_total", "Processed extraction tasks by outcome.", ["status"])
DOWNLOADED_BYTES = Counter("extractor_downloaded_bytes_total", "Bytes downloaded from the OCDS portal.")
TASK_DURATION = Histogram(
    "extractor_task_duration_seconds",
    "End-to-end task duration.",
    buckets=(0.5, 1, 2, 5, 10, 20, 40, 80, 160, 320),
)
IN_PROGRESS = Gauge("extractor_tasks_in_progress", "Tasks currently being processed.")


def setup(service_name: str, metrics_port: int) -> TracerProvider:
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    start_http_server(metrics_port)
    return provider
