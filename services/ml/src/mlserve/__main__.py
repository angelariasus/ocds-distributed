import argparse
import logging
import os
import socket
from concurrent import futures

import grpc
import pandas as pd
import psycopg
from grpc_health.v1 import health, health_pb2_grpc
from prometheus_client import Counter, Histogram, start_http_server

from ml.v1 import predict_pb2, predict_pb2_grpc

from . import model

log = logging.getLogger("mlserve")

PREDICTIONS = Counter("ml_predictions_total", "Predictions served.")
LATENCY = Histogram("ml_predict_seconds", "Prediction latency.", buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25))


class Predictor(predict_pb2_grpc.PredictorServicer):
    def __init__(self, pipeline, info: model.ModelInfo, served_by: str):
        self.pipeline, self.info, self.served_by = pipeline, info, served_by

    def PredictLeadTime(self, request, context):
        with LATENCY.time():
            days = model.predict(self.pipeline, request.monto_referencial, request.metodo, request.categoria, request.departamento, request.es_essalud)
        PREDICTIONS.inc()
        return predict_pb2.LeadTimeResponse(lead_time_dias=days, model_version=self.info.version, served_by=self.served_by)

    def ModelInfo(self, request, context):
        return predict_pb2.ModelInfoResponse(
            version=self.info.version,
            trained_at=self.info.trained_at,
            training_rows=self.info.training_rows,
            mae_dias=self.info.mae_dias,
            served_by=self.served_by,
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    env = os.environ.get
    parser = argparse.ArgumentParser(prog="mlserve")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("train", help="train from Gold and publish a new model version")
    sub.add_parser("serve", help="serve the latest published model over gRPC")
    args = parser.parse_args()
    bucket = env("MODEL_BUCKET", "gold")

    if args.command == "train":
        with psycopg.connect(env("GOLD_DSN", "postgresql://postgres:postgres@haproxy:5001/postgres"), connect_timeout=10) as conn:
            frame = pd.read_sql(model.TRAINING_SQL, conn)
        log.info("training rows: %d", len(frame))
        pipeline, info = model.train(frame)
        model.publish(pipeline, info, bucket)
        return

    pipeline, info = model.load_latest(bucket)
    served_by = env("INSTANCE_ID", socket.gethostname())
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    predict_pb2_grpc.add_PredictorServicer_to_server(Predictor(pipeline, info, served_by), server)
    health_pb2_grpc.add_HealthServicer_to_server(health.HealthServicer(), server)
    port = f"[::]:{env('GRPC_PORT', '9000')}"
    if env("TLS_CERT"):
        # Mutual TLS: only callers holding a certificate from the internal CA (the API) get through.
        def read(path: str) -> bytes:
            with open(path, "rb") as fh:
                return fh.read()

        creds = grpc.ssl_server_credentials([(read(env("TLS_KEY")), read(env("TLS_CERT")))], root_certificates=read(env("TLS_CA")), require_client_auth=True)
        server.add_secure_port(port, creds)
    else:
        server.add_insecure_port(port)
    start_http_server(int(env("METRICS_PORT", "8000")))
    server.start()
    log.info("serving model %s on :%s as %s (mtls=%s)", info.version, env("GRPC_PORT", "9000"), served_by, bool(env("TLS_CERT")))
    server.wait_for_termination()


if __name__ == "__main__":
    main()
