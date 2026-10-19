"""Lead-time model: train from Gold, store as an immutable versioned artifact, serve from it.

The model is small on purpose (a gradient-boosted regressor on a handful of features); the point
is the operational shape: a training job writes `models/lead_time/<version>.joblib` plus a
`latest` pointer to the object store, and any number of serving replicas load the same bytes, so
rolling a model forward or back is a pointer change, not a deployment.
"""

import io
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime

import boto3
import joblib
import pandas as pd
from botocore.config import Config
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

log = logging.getLogger(__name__)

FEATURES = ["monto_referencial", "metodo", "categoria", "departamento", "es_essalud"]
MODEL_PREFIX = "models/lead_time"

TRAINING_SQL = """
SELECT f.monto_referencial, t.metodo, coalesce(i.clasificacion, 'SIN-CLASIFICACION') AS categoria,
       coalesce(e.departamento, '') AS departamento, e.es_essalud, f.lead_time_dias
  FROM gold.fact_items f
  JOIN gold.dim_entidad_compradora e ON e.sk_entidad = f.fk_entidad
  JOIN gold.dim_tipo_proceso t ON t.sk_tipo_proceso = f.fk_tipo_proceso
  LEFT JOIN gold.dim_item i ON i.sk_item = f.fk_item
 WHERE f.lead_time_dias IS NOT NULL AND f.lead_time_dias BETWEEN 0 AND 730 AND f.monto_referencial IS NOT NULL
"""


@dataclass(frozen=True)
class ModelInfo:
    version: str
    trained_at: str
    training_rows: int
    mae_dias: float


def _s3():
    env = os.environ.get
    return boto3.client(
        "s3",
        endpoint_url=env("S3_ENDPOINT", "http://garage:3900"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="garage",
        config=Config(s3={"addressing_style": "path"}),
    )


def build_pipeline() -> Pipeline:
    features = ColumnTransformer(
        [
            ("num", "passthrough", ["monto_referencial"]),
            ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=5, sparse_output=False), ["metodo", "categoria", "departamento"]),
            ("flag", "passthrough", ["es_essalud"]),
        ]
    )
    return Pipeline([("features", features), ("model", HistGradientBoostingRegressor(max_iter=200, learning_rate=0.05, random_state=7))])


def train(frame: pd.DataFrame) -> tuple[Pipeline, ModelInfo]:
    frame = frame.dropna(subset=["lead_time_dias"]).copy()
    frame["es_essalud"] = frame["es_essalud"].astype(int)
    x, y = frame[FEATURES], frame["lead_time_dias"].astype(float)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=7)
    pipeline = build_pipeline().fit(x_train, y_train)
    mae = float(mean_absolute_error(y_test, pipeline.predict(x_test)))
    version = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return pipeline, ModelInfo(version=version, trained_at=datetime.now(UTC).isoformat(), training_rows=len(frame), mae_dias=mae)


def publish(pipeline: Pipeline, info: ModelInfo, bucket: str) -> str:
    s3 = _s3()
    buf = io.BytesIO()
    joblib.dump({"pipeline": pipeline, "info": info.__dict__}, buf)
    key = f"{MODEL_PREFIX}/{info.version}.joblib"
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream")
    s3.put_object(Bucket=bucket, Key=f"{MODEL_PREFIX}/latest", Body=json.dumps({"key": key, **info.__dict__}).encode(), ContentType="application/json")
    log.info("model published: s3://%s/%s (mae %.1f days, %d rows)", bucket, key, info.mae_dias, info.training_rows)
    return key


def load_latest(bucket: str) -> tuple[Pipeline, ModelInfo]:
    s3 = _s3()
    pointer = json.loads(s3.get_object(Bucket=bucket, Key=f"{MODEL_PREFIX}/latest")["Body"].read())
    blob = s3.get_object(Bucket=bucket, Key=pointer["key"])["Body"].read()
    bundle = joblib.load(io.BytesIO(blob))
    info = ModelInfo(**bundle["info"])
    log.info("model loaded: %s (version %s)", pointer["key"], info.version)
    return bundle["pipeline"], info


def predict(pipeline: Pipeline, monto_referencial: float, metodo: str, categoria: str, departamento: str, es_essalud: bool) -> float:
    row = pd.DataFrame(
        [
            {
                "monto_referencial": monto_referencial,
                "metodo": metodo or "unknown",
                "categoria": categoria or "SIN-CLASIFICACION",
                "departamento": departamento or "",
                "es_essalud": int(es_essalud),
            }
        ]
    )
    return float(max(0.0, pipeline.predict(row)[0]))
