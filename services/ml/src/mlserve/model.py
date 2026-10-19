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
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

log = logging.getLogger(__name__)

FEATURES = ["monto_referencial", "metodo", "categoria", "departamento", "es_essalud"]
MODEL_PREFIX = "models/lead_time"

# Only processes called at least HORIZON_DAYS before the data cutoff have had time to sign: newer
# ones are right-censored (only the fast ones have a lead time yet) and would bias the model low.
HORIZON_DAYS = 180

TRAINING_SQL = """
SELECT f.monto_referencial, t.metodo, coalesce(i.clasificacion, 'SIN-CLASIFICACION') AS categoria,
       coalesce(e.departamento, '') AS departamento, e.es_essalud, f.lead_time_dias,
       to_date(f.fk_tiempo_convocatoria::text, 'YYYYMMDD') AS fecha_convocatoria
  FROM gold.fact_items f
  JOIN gold.dim_entidad_compradora e ON e.sk_entidad = f.fk_entidad
  JOIN gold.dim_tipo_proceso t ON t.sk_tipo_proceso = f.fk_tipo_proceso
  LEFT JOIN gold.dim_item i ON i.sk_item = f.fk_item
 WHERE f.monto_referencial IS NOT NULL AND f.fk_tiempo_convocatoria IS NOT NULL
"""


@dataclass(frozen=True)
class ModelInfo:
    version: str
    trained_at: str
    training_rows: int
    mae_dias: float
    horizon_dias: int = 0
    cohort_until: str = ""
    signed_within_horizon_pct: float = 0.0


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


def train(frame: pd.DataFrame, horizon: int = HORIZON_DAYS) -> tuple[Pipeline, ModelInfo]:
    """Fits on the uncensored cohort and evaluates on its newest 20% (a time split, not a random one)."""
    frame = frame.copy()
    frame["fecha_convocatoria"] = pd.to_datetime(frame["fecha_convocatoria"])
    cohort_until = frame["fecha_convocatoria"].max() - pd.Timedelta(days=horizon)
    cohort = frame[frame["fecha_convocatoria"] <= cohort_until]
    signed = cohort[cohort["lead_time_dias"].between(0, horizon)].sort_values("fecha_convocatoria").copy()
    signed["es_essalud"] = signed["es_essalud"].astype(int)
    cut = int(len(signed) * 0.8)
    train_part, test_part = signed.iloc[:cut], signed.iloc[cut:]
    pipeline = build_pipeline().fit(train_part[FEATURES], train_part["lead_time_dias"].astype(float))
    mae = float(mean_absolute_error(test_part["lead_time_dias"].astype(float), pipeline.predict(test_part[FEATURES])))
    now = datetime.now(UTC)
    info = ModelInfo(
        version=now.strftime("%Y%m%dT%H%M%SZ"),
        trained_at=now.isoformat(),
        training_rows=len(signed),
        mae_dias=mae,
        horizon_dias=horizon,
        cohort_until=cohort_until.date().isoformat(),
        signed_within_horizon_pct=round(100.0 * len(signed) / max(len(cohort), 1), 1),
    )
    return pipeline, info


def publish(pipeline: Pipeline, info: ModelInfo, bucket: str) -> str:
    s3 = _s3()
    buf = io.BytesIO()
    joblib.dump({"pipeline": pipeline, "info": info.__dict__}, buf)
    key = f"{MODEL_PREFIX}/{info.version}.joblib"
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream")
    s3.put_object(Bucket=bucket, Key=f"{MODEL_PREFIX}/latest", Body=json.dumps({"key": key, **info.__dict__}).encode(), ContentType="application/json")
    log.info("model published: s3://%s/%s (mae %.1f d, time split, %d rows, until %s)", bucket, key, info.mae_dias, info.training_rows, info.cohort_until)
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
