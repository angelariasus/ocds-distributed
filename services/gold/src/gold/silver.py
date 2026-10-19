"""Reads the Silver items written by Spark (Parquet partitioned by anio_fiscal) from Garage."""

import logging
import os
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.config import Config

log = logging.getLogger(__name__)


def _client():
    env = os.environ.get
    return boto3.client(
        "s3",
        endpoint_url=env("S3_ENDPOINT", "http://garage:3900"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="garage",
        config=Config(s3={"addressing_style": "path"}),
    )


def read_items(path: str, bucket: str | None = None) -> pd.DataFrame:
    bucket = bucket or os.environ.get("SILVER_BUCKET", "silver")
    s3 = _client()
    frames = []
    with tempfile.TemporaryDirectory() as tmp:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=path.rstrip("/") + "/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".parquet"):
                    continue
                local = Path(tmp) / key.replace("/", "_")
                s3.download_file(bucket, key, str(local))
                frame = pq.read_table(local).to_pandas()
                # Hive-style partition directories carry the column that is not stored in the file.
                for part in key.split("/"):
                    if "=" in part:
                        column, value = part.split("=", 1)
                        frame[column] = int(value) if value.isdigit() else value
                frames.append(frame)
    if not frames:
        raise RuntimeError(f"no parquet files under s3://{bucket}/{path}")
    items = pd.concat(frames, ignore_index=True)
    log.info("silver items read: %d rows from %d files", len(items), len(frames))
    return items
