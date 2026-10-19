import os

from pyspark.sql import SparkSession


def build(app_name: str) -> SparkSession:
    """SparkSession pointed at the cluster master and at Garage through the S3A connector."""
    env = os.environ.get
    builder = (
        SparkSession.builder.appName(app_name)
        .master(env("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.sql.shuffle.partitions", env("SHUFFLE_PARTITIONS", "12"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", env("S3_ENDPOINT", "http://garage:3900"))
        .config("spark.hadoop.fs.s3a.access.key", env("S3_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.secret.key", env("S3_SECRET_KEY", ""))
        .config("spark.hadoop.fs.s3a.endpoint.region", "garage")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    return builder.getOrCreate()


def kafka_options() -> dict[str, str]:
    return {"kafka.bootstrap.servers": os.environ.get("KAFKA_BROKERS", "kafka1:9092,kafka2:9092,kafka3:9092")}
