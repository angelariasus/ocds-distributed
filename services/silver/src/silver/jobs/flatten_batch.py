"""Batch Silver job: compacted record states → Parquet items partitioned by fiscal year."""

import logging
import os
import time

from pyspark.sql import functions as F

from silver import ocds, session

log = logging.getLogger("silver.flatten_batch")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = session.build("silver-flatten-batch")
    topic = os.environ.get("INPUT_TOPIC", "records.compiled")
    output = os.environ.get("OUTPUT_PATH", "s3a://silver/items")
    started = time.monotonic()

    raw = (
        spark.read.format("kafka")
        .options(**session.kafka_options())
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    states = ocds.latest_states(raw)
    # Scaling experiment only: replicate every state N times under distinct OCIDs so the parallel
    # part of the job is large enough to measure speed-up against the fixed coordination cost.
    amplify = int(os.environ.get("AMPLIFY", "1"))
    if amplify > 1:
        copies = spark.range(amplify).withColumnRenamed("id", "copy")
        states = states.crossJoin(copies).withColumn("ocid", F.concat_ws("#", "ocid", F.col("copy").cast("string"))).drop("copy")
    if amplify == 1:
        states = states.cache()
    n_states = states.count()
    items = ocds.flatten(states)
    items.write.mode("overwrite").partitionBy("anio_fiscal").parquet(output)

    written = spark.read.parquet(output)
    n_items = written.count()
    per_year = (
        written.groupBy("anio_fiscal").agg(F.count("*").alias("items"), F.countDistinct("ocid").alias("ocids")).orderBy("anio_fiscal")
    )
    log.info(
        "flatten done: topic=%s amplify=%d states=%d items=%d executors=%d took=%.1fs",
        topic,
        amplify,
        n_states,
        n_items,
        spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1,
        time.monotonic() - started,
    )
    for row in per_year.collect():
        log.info("  anio_fiscal=%s items=%d ocids=%d", row["anio_fiscal"], row["items"], row["ocids"])
    spark.stop()


if __name__ == "__main__":
    main()
