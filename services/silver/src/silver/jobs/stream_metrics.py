"""Streaming Silver job: per-buyer daily procurement metrics with event-time watermarks.

Event time is the compiled release date, not the time the record reached Kafka, so a record that
the portal publishes weeks after the fact lands in the window of the day it belongs to. The
watermark bounds how long a window stays open for such late data; records older than the watermark
are dropped and counted as late.
"""

import logging
import os

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from silver import ocds, session

log = logging.getLogger("silver.stream_metrics")


class ProgressLogger(StreamingQueryListener):
    """Logs the stream's progress: input rows, the watermark and how far event time got."""

    def onQueryStarted(self, event):
        log.info("query started id=%s", event.id)

    def onQueryProgress(self, event):
        p = event.progress
        log.info(
            "batch=%d input_rows=%d watermark=%s event_time_max=%s windows_in_state=%s dropped_late=%s",
            p.batchId,
            p.numInputRows,
            p.eventTime.get("watermark"),
            p.eventTime.get("max"),
            [s.numRowsTotal for s in p.stateOperators],
            [s.numRowsDroppedByWatermark for s in p.stateOperators],
        )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.info("query terminated id=%s exception=%s", event.id, event.exception)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    env = os.environ.get
    spark = session.build("silver-stream-metrics")
    spark.streams.addListener(ProgressLogger())

    raw = (
        spark.readStream.format("kafka")
        .options(**session.kafka_options())
        .option("subscribe", env("INPUT_TOPIC", "records.compiled"))
        .option("startingOffsets", env("STARTING_OFFSETS", "earliest"))
        .option("maxOffsetsPerTrigger", env("MAX_OFFSETS_PER_TRIGGER", "2000"))
        .load()
    )
    states = raw.select(F.from_json(F.col("value").cast("string"), ocds.STATE_SCHEMA).alias("state")).select(
        F.col("state.ocid").alias("ocid"),
        F.col("state.lamport").alias("lamport"),
        F.to_timestamp(F.substring(F.col("state.compiled.date"), 1, 19)).alias("event_time"),
        F.from_json(F.col("state.compiled.body"), ocds.COMPILED_SCHEMA).alias("compiledRelease"),
    )
    buyer = F.element_at(F.filter(F.col("compiledRelease.parties"), lambda p: F.array_contains(p["roles"], F.lit("buyer"))), 1)
    events = states.select(
        "ocid",
        "event_time",
        F.coalesce(buyer["name"], F.lit("(unknown)")).alias("comprador"),
        F.col("compiledRelease.tender.mainProcurementCategory").alias("categoria"),
        F.coalesce(F.col("compiledRelease.tender.value.amount"), F.lit(0.0)).alias("monto_referencial"),
    )

    window = env("WINDOW", "1 day")
    watermark = env("WATERMARK", "7 days")
    metrics = (
        events.withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window).alias("ventana"), "comprador", "categoria")
        .agg(F.count("*").alias("procesos"), F.sum("monto_referencial").alias("monto_referencial"))
        .select(
            F.col("ventana.start").alias("inicio"),
            F.col("ventana.end").alias("fin"),
            "comprador",
            "categoria",
            "procesos",
            "monto_referencial",
        )
    )
    query = (
        metrics.writeStream.format("parquet")
        .outputMode("append")
        .option("path", env("OUTPUT_PATH", "s3a://silver/metrics/daily_buyer"))
        .option("checkpointLocation", env("CHECKPOINT_PATH", "s3a://silver/checkpoints/daily_buyer"))
        .trigger(processingTime=env("TRIGGER", "15 seconds"))
        .queryName("daily_buyer_metrics")
        .start()
    )
    log.info("streaming window=%s watermark=%s", window, watermark)
    query.awaitTermination()


if __name__ == "__main__":
    main()
