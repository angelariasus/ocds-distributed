# Silver: distributed processing with Spark

`services/silver` is the first layer that computes over the whole corpus rather than one record at
a time. It runs as PySpark jobs on a standalone Spark cluster (`spark-master` + N `spark-worker`,
compose profile `silver`), reading the compiled records from Kafka and writing Parquet to the
`silver` bucket in Garage through the S3A connector.

## Batch: `flatten_batch`

1. Reads the compacted `records.compiled` topic as a batch source (12 Kafka partitions → 12 Spark
   partitions, so the read itself is parallel).
2. Reduces to the newest state per OCID: compaction is eventual, so a batch read can still contain
   older versions of a key; a window ordered by Lamport time then offset keeps one. This is the
   only wide transformation (a shuffle by OCID) before the flattening.
3. Parses the compiled release with a strict projected schema and flattens it to **one row per
   tender item**, resolving the award and the contract of each item with Spark's higher-order
   array functions (`filter`, `exists`, `aggregate`). The logic is ported from
   `essalud-pipeline`'s `ocds_flattener`, so the Silver grain matches the original project.
4. Writes Parquet partitioned by `anio_fiscal` with dynamic partition overwrite: re-running for a
   subset of years replaces only those partitions.

Horizontal scaling is measured by running the same job with `SPARK_WORKERS=1,2,3` and comparing
wall-clock time; the read and the flattening are embarrassingly parallel, the window is the part
that needs a shuffle.

## Streaming: `stream_metrics`

A Structured Streaming query over the same topic computes per-buyer, per-category daily metrics
(number of processes, referential amount):

- **Event time** is `compiledRelease.date`, not arrival time. A record the portal publishes weeks
  late lands in the window of the day it belongs to.
- **Watermark** (`WATERMARK`, default 7 days) bounds how long a window stays open for late records.
  Once the watermark passes a window's end, the window is finalised and appended to the Parquet
  sink; records that arrive with an event time older than the watermark are dropped. The listener
  logs the watermark and the maximum event time after every micro-batch, which shows the
  difference between a physical clock (arrival) and a logical notion of progress (watermark)
  derived from the data itself.
- **Exactly-once sink**: the Parquet sink with a checkpoint location on S3A records which batches
  were committed, so restarting the job after a crash does not duplicate windows.

## Live experiments

Measured on 2026-10-19 on one laptop (24 vCPUs shared by every container; workers with 2 cores
and 1.5 GB each; 12 Kafka and shuffle partitions).

### Batch flattening

September 2026, 6,625 compiled states → 7,255 tender items in `silver/items/anio_fiscal=2026/`
(12 Parquet files, 1.4 MB). The same job with `AMPLIFY` replicating the states to make the
parallel part larger:

| States | Items | 1 worker | 2 workers | 3 workers |
|---|---|---|---|---|
| 6,625 | 7,255 | 37.8 s | 65.1 s | 72.1 s |
| 265,000 | 290,200 | 44.4 s | 42.8 s | 61.7 s |
| 2,650,000 | 2,902,000 | 47.8 s | — | 73.3 s |

Adding workers never made the job faster, and 2.9 million items cost only 10 s more than 7
thousand on one worker. The flattening itself is cheap (Spark's generated code processes millions
of rows per second per core); what the job pays for is fixed: JVM and executor start-up, the Kafka
read, the shuffle behind the per-key window, the S3A directory commit and the verification
re-read. More executors on the *same physical machine* add scheduling and network shuffle cost
without adding real CPU. This is Amdahl's law as measured rather than as drawn: distribution pays
when the parallel work dominates the coordination, and the only way to know is to measure. On the
full corpus (30–50 GB of JSON across several machines) the parallel part would dominate; on one
laptop it does not, and the honest conclusion is that this stage should run on a single executor
until the data justifies more.

On real data, 24 months of `seace_v3` (160,063 states, 183,492 items) took 78 s on two workers:
in the same range as the amplified runs, as the fixed costs predict.

### Streaming

`stream_metrics` over the same 13,250 compiled states with `WINDOW=1 hour`, `WATERMARK=30 minutes`
and a 15-second trigger, two workers:

```
batch=0 input_rows=1994 watermark=1970-01-01T00:00:00Z event_time_max=2026-10-18T18:50:59Z windows_in_state=[1395] dropped_late=[0]
batch=1 input_rows=1995 watermark=2026-10-18T18:20:59Z event_time_max=2026-10-16T10:21:32Z windows_in_state=[629]  dropped_late=[0]
batch=2 input_rows=1993 watermark=2026-10-18T18:20:59Z event_time_max=2026-10-09T10:02:03Z windows_in_state=[629]  dropped_late=[1606]
batch=3 input_rows=1994 watermark=2026-10-18T18:20:59Z event_time_max=2026-10-18T18:50:59Z windows_in_state=[629]  dropped_late=[1439]
batch=4 input_rows=1995 watermark=2026-10-18T18:20:59Z event_time_max=2026-10-18T18:50:57Z windows_in_state=[629]  dropped_late=[1447]
batch=5 input_rows=1995 watermark=2026-10-18T18:20:59Z event_time_max=2026-10-18T18:50:57Z windows_in_state=[629]  dropped_late=[1541]
batch=6 input_rows=1284 watermark=2026-10-18T18:20:59Z event_time_max=2026-10-18T18:50:56Z windows_in_state=[629]  dropped_late=[1090]
```

Batch 0 opened 1,395 hourly windows with the watermark still at the epoch. Batch 1 advanced the
watermark to 30 minutes behind the newest event time seen (18:50:59 → 18:20:59), which finalised
766 windows at once: they left the state store and were appended to the sink. From batch 2 on the
watermark never moved back, even though batches 1 and 2 had a *lower* maximum event time, and
every record older than it was dropped.

The watermark is derived from the data: 30 minutes behind the newest compiled date seen so far.
Because the compacted topic is not ordered by event time (it is ordered by when the compiler last
touched each record), most of the historical records arrive *after* the watermark has moved past
their hour and are dropped as late, roughly three out of four rows per micro-batch, while 629
hourly windows stayed open in state and the finalised ones were appended to
`silver/metrics/daily_buyer/` (18 Parquet files). A record injected with an event time of
2026-06-01 was dropped the same way. The number that matters is the dropped count:
a watermark is a promise about how late data may be, and the 7-day default exists because the
portal's own publication lag is measured in days, not minutes. Choosing it too tight does not make
the pipeline faster, it makes it silently incomplete.

## Operating it

```sh
docker compose --profile silver up -d --scale spark-worker=2          # cluster, UI at http://localhost:8083
docker compose --profile silver-jobs run --rm silver-batch            # one-shot flatten
docker compose --profile silver-jobs up -d silver-stream              # long-running metrics
```

Resources are small by default (`SPARK_WORKER_CORES=2`, `SPARK_WORKER_MEMORY=1500m`, 1 GB driver
and executor memory) so the cluster fits next to Kafka, Garage and the catalog on one laptop;
raise them for real data.
