#!/usr/bin/env bash
# entrypoint.sh <job>  → spark-submit src/silver/jobs/<job>.py against the cluster.
set -euo pipefail

job="${1:?job name required (flatten_batch | stream_metrics)}"
shift

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-spark://spark-master:7077}" \
  --name "silver-${job}" \
  --deploy-mode client \
  --conf spark.jars.ivy="${SPARK_IVY}" \
  --packages "${SPARK_PACKAGES}" \
  --py-files /app/silver.zip \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --conf spark.driver.host="$(hostname)" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-1g}" \
  --conf spark.cores.max="${SPARK_CORES_MAX:-4}" \
  "$@" \
  "/app/src/silver/jobs/${job}.py"
