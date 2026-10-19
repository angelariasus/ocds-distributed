#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP=kafka1:9092,kafka2:9092,kafka3:9092

create() {
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 3 \
    --config min.insync.replicas=2 "${@:3}"
}

create extract.tasks 12
create extract.tasks.dlq 3
create extract.results 12

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe
