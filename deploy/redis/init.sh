#!/bin/sh
# Forms a Redis Cluster from six nodes: 16384 hash slots split across three
# masters, each with one replica. Idempotent: exits if the cluster already exists.
set -e
NODES="redis1:6379 redis2:6379 redis3:6379 redis4:6379 redis5:6379 redis6:6379"

for n in $NODES; do
  host=${n%%:*}
  until redis-cli -h "$host" ping 2>/dev/null | grep -q PONG; do sleep 1; done
done

if redis-cli -h redis1 cluster info 2>/dev/null | grep -q "cluster_state:ok"; then
  echo "redis cluster already formed"
  redis-cli -h redis1 cluster nodes
  exit 0
fi

redis-cli --cluster create $NODES --cluster-replicas 1 --cluster-yes
redis-cli -h redis1 cluster nodes
