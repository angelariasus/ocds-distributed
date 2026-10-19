#!/usr/bin/env bash
# The same failure, losing a majority of a system's nodes, applied to every
# replicated system in the stack, measuring whether writes stay available.
# Nodes are cut off with `docker network disconnect` (or stopped, for Kafka).
set -uo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
NET=ocds_default
PKI="$(pwd -W 2>/dev/null || pwd)/deploy/pki"
PW=$(grep PG_SUPERUSER_PASSWORD .env | cut -d= -f2)

cut() { for s in "$@"; do docker network disconnect "$NET" "$(docker compose --profile gold ps -q "$s")" 2>/dev/null; done; }
heal() { for s in "$@"; do docker network connect "$NET" "$(docker compose --profile gold ps -q "$s")" 2>/dev/null; done; }
bench() { docker run --rm --network "$NET" -v "$PKI:/pki:ro" -e TLS_CERT=/pki/catalog.crt -e TLS_KEY=/pki/catalog.key -e TLS_CA=/pki/ca.crt ocds-catalog bench -write-ratio 1 -duration 8s -concurrency 8 "$@" | sed 's/^/    /'; }
pgwrite() { # attempts a write on the primary through HAProxy; prints ok/failed
  docker compose --profile gold exec -T -e PGPASSWORD="$PW" pg1 timeout 6 psql -h haproxy -p 5000 -U postgres -tAc \
    "CREATE TABLE IF NOT EXISTS gold.cap_probe (at timestamptz); INSERT INTO gold.cap_probe VALUES (now())" >/dev/null 2>&1 && echo ok || echo failed
}
etcdwrite() { docker compose --profile gold exec -T etcd1 etcdctl --command-timeout=3s put /cap/probe "$(date +%s)" >/dev/null 2>&1 && echo ok || echo failed; }
raftleader() {
  for n in coordinator1 coordinator2 coordinator3; do
    docker run --rm --network "$NET" curlimages/curl:8.10.1 -s --max-time 2 "http://$n:2112/status" 2>/dev/null |
      python -X utf8 -I -c "import json,sys;d=json.load(sys.stdin)['raft'];print(d['id'] if d['role']=='leader' else '')" 2>/dev/null
  done | grep -v '^$' | head -1
}
kafkawrite() { # produces a probe with acks=all and reports whether it can be read back
  local probe="cap-probe-$(date +%s%N)"
  # The console producer exits 0 even when delivery fails, so success is judged by reading the record back.
  echo "$probe" | docker compose exec -T kafka1 timeout 30 /opt/kafka/bin/kafka-console-producer.sh     --bootstrap-server localhost:9092 --topic extract.tasks.dlq --producer-property acks=all     --producer-property delivery.timeout.ms=15000 --producer-property request.timeout.ms=5000 >/dev/null 2>&1
  docker compose exec -T kafka1 timeout 25 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092     --topic extract.tasks.dlq --from-beginning --timeout-ms 10000 2>/dev/null | grep -q "$probe" && echo ok || echo failed
}

echo "== catalog (Dynamo-style, N=3 W=2, sloppy quorum): clients on the majority side"
echo "  all 5 nodes up"; bench -addrs catalog1:7100,catalog2:7100,catalog3:7100 -label baseline
cut catalog4 catalog5
echo "  2 of 5 nodes cut off"; bench -addrs catalog1:7100,catalog2:7100,catalog3:7100 -label "2 cut, W=2"
cut catalog3
echo "  3 of 5 nodes cut off: a majority of the cluster is gone"; bench -addrs catalog1:7100,catalog2:7100 -label "3 cut, W=2"
bench -addrs catalog1:7100,catalog2:7100 -w 3 -label "3 cut, W=3"
heal catalog3 catalog4 catalog5; sleep 15

echo "== etcd (Raft) and PostgreSQL under Patroni"
echo "  baseline: etcd write $(etcdwrite), PostgreSQL write $(pgwrite)"
cut etcd2 etcd3
sleep 5; echo "  2 of 3 etcd members cut off, +5s:  etcd write $(etcdwrite), PostgreSQL write $(pgwrite)"
sleep 30; echo "  +35s (past Patroni's TTL):         etcd write $(etcdwrite), PostgreSQL write $(pgwrite)"
docker compose --profile gold exec -T pg1 curl -s localhost:8008/patroni 2>/dev/null | python -X utf8 -I -c "import json,sys;d=json.load(sys.stdin);print('    pg1 says role',d.get('role'),'state',d.get('state'))" 2>/dev/null
heal etcd2 etcd3
for i in $(seq 1 30); do [ "$(pgwrite)" = ok ] && break; sleep 2; done
echo "  quorum restored: etcd write $(etcdwrite), PostgreSQL write $(pgwrite) after ~$((i*2))s"

echo "== coordinator (our Raft, 3 replicas)"
echo "  baseline leader: $(raftleader)"
cut coordinator2 coordinator3; sleep 6
echo "  2 of 3 cut off: leader seen by coordinator1: '$(raftleader)'"
heal coordinator2 coordinator3; sleep 8
echo "  healed: leader $(raftleader)"

echo "== Kafka (RF=3, min.insync.replicas=2, acks=all)"
echo "  baseline write: $(kafkawrite)"
docker compose kill kafka3 >/dev/null 2>&1; sleep 5
echo "  1 of 3 brokers down: $(kafkawrite)"
docker compose kill kafka2 >/dev/null 2>&1; sleep 5
echo "  2 of 3 brokers down: $(kafkawrite)"
docker compose start kafka2 kafka3 >/dev/null 2>&1; sleep 25
echo "  brokers back: $(kafkawrite)"
