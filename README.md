# ocds-distributed

> Una plataforma distribuida de ingeniería de datos para el Portal de Contrataciones Abiertas del
> Perú (estándar OCDS), basada en una arquitectura orientada a eventos con extracción coordinada y
> tolerante a fallos, control adaptativo de carga, almacenamiento replicado y procesamiento
> paralelo, aplicada a la evaluación de la eficiencia de las compras de medicamentos de EsSalud.

Distributed ingestion and analytics platform for Peru's Open Contracting (OCDS) portal,
built as the final project for a Distributed Systems course. It rebuilds the
[essalud-pipeline](https://github.com/angelariasus/essalud-pipeline) Medallion ELT as a set of
cooperating services, with EsSalud medicine procurement as the analytical use case.

The portal publishes ~2.76 million procurement records (2004–2026, two source systems) as 325
monthly bulk files plus a cursor-paginated API. See [docs/ocds-api.md](docs/ocds-api.md) for what
was measured and how it shapes the design.

The platform is designed for the whole corpus and has been validated end to end on two years of
real data: 24 monthly files (October 2024 to September 2026, 160,063 records, 183,492 tender
items) from download to the EsSalud medicine analysis. Crawling, load and chaos experiments ran
against a simulated portal, never the real one. Scope, findings and limits are in
[docs/real-data.md](docs/real-data.md).

## Status: all nine phases complete

```
coordinator (Go) ──▶ Kafka: extract.tasks ──▶ extractor ×N (Python) ──▶ Garage (S3, RF=3)
                                                 │   │
                             gRPC Acquire/Report │   ├──▶ Kafka: extract.results
                                                 ▼   └──▶ Kafka: extract.tasks.dlq
                                        limiter (Go, AIMD)

           extractor ──▶ Toxiproxy ──▶ ocds-mock (Go)        [profile: sim]

           extractor ──▶ catalog ×5 (Go, Dynamo-style KV)     crawl checkpoints

           extractor ──▶ Kafka: bronze.records ──▶ compiler ×2 (Go, CRDT) ──▶ Kafka: records.compiled (compacted)

           records.compiled ──▶ Spark cluster (master + workers): silver-batch, silver-stream ──▶ Garage: silver/ (Parquet)

           silver/ ──▶ gold-loader (saga, lease in etcd, fencing) ──▶ PostgreSQL ×3 (Patroni/etcd, HAProxy) ──▶ outbox relay ──▶ Kafka: gold.events

           clients ──▶ Envoy ──▶ api ×3 (gRPC + REST) ──▶ PostgreSQL replicas / Redis Cluster / ml ×2 (gRPC) ; Grafana dashboard
```

Two task kinds flow through `extract.tasks`:

| Kind | Planned from | Worker behaviour |
|---|---|---|
| `bulk_file` | `/files` index, one per (source, year, month) | Download zip, verify the published SHA-256 of the inner JSON, store |
| `records_range` | Tender-id prefixes (`PLAN_API_PREFIX_LEN` / `PLAN_API_PREFIXES`) | Walk `/recordsAfter` from a synthetic cursor over one prefix, store each page under a deterministic key |

| Component | Role | Distributed-systems topics it exercises |
|---|---|---|
| `services/coordinator` | Three replicas kept consistent by our own Raft (PreVote, CheckQuorum, persistence); only the leader plans, through a replicated schedule that lets a new leader finish a round the old one left half done | Consensus, leader election, replicated state machines, work partitioning |
| `services/extractor` | Consumer group of workers: download or crawl, verify, store, publish result | Horizontal scaling, consumer rebalancing, at-least-once delivery, idempotency, retries with backoff, circuit breaker, dead-letter queue |
| `services/limiter` | gRPC service handing out permits from a token bucket whose rate adapts with additive-increase/multiplicative-decrease on p95 latency and throttling signals | Coordination, RPC, feedback control, graceful degradation (workers fall back to a local rate if it is down) |
| `services/catalog` | Dynamo-style key-value store holding crawl checkpoints: consistent-hashing ring, N/R/W quorums, vector clocks with client-side conflict resolution, hinted handoff, read repair, SWIM-style gossip membership, Merkle anti-entropy, write-ahead log | Partitioning, replication, tunable consistency, failure detection, eventual consistency, reconciliation |
| `services/compiler` | Merges every sighting of an OCID (bulk files, API pages, redeliveries, any order) into one record modelled as a state-based CRDT, stamped with Lamport time; state lives in a compacted Kafka topic that doubles as changelog for restoration after rebalances | CRDTs, logical clocks, convergence, co-partitioning, state restoration, idempotency under at-least-once |
| `services/silver` | PySpark jobs on a standalone Spark cluster: batch flattening of compiled records to one row per tender item (Parquet by fiscal year) and a Structured Streaming query with event-time windows and watermarks | Data-parallel processing, shuffles, horizontal speed-up, stream processing, event time vs. arrival time, late data, exactly-once sinks |
| `services/gold` | Classifies medicines against EsSalud's Petitorio and loads the star schema into a Patroni-managed PostgreSQL cluster as a saga: lease with fencing token in etcd, staging, one-transaction swap with a transactional outbox relayed to Kafka | Leader election and automatic failover, consensus (etcd/Raft), distributed mutual exclusion, fencing, sagas, outbox instead of 2PC |
| `services/api` | Query service over Gold, gRPC and REST from one implementation, behind Envoy (DNS discovery, least-request, retries, outlier ejection); reads from PostgreSQL replicas; cache-aside on a Redis Cluster invalidated by `gold.events` | RPC, service discovery, load balancing, caching, read scaling, timeouts and retries |
| `services/ml` | Lead-time model trained from Gold, published as a versioned artifact to the object store and served by gRPC replicas balanced client-side by the API | Model serving, immutable artifacts, client-side load balancing, deadlines |
| `services/ledger` | Append-only transparency log (RFC 6962 Merkle tree, Ed25519-signed tree heads, inclusion and consistency proofs) attesting every Bronze object and Gold load from Kafka; `ledger audit` is an independent auditor | Integrity, tamper evidence, signed state, distributed trust through auditors |
| PKI, mTLS, JWT | Internal CA and per-service identities; mutual TLS on every service-to-service hop; Envoy verifies ES256 tokens, the API authorizes by role | Secure channels, mutual authentication, authentication vs. authorization, trust boundaries |
| `services/ocds-mock` | Portal replica with measured behaviour (WAF, page-size cap, latency that grows with concurrency); behind Toxiproxy for chaos | Fault injection, repeatable experiments |
| Kafka (3 brokers, KRaft) | Durable task and result log, RF=3, `min.insync.replicas=2` | Replicated log, quorum writes, leader election per partition |
| Garage (4 nodes) | S3-compatible object store for the Bronze layer | Replication, consistent hashing, CRDT-based metadata |
| Prometheus, Grafana, Jaeger | Metrics and distributed tracing | Observability; one trace spans Go → Kafka → Python |

Idempotency: a task is skipped when the object already stored carries the SHA-256 the portal
currently publishes, so re-planning or redelivery never re-downloads unchanged data. A changed
checksum (the portal regenerates the current month daily) triggers a re-fetch.

## Running

Requirements: Docker with Compose v2. Go and Python are only needed for local development; CI and
the images build everything inside containers.

```sh
python scripts/gen_env.py > .env
bash scripts/gen_pki.sh            # internal CA, service certificates, signing keys (deploy/pki, not committed)
docker compose up -d --build
docker compose logs -f coordinator1 coordinator2 coordinator3 extractor
bash scripts/plan_now.sh            # start a planning round now instead of waiting for PLAN_INTERVAL
```

| UI | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Grafana (dashboard `Gold: compras públicas`) | http://localhost:3000 |
| API (REST via Envoy) | http://localhost:8090 |
| Envoy admin | http://localhost:9901 |
| HAProxy stats | http://localhost:7000 |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |

Plan window and worker count live in `.env` (`PLAN_SOURCES`, `PLAN_FROM`, `PLAN_TO`,
`EXTRACTOR_REPLICAS`). Scale workers at runtime with `docker compose up -d --scale extractor=6`.

The portal is a public government service: keep the plan window small during development and
never run load or chaos experiments against it. Use the simulation profile instead, see
[docs/simulation.md](docs/simulation.md). The catalog's design and its live experiments are in
[docs/catalog.md](docs/catalog.md); the record compiler's in [docs/compiler.md](docs/compiler.md); Silver's in [docs/silver.md](docs/silver.md); Gold's in [docs/gold.md](docs/gold.md); the serving layer's in [docs/serving.md](docs/serving.md); security in [docs/security.md](docs/security.md); consensus in [docs/consensus.md](docs/consensus.md); chaos, CAP and benchmarks in [docs/chaos.md](docs/chaos.md); the real-data run and the EsSalud medicine analysis in [docs/real-data.md](docs/real-data.md). What the live runs taught, and what it changed, is in
[docs/lessons.md](docs/lessons.md).

## Development

```sh
# any Go service: coordinator, limiter, ocds-mock, catalog, compiler
docker run --rm -v "$PWD/services/coordinator:/src" -w /src golang:1.26-alpine \
  sh -c 'gofmt -l . && go vet ./... && go test ./...'

# gRPC stubs, after editing proto/
bash scripts/gen_proto.sh

# extractor
docker run --rm -v "$PWD/services/extractor:/app" -w /app python:3.12-slim \
  sh -c 'pip install -q -e ".[dev]" && ruff check . && ruff format --check . && pytest'
```

Commits follow [Conventional Commits](https://www.conventionalcommits.org/).

## Roadmap

1. ~~Distributed extraction~~ — coordinator, workers, Kafka, Garage, observability
2. ~~Adaptive global rate limiter, circuit breaker, cursor-API crawling, simulation and chaos tooling~~
3. ~~Catalog: Dynamo-style key-value store (consistent hashing, quorum, vector clocks, gossip, Merkle anti-entropy)~~
4. ~~Record compiler: logical clocks and CRDT merge of out-of-order OCDS releases~~
5. ~~Silver: Spark cluster, batch and Structured Streaming with watermarks~~
6. ~~Gold: PostgreSQL with automatic failover, saga/outbox loading, fencing tokens~~
7. ~~Query API (gRPC/REST), load balancing, caching, ML serving, dashboard~~
8. ~~Security: internal PKI, mutual TLS, JWT at the edge with role authorization, Merkle transparency log with auditors~~
9. ~~Raft-replicated coordinator, CAP experiments across every replicated system, tunable-consistency benchmarks~~
