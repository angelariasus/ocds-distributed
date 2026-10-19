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

## Status: phase 2 — adaptive rate limiting and fault isolation

```
coordinator (Go) ──▶ Kafka: extract.tasks ──▶ extractor ×N (Python) ──▶ Garage (S3, RF=3)
                                                 │   │
                             gRPC Acquire/Report │   ├──▶ Kafka: extract.results
                                                 ▼   └──▶ Kafka: extract.tasks.dlq
                                        limiter (Go, AIMD)

           extractor ──▶ Toxiproxy ──▶ ocds-mock (Go)        [profile: sim]
```

Two task kinds flow through `extract.tasks`:

| Kind | Planned from | Worker behaviour |
|---|---|---|
| `bulk_file` | `/files` index, one per (source, year, month) | Download zip, verify the published SHA-256 of the inner JSON, store |
| `records_range` | Tender-id prefixes (`PLAN_API_PREFIX_LEN` / `PLAN_API_PREFIXES`) | Walk `/recordsAfter` from a synthetic cursor over one prefix, store each page under a deterministic key |

| Component | Role | Distributed-systems topics it exercises |
|---|---|---|
| `services/coordinator` | Lists the portal's bulk files, plans one task per (source, year, month), publishes to Kafka | Work partitioning, message-oriented middleware |
| `services/extractor` | Consumer group of workers: download or crawl, verify, store, publish result | Horizontal scaling, consumer rebalancing, at-least-once delivery, idempotency, retries with backoff, circuit breaker, dead-letter queue |
| `services/limiter` | gRPC service handing out permits from a token bucket whose rate adapts with additive-increase/multiplicative-decrease on p95 latency and throttling signals | Coordination, RPC, feedback control, graceful degradation (workers fall back to a local rate if it is down) |
| `services/catalog` | Dynamo-style key-value store holding crawl checkpoints: consistent-hashing ring, N/R/W quorums, vector clocks with client-side conflict resolution, hinted handoff, read repair, SWIM-style gossip membership, Merkle anti-entropy, write-ahead log | Partitioning, replication, tunable consistency, failure detection, eventual consistency, reconciliation |
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
docker compose up -d --build
docker compose logs -f coordinator extractor
```

| UI | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |

Plan window and worker count live in `.env` (`PLAN_SOURCES`, `PLAN_FROM`, `PLAN_TO`,
`EXTRACTOR_REPLICAS`). Scale workers at runtime with `docker compose up -d --scale extractor=6`.

The portal is a public government service: keep the plan window small during development and
never run load or chaos experiments against it. Use the simulation profile instead, see
[docs/simulation.md](docs/simulation.md). The catalog's design and its live experiments are in
[docs/catalog.md](docs/catalog.md).

## Development

```sh
# any Go service: coordinator, limiter, ocds-mock, catalog
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
4. Record compiler: logical clocks and CRDT merge of out-of-order OCDS releases
5. Silver: Spark cluster, batch and Structured Streaming with watermarks
6. Gold: sharded PostgreSQL with automatic failover, saga/outbox loading, fencing tokens
7. Query API (gRPC/REST), load balancing, caching, ML serving, dashboard
8. Security: mTLS, secrets management, Merkle audit ledger
9. Chaos experiments and benchmarks; Raft-based coordinator replication
