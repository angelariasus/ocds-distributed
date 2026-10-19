# Serving: API, load balancing, caching and model serving

The last mile replaces the original project's Power BI with services that any client can call:
a query API over the Gold warehouse, a model-serving service, and a Grafana dashboard. This layer is
where the course's client-server topics live: RPC, service discovery, load balancing, caching and
timeouts.

```
clients ──▶ Envoy (:8090 REST, :8091 gRPC) ──▶ api ×3 ──▶ HAProxy :5001 (PostgreSQL replicas)
                                                  │  ├──▶ Redis Cluster (3 masters + 3 replicas)
                                                  │  └──▶ ml ×2 (gRPC, client-side round robin)
                                       gold.events ┘  (cache invalidation)
```

## One service, two transports

`services/api` (Go) implements the `Query` gRPC service once and exposes the same handlers over
REST/JSON (`protojson`), so a browser and another service get identical answers. Every response
carries a `provenance` block (`served_by`, `cache_hit`, `source`, `latency_ms`): the demo can show
which replica answered and whether the cache or a database replica did the work.

## Discovery and load balancing, twice

**North-south, with a proxy.** Envoy discovers the API replicas through compose DNS (`STRICT_DNS`,
one A record per container, re-resolved every 5 s), balances with least-request, retries on 5xx
and connection failures, applies a 5 s route timeout and **passive health checking**: a replica
with three consecutive failures is ejected for 30 s (outlier detection). Scaling the service or
killing a replica changes the pool without configuration changes.

**East-west, without one.** The API calls the ML service through gRPC's own client-side load
balancer: the target `dns:///ml:9000` resolves to every replica and `round_robin` spreads calls
across them, with a 2 s deadline per call. Two legitimate ways to balance, chosen by who the
client is.

## Reads go to replicas

The API connects to HAProxy port 5001, which only forwards to healthy standbys. The primary is
reserved for the loader; reads scale by adding replicas, and a failover of the primary does not
interrupt reads.

## Cache-aside with generation-based invalidation

Query results are cached in a **Redis Cluster**: 16384 hash slots over three masters, each with a
replica, and a client that follows redirections, so the cache is itself sharded and replicated.
Entries are serialized protobuf keyed by `q:<generation>:<query>:<params>` with a TTL.

Invalidation does not enumerate keys. The API consumes `gold.events`; when a load commits, it
bumps `gold:generation`, every existing key becomes unreachable at once and expires on its own.
Readers refresh the generation at most once per second, so the hot path costs one Redis `GET`.
If Redis is down, the API serves from the database and keeps going (`api_cache_total{outcome="error"}`).

## Model serving

`services/ml` trains a gradient-boosted regressor for the lead time of a process (days from call to
signature) from Gold and publishes it to the object store as an immutable, versioned artifact plus a
`latest` pointer. Serving replicas load the same bytes at start; promoting or rolling back a model
is a pointer change, not a deployment. Each prediction reports the model version and the replica
that served it.

## Dashboard

Grafana reads Gold through the same replica port (datasource `gold`) and Prometheus for the
services; the provisioned dashboard shows the warehouse figures next to the API cache hit ratio,
p99 latency and per-replica request rates.

## Live experiments

Measured on 2026-10-19 with three API replicas, two ML replicas and the six-node Redis Cluster.

### Cache and provenance

Two consecutive `GET /v1/top-buyers?year=2026&limit=3` through Envoy:

```
served_by 70465ba0d512  source replica  cache_hit false  latency_ms 32
served_by 61e82984b113  source cache    cache_hit true   latency_ms 0
```

Different replicas answered, and the second one found the first one's entry in the shared cache:
the cache is a cluster-wide resource, not per process. `GET /v1/buyers/20131257750` (EsSalud):
207 items, 32 suppliers, mean lead time 15.2 days.

### Load balancing and a replica failure

30 requests through Envoy were spread over the three replicas (13 / 10 / 7, least-request). One
replica was then killed with `SIGKILL` and 40 requests were sent immediately: **40 succeeded, 0
failed**. Envoy's statistics afterwards: `outlier_detection.ejections_enforced_total: 2`,
`upstream_rq_retry: 11` — the retry policy absorbed the requests that hit the dead replica before
outlier detection and DNS re-resolution removed it from the pool.

### Invalidation by a Gold load

```
before load:  source cache
gold-loader:  load committed token=255 rows_fact=7255
api logs:     cache invalidated by gold load event 4 generation 2
after load:   source replica
```

The load's outbox event reached every API replica through `gold.events`, each bumped the shared
generation, and the next request for the same key missed and recomputed.

### Model serving

`ml-train` fitted the lead-time model on 1,036 Gold rows with a lead time (MAE 6.1 days on the
held-out 20%) and published `models/lead_time/20261019T055929Z.joblib`; both replicas loaded that
version at start. Predictions through the API alternate between replicas (gRPC round robin):

```
lead_time_dias 21.0  model 20261019T055929Z  served_by a50f0674112c
lead_time_dias 21.0  model 20261019T055929Z  served_by a557c4b77594
direct -> 18.1 days      open -> 21.0 days
```

One ML replica was killed; the next four predictions were all served by the survivor with no
error, the client-side balancer having dropped the dead address. A single `Unavailable: no
children to pick from` was observed right after the ML replicas were recreated, before the API's
DNS resolver had refreshed: the price of discovery through DNS is a refresh window, the same
window Envoy covers with retries on the REST side.

### Dashboard

Grafana's `gold` datasource reads through HAProxy port 5001 (`7255` items on the provisioned
dashboard) next to the API's cache hit ratio and p99 from Prometheus.

## Operating it

```sh
docker compose --profile serve up -d                                   # redis ×6, ml ×2, api ×3, envoy
docker compose --profile serve --profile serve-jobs run --rm ml-train  # train and publish a model
TOKEN=$(bash scripts/mint_token.sh analyst)                           # since phase 8 every call needs a JWT
curl -H "Authorization: Bearer $TOKEN" 'localhost:8090/v1/top-buyers?year=2026&limit=5'
curl -H "Authorization: Bearer $TOKEN" 'localhost:8090/v1/buyers/20131257750'
curl -H "Authorization: Bearer $TOKEN" 'localhost:8090/v1/items?q=paracetamol&limit=5'
curl -H "Authorization: Bearer $TOKEN" -X POST localhost:8090/v1/predict/lead-time -d '{"monto_referencial":250000,"metodo":"open","categoria":"goods","departamento":"LIMA","es_essalud":true}'
docker compose --profile serve kill api-2        # Envoy ejects it; the other replicas take the traffic
```

Envoy admin and statistics: http://localhost:9901. Dashboard: http://localhost:3000.
