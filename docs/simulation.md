# Simulation and chaos experiments

The real portal is a public government service, so every load or failure experiment runs
against `services/ocds-mock`, a replica of the portal API fed with archives previously stored
in Bronze, behind [Toxiproxy](https://github.com/Shopify/toxiproxy).

## Setup

```sh
# one or more monthly archives named <source>-<year>-<month>.json.zip
mkdir -p data/sample
cp <bronze export>/seace_v3-2026-09.json.zip data/sample/

OCDS_BASE_URL=http://toxiproxy:8081/api/v1 \
PLAN_FROM=2026-09 PLAN_TO=2026-09 \
PLAN_API_SOURCES=seace_v3 PLAN_API_PREFIXES=12 \
docker compose --profile sim up -d --build
```

The mock reproduces the measured portal behaviour: `403` for generic user agents, `size > 100`
returns `404`, and service time grows with concurrency (`MOCK_BASE_LATENCY` +
`MOCK_PER_INFLIGHT_LATENCY` × in-flight requests). `MOCK_THROTTLE_ABOVE=n` additionally returns
`429` beyond `n` concurrent requests.

## Experiments

### Adaptive rate limiting

Scale workers beyond what the upstream tolerates and watch the limiter back off:

```sh
MOCK_PER_INFLIGHT_LATENCY=700ms LIMITER_LATENCY_HIGH=2s \
docker compose --profile sim up -d --scale extractor=8 ocds-mock limiter extractor
docker compose restart coordinator   # re-plans the same tasks; stored pages are overwritten idempotently
```

Prometheus series to plot: `limiter_rate_permits_per_second`, `limiter_window_p95_seconds`,
`ocdsmock_inflight_requests`, `rate(extractor_pages_total[30s])`,
`histogram_quantile(0.5, rate(extractor_permit_wait_seconds_bucket[30s]))`.

Measured on 2026-10-19 with 8 workers, 100 `records_range` tasks (`PLAN_API_PREFIX_LEN=2`),
`MOCK_PER_INFLIGHT_LATENCY=400ms`, `LIMITER_LATENCY_HIGH=2s`, sampled every 5 s:

| t (s) | rate (permits/s) | upstream p95 (s) | in-flight | permit wait p50 (s) |
|---|---|---|---|---|
| 25 | 1.7 | 0.31 | 1 | 2.0 |
| 30 | 1.7 | 0.31 | 6 | 2.0 |
| 35 | 1.1 | 2.41 | 6 | 0.26 |
| 45 | 0.2 | 4.51 | 3 | 0.01 |
| 55 | 1.2 | 0.31 | 0 | 5.0 |
| 65 | 1.7 | 1.71 | 3 | 0.01 |
| 75 | 0.2 | 2.41 | 1 | 0.01 |
| 85 | 2.2 | 1.01 | 1 | 2.75 |
| 95 | 1.1 | 1.71 | 2 | 0.13 |
| 115 | 3.6 | 0.31 | 1 | 0.19 |
| 125 | 5.1 | 0.32 | 1 | 0.01 |

The rate grows by 0.5 permits/s per healthy window and halves whenever the p95 crosses the
threshold, which keeps the upstream's in-flight count bounded instead of letting eight workers
pile up. All 100 tasks completed; the dead-letter topic stayed empty.

### Circuit breaker

Cut the upstream for a while, then restore it:

```sh
# fail every connection
curl -s -X POST localhost:8474/proxies/ocds -d '{"enabled": false}'
# or add 10 s of latency to every response
curl -s -X POST localhost:8474/proxies/ocds/toxics \
  -d '{"name":"slow","type":"latency","attributes":{"latency":10000}}'

# restore
curl -s -X POST localhost:8474/proxies/ocds -d '{"enabled": true}'
curl -s -X DELETE localhost:8474/proxies/ocds/toxics/slow
```

Watch `extractor_breaker_state` (0 closed, 1 open, 2 half-open) and
`extractor_breaker_transitions_total`. Workers pause while the breaker is open and resume with a
single probe request after `BREAKER_RECOVERY_SECONDS`.

Measured on 2026-10-19 with 8 workers and 100 tasks in flight, upstream disabled for 60 s
(`BREAKER_FAILURES=3`, `BREAKER_RECOVERY_SECONDS=30`), breaker state sampled every 5 s per worker
(`0` closed, `1` open, `2` half-open; cut at t≈15 s, restored at t≈75 s):

```
worker 1   000000000011111111111100000000000000
worker 2   000000000111111111111220000000000000
worker 3   000000000111111111111000000000000000
worker 4   000000000111111111111220000000000000
worker 5   000000000011111111111100000000000000
worker 6   000000000111111111111000000000000000
worker 7   000000000111111111111000000000000000
worker 8   000000000011111111111100000000000000
```

Every worker opened its breaker 15–22 s after the cut, paused, re-opened on the first failed
probe while the upstream was still down, and closed again on the first successful probe after
the restore. All 100 tasks completed and the dead-letter topic stayed empty.

Outages and overload are handled by different mechanisms on purpose. Responses the upstream did
serve (latency, `429`, `5xx`) feed the limiter; connection failures do not, because the upstream
gave no load signal and an earlier version that reported them starved every worker down to the
rate floor, which in turn kept each worker's breaker from ever seeing enough failures to open.

### Limiter outage

```sh
docker compose stop limiter
```

Workers log `limiter unavailable` and continue at `LIMITER_FALLBACK_RATE` permits per second
each (`extractor_limiter_fallbacks_total`). Starting the limiter again restores global control
without restarting workers.

### Worker crash

```sh
docker compose kill -s KILL ocds-extractor-2
```

The consumer group rebalances, the in-flight task is redelivered to another worker (offsets are
committed only after the result is published) and already stored pages are overwritten with
identical content.
