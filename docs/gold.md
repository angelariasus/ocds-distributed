# Gold: the warehouse, transactions and failover

`services/gold` loads the Silver items into a PostgreSQL star schema (the EsSalud model of the
original project, ported from SQL Server) and is where the project meets three classic problems:
keeping a database available when its primary dies, making a multi-step load behave like one
transaction, and making sure two loaders never write at once, even when one of them is a zombie.

## PostgreSQL in high availability

Three PostgreSQL 16 nodes are managed by **Patroni** (Spilo image). Patroni keeps the cluster's
state (who is the leader, the leader lease) in **etcd**, a Raft-replicated key-value store with
three members, and runs the standard protocol: the leader renews its key with a TTL; if it stops
(crash, pause, partition) the key expires and the replicas race to take it; the one that wins
promotes itself and the others re-point their streaming replication to it. Raft gives etcd a
linearizable view of that key, which is what makes the race safe: two nodes can never both believe
they won.

**HAProxy** turns the election into connectivity: port 5000 health-checks Patroni's `/primary`
endpoint on every node and only forwards to the one that answers 200, port 5001 does the same with
`/replica` for read scaling. Clients keep one address and reconnect after a failover.

Measured on 2026-10-19: see the experiments section.

## The load as a saga

```
acquire lease ─▶ read Silver ─▶ load staging ─▶ swap ─▶ cleanup ─▶ release
                                     │            │
                                     └── on failure: drop staging, record the failed load
```

The swap is one PostgreSQL transaction: fence check, dimension upserts, per-year replacement of
the fact partitions, the outbox event and the load record. Production is therefore either untouched
or fully replaced for the loaded years. The steps before it (reading Silver, filling staging
tables) can fail or be retried freely because they touch nothing visible. This is the
saga pattern: local transactions plus compensations instead of a global lock, with the single
irreversible step placed last.

### Outbox instead of two-phase commit

Downstream consumers learn about a load from Kafka (`gold.events`), but PostgreSQL and Kafka cannot
join one transaction. The swap writes the event into `gold.outbox` *in the same transaction* as the
data; a relay process then publishes unpublished rows and marks them published. Delivery is at
least once (a crash between publish and mark re-sends the event), so events carry an id and
consumers deduplicate. Nothing is ever announced that was not committed, and nothing committed goes
unannounced.

## Mutual exclusion with fencing tokens

Only one loader may run at a time. The lease lives in etcd, not in the project's own Dynamo-style
catalog: an eventually consistent store cannot say which of two concurrent acquirers won, a
linearizable one can. Acquisition is a compare-and-swap (create the key only if it does not exist)
tied to a TTL lease that the holder refreshes from a background thread.

A lease alone is not enough. A loader that stalls (long GC pause, `SIGSTOP`, a network hiccup
longer than the TTL) loses its lease without noticing, a second loader acquires it and completes,
and then the first one wakes up and keeps writing. The **fencing token** closes that hole: every
acquisition returns the etcd revision at which the key was created, a number that only grows, and
every swap starts with `gold.assert_fence(token)`, an `UPDATE ... WHERE token <= $1` on a one-row
table. The stalled loader's token is smaller than its successor's, the update matches no row, and
the transaction aborts before touching any data. The database is the last line of defence, not the
lock service.

## Star schema

| Table | Grain | Notes |
|---|---|---|
| `gold.fact_items` | one tender item | partitioned by `anio_fiscal`, foreign keys to every dimension, `load_token` for lineage |
| `gold.dim_tiempo` | one day | `sk = yyyymmdd` |
| `gold.dim_entidad_compradora` | one buyer (RUC) | `es_essalud` flag |
| `gold.dim_proveedor` | one supplier (RUC) | natural / legal person / consortium |
| `gold.dim_item` | classification + normalised description | `es_medicamento` (CUBSO segment 51) and the Petitorio match with its method and score, see [real-data.md](real-data.md) |
| `gold.dim_tipo_proceso` | procurement method + details | |

Derived measures follow the original project: `ratio_sobrecosto_pct` (amendments over contracted
amount) and `lead_time_dias` (call to signature). The view `gold.v_essalud_medicamentos` joins
EsSalud's medicine items with their Petitorio status for `analysis/essalud_medicines.sql`.

## Live experiments

Measured on 2026-10-19 on the compose stack (etcd ×3, PostgreSQL 16 ×3 under Patroni, HAProxy,
lease TTL 20 s).

### Primary failure

`pg2` was the primary. `docker compose kill pg2`:

```
primary was pg2
new primary reachable through haproxy after 30s
  pg1 replica running timeline 1
  pg2 down
  pg3 master  running timeline 2
```

Patroni's leader key expired, `pg3` won the election and promoted itself (timeline 2), HAProxy's
health check moved port 5000 to it, and a load started right after went through the new primary.
When `pg2` was started again it rejoined as a replica on timeline 2 without manual intervention:

```
  pg1 replica running timeline 2
  pg2 replica running timeline 2
  pg3 master  running timeline 2
```

The 30 s are Patroni's defaults (`ttl` 30, `loop_wait` 10, `retry_timeout` 10): the window during
which writes fail is the price of being certain the old primary is really gone, the same trade-off
as the catalog's suspect timeout.

### Load, re-run and compensation

| Run | Result |
|---|---|
| First load (`loader-a`, token 197) | 7,255 fact rows in `gold.fact_items_2026`; dimensions: 1,510 buyers, 3,104 suppliers, 6,309 items, 22 process types, 76 dates; outbox row published and visible on `gold.events` within 5 s; 1.5 s end to end |
| Second load, same data (token 207) | committed; still 7,255 rows and a single `load_token`: the year's partition was replaced, not appended to |
| `FAIL_AT=swap` (token 213) | `failed:swap`, fact unchanged at 7,255 rows, zero staging tables left, failed load recorded |

The Kafka event, as the relay published it:

```json
{"id": 1, "type": "gold.loaded", "created_at": "2026-10-19T05:38:43Z", "token": 197, "years": [2026], "holder": "loader-a", "rows_fact": 7255}
```

### The frozen loader

`loader-a` took the lease (token 215) and was frozen with `docker pause` 5 s later, which stops
its keep-alive thread exactly as a long GC pause or a `SIGSTOP` would. `loader-b` started at the
same time and waited for the lease:

```
05:39:12  loader-a  lease acquired token=215
05:39:17  loader-a  (docker pause)
05:39:33  loader-b  lease acquired token=225          ← 16 s later: loader-a's TTL (20 s) expired
05:39:34  loader-b  swap committed, 7,255 rows
05:39:35  loader-a  (docker unpause)
05:40:03  loader-a  staging loaded for token 215
05:40:03  loader-a  saga aborted at swap: stale fencing token 215 for gold-loader (a newer loader already wrote)
```

`loader-a` never learned that it had lost the lease: its own code path ran to completion, it even
filled staging. The only thing that stopped it from overwriting `loader-b`'s load was the fence
check inside the swap transaction, which saw 215 < 225 and aborted. `gold.fence` ends at token 225
held by `loader-b`, and the fact table carries `load_token = 225` on every row.

A first version of this experiment used `time.sleep` to simulate the stall and proved nothing: the
keep-alive thread kept refreshing the lease while the main thread slept, so `loader-b` simply
waited for the release. Only freezing the whole process reproduces the failure mode the fencing
token exists for.

## Operating it

```sh
docker compose --profile gold up -d                     # etcd ×3, PostgreSQL ×3, HAProxy, outbox relay
docker compose --profile gold --profile gold-jobs run --rm gold-loader            # one load
docker compose --profile gold --profile gold-jobs run --rm -e FAIL_AT=swap gold-loader     # compensation
docker compose --profile gold --profile gold-jobs run --rm -e PAUSE_AFTER_LEASE=45 gold-loader   # stalled loader
docker compose --profile gold exec pg1 patronictl -c /home/postgres/postgres.yml list
docker compose --profile gold kill pg2                  # kill the primary; watch Patroni promote
```

HAProxy statistics: http://localhost:7000.
