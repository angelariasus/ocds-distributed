# Record compiler: CRDTs and logical clocks

`services/compiler` turns the stream of raw sightings produced by the extractors into one
canonical, continuously updated record per procurement process (OCID).

## Why a CRDT

The same OCID is observed many times: in the monthly bulk file, in the cursor-API pages, in the
daily regeneration of the current month, and again whenever a task is redelivered. Those sightings
are produced by different workers and land on Kafka in no particular order. Instead of trying to
order them, the compiler models the record as a **state-based CRDT** whose merge is commutative,
associative and idempotent, so every replica converges to the same state regardless of the order
in which it saw the sightings:

| Field | CRDT | Merge rule |
|---|---|---|
| `compiled` | last-writer-wins register | later `compiledRelease.date` wins; equal dates fall back to the content digest so the choice is deterministic |
| `releases` | map of max-registers keyed by release id | a newer version of the same release id replaces the older one |
| `origins` | G-Set | union |
| `sightings` | G-Set of `(origin, worker)` | union; a redelivered message adds nothing, a new page or file does |
| `first_seen` / `last_seen` | min / max registers | |
| `lamport` | max register | see below |

Sightings are a set rather than a G-Counter on purpose: a counter cannot tell a redelivered
duplicate from a genuinely new observation; a set of `(origin, worker)` pairs can, which is what
makes the whole pipeline idempotent under at-least-once delivery.

`record_test.go` checks the algebra directly: 200 random permutations of the same sightings
produce the same fingerprint, re-merging every sighting reports no change, and
`(a ⊔ b) ⊔ c == a ⊔ (b ⊔ c)`.

## Logical time

Every extractor stamps each sighting with its own **Lamport clock** (`lamport` field and Kafka
header). The compiler keeps one clock per instance: receiving a sighting advances it to
`max(local, sighting) + 1`, and the merged state is stamped with that receive time. The state's
Lamport time therefore orders the versions of a record causally, independent of the wall clocks of
the machines involved; `observed_at` (physical time) is kept next to it so the two can be compared.

## State as a changelog

Input (`bronze.records`) and output (`records.compiled`) are **co-partitioned** by OCID with the
same partition count. The compiler keeps the state of the partitions it owns in memory and writes
every change to the output topic, which has `cleanup.policy=compact`: Kafka keeps only the latest
state per key, so the topic *is* the materialised view of all records and, at the same time, the
changelog from which an instance rebuilds its state.

On a rebalance, the instance that receives partition *p* reads `records.compiled/p` from the
beginning to the current end offset before consuming new input, exactly as Kafka Streams restores
a state store.

Co-partitioning has to be enforced, not assumed. The first version set `Record.Partition` on the
output and let the Go client choose: franz-go ignores that field unless `ManualPartitioner` is
configured, so it re-hashed the key with murmur2 while the Python producer had placed the input
with librdkafka's CRC32 partitioner. Every changelog partition then held keys from several input
partitions: each instance restored states it did not own, counts inflated to ~6,300 keys per
instance for 6 partitions, and a restore after a crash would have missed keys whose changelog
lived elsewhere. The fix is one option, but the lesson is general: when two topics must be
co-partitioned across languages, derive the output partition from the input partition instead of
trusting two partitioners to agree. Offsets of the input are committed only after the merged states have been flushed
to the output, so a crash can at worst replay a few sightings, which the merge absorbs as
duplicates.

## Live experiments

Measured on 2026-10-19 with the simulation profile: the September 2026 archive (6,625 records)
observed through the bulk file and through 67 cursor-API pages, two compiler instances, 12
partitions.

| Check | Result |
|---|---|
| Observations published | 6,625 `bulk` + 6,625 `api` per round (`extractor_observations_total`) |
| Compiled records | 6,625 distinct OCIDs, every one with origins `{api, bulk}` |
| Sightings per OCID | 2 for 6,059 records, 3 for 566 (seen on two different API pages because page boundaries shifted between crawls) |
| Re-observation of unchanged records | reported as `duplicate`, not republished (6,411 duplicates over four rounds) |
| State split | 3,325 + 3,300 keys on two instances owning 6 partitions each; changelog partitions hold 508–585 keys each, 6,625 in total |
| Instance killed with SIGKILL | survivor restored partitions 0–5 from the changelog in 58–69 ms each (1,524–1,755 entries per partition) and ended with exactly 6,625 keys |

Sample compiled state after both origins:

```
ocid     ocds-dgv273-seacev3-1254417
lamport  29261
origins  api:ocds/api/source=seace_v3/prefix=12/1254588.json, bulk:seace_v3-2026-09
compiled 2026-10-17T10:12:13   merges 2
```

## Operating it

```sh
docker compose up -d --scale compiler=2 compiler
docker compose kill compiler-1            # survivor restores the lost partitions from the changelog
docker compose exec kafka1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic records.compiled --from-beginning --property print.key=true
```

Prometheus: `compiler_events_total{outcome=merged|duplicate|invalid}`, `compiler_state_keys`,
`compiler_partitions`, `compiler_restored_keys_total`, `compiler_restore_seconds`,
`compiler_lamport_time`, `extractor_observations_total{kind=bulk|api}`.
