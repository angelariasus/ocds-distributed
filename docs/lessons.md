# Lessons from running it

Things the live experiments taught that the design documents did not predict. Each one changed
the code; the measurements that exposed them are in the phase documents.

## Two protection mechanisms can cancel each other out

The first circuit-breaker test never opened a breaker. Connection failures were being reported to
the adaptive rate limiter as errors, so the limiter collapsed to its floor and handed out one
permit every five seconds across eight workers; each worker saw one or two failures during a
45-second outage, never the three it needed. Overload and outage are different signals: served
responses (latency, 429, 5xx) feed the limiter, connection failures only feed the breaker.
([simulation.md](simulation.md))

## Rate floors must be realistic for the fleet size

A floor of 0.2 permits/s looked conservative until eight workers started at once: the first burst
pushed the p95 over the threshold and additive-increase/multiplicative-decrease halved the rate
four times in twelve seconds. The floor is now 1 permit/s and the initial burst 2.

## A restarted node must outrank the rumour of its death

Gossip declared a killed catalog node dead at incarnation 0. When it came back it also started at
incarnation 0, so its "alive" could not override the "dead" its peers were spreading. Nodes now
start with a wall-clock incarnation, and a node that hears it is suspected refutes by incrementing.
([catalog.md](catalog.md))

## Co-partitioning across languages has to be enforced

The compiler set `Record.Partition` on its output and assumed the Go client would honour it; it
re-hashed the key with murmur2 instead, while the Python producer had placed the input with CRC32.
Every changelog partition held keys from several input partitions and a restore after a crash would
have been incomplete. `ManualPartitioner` fixed it; the general rule is to derive the output
partition from the input partition rather than trust two partitioners to agree.
([compiler.md](compiler.md))

## Idempotency needs a stable identity for "the same thing"

Re-crawling an unchanged record produced a new changelog entry every round because the merge
counted a newer `last_seen` as a change. Timestamps are now merged but not material: only new
content, origins or sightings republish a record.

## Durability is a configuration, not a default

Recreating the Kafka containers emptied every topic. The `apache/kafka` image writes its logs to
`/tmp/kraft-combined-logs` unless `KAFKA_LOG_DIRS` is set, so the named volume mounted at
`/var/lib/kafka/data` had never been used. Replication factor 3 and `min.insync.replicas=2`
protect against a broker failing, not against all three brokers being recreated with ephemeral
disks. The same check (does the process actually write where the volume is?) was applied to
Garage and the catalog, which were fine.

## A lease without a fence is not a lock

The stalled-loader test first used `time.sleep` to simulate a pause. It proved nothing: the
keep-alive thread kept refreshing the etcd lease while the main thread slept, so the second loader
just waited. Freezing the container (`docker pause`) reproduced the real failure: the lease
expired, a second loader loaded and committed, the first one woke up with no idea it had lost the
lock and ran all the way to the swap, where only the fencing token check stopped it.
([gold.md](gold.md))

## Measure the number of workers, not just the number of records

Three Spark workers were slower than one on every data size tried on a single laptop, up to 2.9
million rows. The parallel part of the job is cheap and the fixed part (JVM start-up, Kafka read,
shuffle, S3A commit) is not; extra executors on the same CPU add coordination without adding
compute. ([silver.md](silver.md))

## Discovery through DNS has a refresh window

Right after the ML replicas were recreated, one prediction failed with `no children to pick from`:
the API's gRPC resolver still held the old addresses. Envoy covers the same window on the REST side
with retries and outlier ejection, which is why killing an API replica cost zero failed requests
while the ML path showed one. Any DNS-based discovery needs either retries or a push-based
resolver on top. ([serving.md](serving.md))

## A healed partition is not a merged cluster

After a network partition was repaired, the catalog stayed split for good: two nodes each saw a
ring of one, the rest listed them as dead, and the islands kept accepting writes with W=1. Gossip
only probed members it believed alive and contacted seeds only at start-up, so once both sides had
expired each other nothing ever reached across again; and peers that had not yet expired a dead
member kept re-introducing it to those that had. Every earlier failure test had restarted the
failed process, which re-reads its seeds and hides the problem. Probing seeds and dead members
every few rounds, and ignoring news of the death of a node one has never heard of, made the
cluster merge 10 s after the network healed. ([chaos.md](chaos.md))

## A probe that cannot fail proves nothing

The CAP experiment first reported Kafka accepting writes with two of three brokers down. The
console producer exits 0 even when delivery fails, so the probe could not tell success from
failure. Success is now judged by reading the record back, and the result reversed. Every check
in `scripts/` that reports "ok" has to be able to report "failed" too.

## Textbook Raft lets one lonely node depose a healthy leader

The first live partition test left `coordinator3` cut off for fourteen minutes. Following the
basic algorithm, it timed out, became a candidate, incremented its term, failed to win, and did it
again: 348 elections, term 4 → 348. When the network came back, its higher term forced the healthy
leader to step down; in ten seconds the cluster changed leader five times and ended at term 362,
with nothing actually broken. Two additions from Raft's own author fix it: **PreVote** (a node asks
whether it could win before incrementing its term, and peers that still hear a leader say no) and
**CheckQuorum** (a leader that cannot reach a majority steps down instead of believing it still
leads). The unit tests did not catch it, because they never left a node isolated for long; the
live run did in one try. ([consensus.md](consensus.md))

## A valid signature is not evidence of honesty

The tampered ledger in the integrity experiment served a new tree head with a perfectly valid
Ed25519 signature: the operator who rewrites the log also holds the signing key. What caught it
was an auditor that remembered an earlier head and demanded a consistency proof. Signatures prove
*who* said something; only state held by someone else proves they did not change their story.
([security.md](security.md))

## Restores are deployments too

Restoring the honest ledger file from a root shell left it owned by root, and the ledger, which
runs unprivileged, refused to start. Recovery procedures run with different privileges than the
service they recover, and need the same ownership checks as a release.

## Streaming numbers are not JSON numbers

`ijson` yields `Decimal` for every number, and `json.dumps` rejects it; the bulk emitter failed on
its first attempt and the retry hit the idempotent skip path, which hid the failure behind a
"skipped" status. The emitter now parses with `use_float=True` and the worker logs every failed
attempt at warning level.

## A skip path needs to know what was done, not only what was stored

Fixing the emitter was not enough: on the next backfill every file was already in Bronze with the
current checksum, so the idempotent skip returned before parsing and not a single record reached
`bronze.records`. "Stored" and "emitted" are two side effects, and idempotency has to track each
one. The extractor now records emitted checksums in the catalog and re-emits stored files that
never made it downstream. ([real-data.md](real-data.md))

## Plausible totals can still be wrong by an order of magnitude

On real data, EsSalud's medicine purchases summed to 12.9 billion soles of referential value,
half of what the rest of the State spent on medicines. Nothing failed; the number just looked too
large. The flattener, ported from the original project, copied each process's value onto every
item, so a 40-item tender was counted 40 times. Each OCDS item carries its own `totalValue`; using
it brought the figure to 1.7 billion. Unit tests had one item per process and could not see it.
([real-data.md](real-data.md))

## Recent data lies about durations

The lead-time model learned from every signed process, and the newest months only contain the
processes that signed fast: their median lead time was 22 days against 32–56 for older months.
Training only on calls old enough to have finished, and testing on the newest of them rather than
a random sample, is the difference between a model and a description of the data cutoff.
([real-data.md](real-data.md))
