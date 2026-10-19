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
