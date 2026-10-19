# Chaos, CAP and benchmarks

The earlier phases each tested their own failure modes. This one applies the *same* failure to
every replicated system in the stack and compares what they do, then measures the price of the
consistency knobs. Scripts: `scripts/cap_experiment.sh`, `scripts/raft_experiments.sh`,
`catalog bench`.

## The same failure everywhere: losing a majority

A partition forces a choice between answering (availability) and answering only what is known to
be agreed (consistency). The project contains systems built on both sides of that choice, so the
same experiment, cutting off a majority of a system's nodes, shows both behaviours side by side.
Measured on 2026-10-19 with `docker network disconnect` (Kafka brokers were killed instead).

| System | Design | Majority lost | Writes | What happened |
|---|---|---|---|---|
| Catalog (Dynamo-style, N=3, W=2) | AP: sloppy quorum, hinted handoff | 3 of 5 nodes cut | **accepted**, 0 % errors | p95 latency 1.6 ms → **2.0 s**: each write waited for the RPC timeout to the unreachable replicas before using stand-ins |
| Catalog, same partition, client asks W=3 | per-request consistency | 3 of 5 nodes cut | **rejected**, 100 % errors | the client chose consistency over availability for that request |
| etcd (Raft) | CP | 2 of 3 members cut | **rejected** immediately | no leader without a majority |
| PostgreSQL under Patroni | CP through its DCS | etcd majority lost | accepted for ~30 s, then **rejected** | Patroni cannot renew the leader key in etcd, so after its TTL the primary demotes itself, although every database was healthy; writes resumed ~10 s after etcd regained quorum |
| Coordinator (our Raft) | CP | 2 of 3 replicas cut | no rounds planned | no leader; a leader came back as soon as the network healed |
| Kafka (RF=3, `min.insync.replicas=2`, `acks=all`) | CP for acknowledged writes | 1 of 3 brokers down | **accepted** | ISR of 2 still meets `min.insync` |
| | | 2 of 3 brokers down | **rejected** | ISR of 1 < 2; even topic metadata was unavailable, because in KRaft combined mode the same three nodes are also the controller quorum |

Three things stand out.

**Availability has a latency.** The catalog kept every write with a majority gone, but each one
took two seconds instead of under two milliseconds, until gossip declared the unreachable nodes
dead and the ring stopped routing to them. PACELC says it: even when the system chooses
availability, the partition is paid for somewhere, here in latency.

**CP systems chain.** PostgreSQL never lost a node, yet it stopped accepting writes, because the
component that tells it who the primary is (Patroni, through etcd) chose consistency. A database's
availability is bounded by its consensus service's.

**The probe can lie.** The first Kafka run reported writes succeeding with two brokers down. The
console producer exits 0 even when delivery fails; the script now judges success by reading the
record back, and the result reversed.

## A partition that healed but did not merge

After the partition experiment the catalog benchmark showed about 40 % errors on every operation
that needed two replicas. Two nodes had become islands: each saw a ring of one, and the majority
listed them as dead. Gossip only probed members it considered alive, so once both sides had
expired each other (suspect timeout plus dead retention, under three minutes) nothing ever tried
again, and seeds were only contacted at start-up. The phase 3 experiment had not shown it because
that node was restarted, and a restarted node re-reads its seeds. Worse, the islands accepted
writes with W=1, so the data diverged.

Two changes fixed it, both standard in SWIM implementations: every few rounds a node also probes
one seed it does not see as alive and one member it has declared dead (`ReconnectEvery`), and a
node ignores gossip about an *unknown* member whose news is that it is dead, which was letting
peers re-introduce expired members forever. The live verification is below.

Live verification after the fix: `catalog3` and `catalog5` cut off for 170 s, the same key
written on both sides with W=1 while apart.

```
after 170 s cut off:  catalog1 ring=3   catalog3 ring=1   catalog5 ring=1      (both sides forgot each other)
healed, +10 s:        catalog1 ring=5   catalog3 ring=5   catalog5 ring=5      (one transient 4/5 at +20 s, then stable)
read diverge/k, R=3:  2 siblings   majority-side {catalog1:1}   island-side {catalog3:1}
benchmark R=2 W=2:    0 % errors (was ~40 % before the fix)
```

The divergent writes were not lost or silently overwritten: anti-entropy carried each side's
version to the other, and because neither vector clock descends from the other the read returns
both for the client to resolve. That is eventual consistency doing exactly what it promises.

## Tunable consistency: what each quorum costs

`catalog bench` on the healthy five-node cluster, 16 concurrent clients, 50 % writes, 10 s per
setting, keys spread over all coordinators:

| Setting | Read ok/s | Read p50 / p99 | Write ok/s | Write p50 / p99 | Guarantee |
|---|---|---|---|---|---|
| R=1 W=1 | 5,016 | 1.0 / 5.4 ms | 4,990 | 1.6 / 6.6 ms | none beyond eventual |
| R=2 W=2 | 3,413 | 1.4 / 9.0 ms | 3,392 | 2.2 / 10.9 ms | R+W > N: reads see the latest acknowledged write |
| R=3 W=3 | 3,760 | 1.4 / 5.3 ms | 3,798 | 2.2 / 7.4 ms | every replica on every operation; one slow node slows everything |
| R=1 W=3 | 3,833 | 0.8 / 4.0 ms | 3,826 | 2.8 / 9.1 ms | read-optimised: fast reads, writes pay for all replicas |
| R=3 W=1 | 3,561 | 2.2 / 10.2 ms | 3,576 | 1.4 / 6.4 ms | write-optimised: the mirror image |

Asking for one replica is about 40 % faster in throughput than any setting that waits for more.
Beyond that the differences are small and partly noise, because the coordinator always sends
every operation to all N replicas and the quorum only decides how many answers it waits for: on one
host all replicas answer within microseconds of each other. On a real network, waiting for the
slowest of three would separate R=3/W=3 from R=2/W=2 clearly; the partition experiment above shows
the extreme case, where one unreachable replica turns milliseconds into seconds. The asymmetric
rows are the useful ones in practice: R=1/W=3 suits data read far more than written (the Gold
dimensions), R=3/W=1 suits write-heavy, read-rarely data (crawl checkpoints).
