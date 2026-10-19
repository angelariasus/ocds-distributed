# Catalog: a Dynamo-style key-value store

`services/catalog` keeps small, frequently updated state that must survive worker crashes and
node failures: crawl checkpoints today, task leases and metadata later. It follows the design of
Amazon's Dynamo paper, implemented from scratch in Go so every mechanism is visible.

## Data model

A key maps to one or more **versions**. Each version carries a **vector clock** with one counter
per node that coordinated a write to that key. When a client writes, it passes back the
`context` it received on its last read; the coordinator increments its own counter and the new
clock descends from everything the client had seen. Versions whose clocks are ordered replace
each other; versions that are **concurrent** (neither descends from the other) are both kept as
siblings and returned together. Resolving them is the client's job: it writes a new value with the
merged context, which descends from both. The extractor resolves checkpoint conflicts by keeping
the most advanced page count.

## Placement

Every node hashes `node#i` for `i < 128` virtual points onto a 64-bit ring (SHA-1 truncated; a
weak hash such as FNV left one node with 75% of the keys). A key's **preference list** is the
first N distinct nodes clockwise from its hash. Adding a node moves only ~1/n of the keys
(measured 16.7% of 50,000 keys for the sixth node, `ring_test.go`).

## Quorums

| Setting | Default | Meaning |
|---|---|---|
| N | 3 | Replicas per key |
| R | 2 | Replicas that must answer a read |
| W | 2 | Replicas that must acknowledge a write |

R + W > N gives read-your-writes consistency under normal operation; clients may override R and
W per request to trade latency for consistency (`-r 1`, `-w 3` in `catalog ctl`).

**Sloppy quorum and hinted handoff**: when a replica is unreachable the coordinator writes to the
next node on the ring instead, tagged with the replica it stands in for. The stand-in keeps the
write as a *hint* and hands it back every `HANDOFF_INTERVAL` until the replica answers. A write
therefore succeeds as long as any W nodes in the cluster are up. `HINTED_HANDOFF=false` switches to
a strict quorum, in which the same failure returns `Unavailable`.

**Read repair**: a read fans out to all N replicas, answers after R, merges the versions and
asynchronously pushes the merged set to any replica that returned something older.

## Membership and failure detection

Nodes learn about each other through **gossip** in the SWIM style. Every second a node pings one
random peer, piggybacking its full membership view; the peer merges it and answers with its own.
A failed ping triggers up to two **indirect probes** through other peers, so a flaky link between
two nodes does not get a healthy node declared dead. After direct and indirect probes fail the
target becomes *suspect*; `SUSPECT_TIMEOUT` later it becomes *dead* and leaves the ring. A node
that hears it is suspected refutes the rumour by incrementing its **incarnation**, which outranks
older gossip about it.

## Anti-entropy

Hints cover short outages; **Merkle trees** cover everything else (a node that was down longer
than the hint retention, or a lost message). Every `ANTI_ENTROPY_INTERVAL` a node picks a random
alive peer and builds a tree over the keys *both* nodes replicate. The two exchange hashes level
by level (1 root, 2, 4, … 1024 leaves) and descend only into mismatching branches, then swap the
keys of the differing buckets and merge them with the same vector-clock rules as a write.

## Durability

Each accepted write is appended as a JSON line to `/data/wal.jsonl`. On start the node replays the
log and rejoins with its previous state; a torn final line from a crash is skipped.

## Operating it

```sh
docker compose exec catalog1 /catalog ctl status                 # members, ring, N/R/W, keys, pending hints
docker compose exec catalog1 /catalog ctl put crawl/x '{"pages":1}'
docker compose exec catalog3 /catalog ctl get crawl/x            # versions, clocks, opaque context
docker compose exec catalog3 /catalog ctl put crawl/x '{"pages":2}' <context>
```

Prometheus: `catalog_members{state}`, `catalog_ops_total{op,outcome}`, `catalog_op_seconds`,
`catalog_hints_pending`, `catalog_hints_delivered_total`, `catalog_read_repairs_total`,
`catalog_anti_entropy_keys_total`, `catalog_conflicting_keys`, `catalog_keys`.

## Live experiments

Measured on 2026-10-19 on the five-node compose cluster (N=3, R=2, W=2, 128 virtual nodes,
`SUSPECT_TIMEOUT=10s`, `ANTI_ENTROPY_INTERVAL=30s`).

### Conflicting writes

Two coordinators wrote `crawl/demo` from the same context; a third read both siblings and
resolved them:

```
siblings: 2
   {"pages":2,"by":"catalog2"}  {catalog1:1, catalog2:1}
   {"pages":2,"by":"catalog4"}  {catalog1:1, catalog4:1}
after resolution: 1 version {"pages":3,"by":"resolver"}  {catalog1:1, catalog2:1, catalog3:1, catalog4:1}
```

### Node failure, hinted handoff, rejoin and anti-entropy

`catalog2` was killed, 40 fresh keys were written through `catalog1` with W=2, then `catalog2`
was started again:

| Moment | catalog1 | catalog2 | catalog3 | catalog4 | catalog5 | Notes |
|---|---|---|---|---|---|---|
| Baseline | 38 | 44 | 41 | 33 | 30 | 5 members alive |
| 8 s after the writes | 64 (4 hints) | down | 70 | 56 | 49 | `catalog2` already *dead*, ring has 4 nodes |
| 15 s after restart | 64 | 57 | 70 | 56 | 49 | WAL replayed 44 keys; 4 hints delivered; peers' anti-entropy pushed 9 keys |
| 60 s after restart | 64 | 67 | 70 | 56 | 49 | anti-entropy repaired 19 keys in total; all 40 keys readable with R=3 |

Writes issued in the ~10 s before gossip declared `catalog2` dead were hinted to the next node on
the ring; writes issued afterwards went to the reshaped ring (four nodes) and reached `catalog2`
only through Merkle anti-entropy once it rejoined. `catalog2` ended up holding 23 of the 40 new
keys, its expected 3/5 share. No write failed at any point.

The `hinted` counter in a `Put` response only counts hints that contributed to the quorum. A dead
replica's RPC usually fails *after* the two healthy acks have already satisfied W=2, so the hint is
stored in the background and shows up in `catalog_hints_pending` rather than in the response.

### Restart

A node restarted from its write-ahead log rejoined with its previous keys and an incarnation
higher than the one its peers had declared dead, so gossip readmitted it within one round.

### Worker crash during a crawl

With the limiter capped at one page per second, a `records_range` task of 67 pages was started on
three extractor workers and the worker holding it was killed with `SIGKILL` 39 s in:

```
03:56:16  extractor-1  task api-seace_v3-12 crawl starting at page 1
03:56:55  (extractor-1 killed)
03:57:38  consumer group rebalanced: partitions reassigned to the two survivors
03:57:39  extractor-3  task api-seace_v3-12 crawl starting at page 41
03:58:04  extractor-3  task api-seace_v3-12 crawled (67 pages, 6625 records)
```

Kafka redelivered the task after the session timeout, and the new worker read the checkpoint
`crawl/api-seace_v3-12/<planned_at>` from the catalog and continued from page 41 instead of page 1
(`extractor_crawls_resumed_total` = 1). The 67 checkpoint writes were coordinated by all five
catalog nodes (vector clock `{catalog1:8, catalog2:21, catalog3:13, catalog4:10, catalog5:15}`)
without producing a single sibling, because each write carried the context of the previous one.
