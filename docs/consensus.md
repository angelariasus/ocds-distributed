# Consensus: a replicated coordinator on our own Raft

Until phase 9 the coordinator was the last single point of failure that mattered: one process
deciding when to plan extraction rounds. Running three copies naively would plan every round three
times; running one means no planning while it is down. The coordinator now runs as three replicas
that agree, through Raft, on one shared schedule.

Raft is implemented from scratch in `services/coordinator/internal/raft` (about 500 lines), the
same way the catalog implements Dynamo, so every rule of the algorithm is visible and tested.

## What is implemented

| Mechanism | Rule |
|---|---|
| Terms and roles | follower → candidate → leader; any message with a higher term turns the receiver into a follower of that term |
| Leader election | randomized election timeout (1.5–3 s); a candidate needs votes from a majority; a node votes at most once per term, and only for a candidate whose log is at least as up to date as its own |
| Log replication | `AppendEntries` carries the previous entry's index and term; a follower whose log does not match rejects it, and the leader backs up (a whole conflicting term per round trip) until they agree, then overwrites the follower's divergent suffix |
| Commit | an entry is committed once stored on a majority, but a leader only counts replicas for entries of *its own* term (§5.4.2); a no-op appended on election lets it commit everything before it |
| Persistence | term, vote and log are written (temp file, fsync, rename) before answering any RPC |
| CheckQuorum | a leader that stops hearing from a majority for an election timeout steps down instead of believing it still leads |
| PreVote | before incrementing its term, a would-be candidate asks whether it *could* win; peers that still hear from a live leader refuse (leader stickiness) |

Snapshots and membership changes are out of scope: the log grows by two entries per planning
round.

## The replicated state machine

Every replica applies the same committed commands in the same order to a small schedule:

```
round_planned   {round, at, leader}   → last_round = round, pending = true
round_published {round, at, tasks}    → published_at = at, pending = false
```

Only the leader acts, and it always acts through the log: it commits `round_planned` *before*
listing the portal and publishing tasks, and `round_published` after. Whoever becomes leader next
reads the schedule and does exactly one of three things: wait (the last round is recent), plan a
new round, or **republish** a round whose `round_planned` was committed but whose
`round_published` never was. Publishing twice is harmless (extractors are idempotent); skipping a
round, or two leaders planning concurrently, is what this prevents. Every task published carries
the round and the Raft term in its Kafka headers.

## Live experiments

Measured on 2026-10-19 with `scripts/raft_experiments.sh` on the three compose replicas
(election timeout 1.5–3 s, heartbeat 300 ms).

| Scenario | Result |
|---|---|
| Leader killed with `SIGKILL` | new leader in **2.0 s, 3.7 s, 2.0 s** (three trials); schedule intact (last round unchanged, no duplicate planning); a forced round then committed in the new term; the killed replica restarted as a follower and caught up to the same commit index |
| Leader dies after committing `round_planned` and before publishing (`POST /plan?crash=1`) | new leader in 1.1 s logged *"republishing a round the previous leader did not finish" round 5*, published it and committed `round_published` in term 408; all three replicas at commit index 42, round 5, nothing pending |
| Leader cut off from the network | it logged *"lost contact with the majority, stepping down" term 408* (CheckQuorum); the majority elected a leader in 3.8 s and committed a round while it was away; after healing it rejoined as a follower in term 409 |
| Follower cut off for 40 s, then reconnected (PreVote) | **0 elections attempted** while isolated; leader and term unchanged on rejoin (`coordinator2`, term 409 before and after) |

One run of the leader-crash case took 37.6 s instead of 2–4 s. It happened right after the
containers had been recreated, while the probe could not reach one of the surviving replicas
either; three repetitions afterwards were back to 2.0–3.7 s. It is recorded as observed and not
explained.

The PreVote row is the second attempt at that experiment; the first, before PreVote existed, is
the disruptive-server lesson in [lessons.md](lessons.md): an isolated follower ran 348 elections,
reached term 348 and on rejoining deposed the healthy leader five times in ten seconds.

## Tests

`raft_test.go` runs clusters of 3 and 5 nodes over an in-memory network that can cut any node off,
each test five times under the race detector:

- exactly one leader per term;
- commands applied in the same order on every node;
- followers refuse proposals;
- an isolated leader cannot commit, steps down, is replaced by a leader with a higher term, and on
  reconnecting discards its uncommitted entry and converges;
- a minority partition cannot elect, the majority keeps committing;
- a node restarted from storage keeps its term and log and catches up;
- a follower isolated for many election timeouts rejoins without changing the leader or the term.
