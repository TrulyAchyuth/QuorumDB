# Benchmarks

Latest measured results: [docs/evidence/benchmarks.md](docs/evidence/benchmarks.md) (raw JSON in
`benchmarks/results/`). The dashboard's Benchmarks page shows the same file.

## Method

- `scripts/bench.py` builds the server and the load generator, then for each configuration starts a
  **fresh cluster of real processes** with `scripts/cluster.sh`.
- The load generator (`benchmarks/`, plain Java, public HTTP API only) runs closed-loop clients: each sends
  one request, waits for the response, and sends the next.
- There is a 3 s warm-up, then 15 s are measured. Only requests that start after warm-up count.
- Latency is the client-side round trip. Throughput counts successful requests.
- Writes go to the leader (discovered via `/api/v1/leader`), like a leader-aware client. `write-via-followers`
  sends them to all nodes round-robin, and followers forward them.
- Node CPU and heap are sampled every second from `/api/v1/metrics`.
- Fault runs inject a leader SIGKILL or a live leader isolation halfway through and report the longest window
  with zero successful requests, at 100 ms resolution.

Reproduce: `python3 scripts/bench.py` (about 6 minutes; `--quick` for a shorter run).

## Results summary (2026-11-13, Apple M5, 10 cores, 24 GB, JDK 21, fsync on)

| Configuration | Writes/s | p50 | p99 |
|---|---:|---:|---:|
| 1 node, 1 client | 242 | 4.0 ms | 5.9 ms |
| 1 node, 64 clients | 7,874 | 8.0 ms | 14.7 ms |
| 3 nodes, 1 client | 97 | 10.5 ms | 12.9 ms |
| 3 nodes, 64 clients | 2,870 | 21.9 ms | 36.7 ms |
| 5 nodes, 64 clients | 2,126 | 29.7 ms | 46.5 ms |
| 3 nodes, 64 clients, **no fsync** | 26,517 | 2.0 ms | 8.1 ms |

| 3 nodes, 32 clients, reads | Reads/s | p50 | p99 |
|---|---:|---:|---:|
| linearizable (ReadIndex) | 31,175 | 0.96 ms | 2.3 ms |
| leader-local | 59,057 | 0.50 ms | 1.4 ms |
| stale (any node) | 54,846 | 0.53 ms | 1.6 ms |

| Fault under load (3 nodes, 32 clients) | Longest window with zero successful writes |
|---|---:|
| Leader SIGKILL | 300 ms |
| Leader isolated (live fault, 3 s) | 300 ms |

## Interpretation

- **fsync dominates.** A single `FileChannel.force` measured 3.8 ms on this machine. A replicated write needs
  the leader's and a follower's fsync in sequence plus two network hops, about 10 ms at one client. The
  same cluster without fsync is about 9× faster at 64 clients, but that mode is not crash-safe.
- **Group commit is what scales throughput.** Concurrent writes share one WAL append and one fsync, so a
  3-node cluster goes from 97 writes/s (1 client) to 2,870 (64 clients) at roughly double the latency.
- **Replication cost.** At 64 clients one node does 7,874 writes/s and three do 2,870. The difference is the
  follower fsync and round trip: the price of tolerating a failure. Five nodes are slower again, since the
  leader replicates to four followers and needs three acknowledgements.
- **Reads.** ReadIndex costs a heartbeat round to a majority, about half the throughput of unconfirmed
  leader reads. Concurrent reads share rounds, which keeps it fast.
- **Mixed load is the weak spot.** Under 50/50 load, throughput and read latency are much worse than either
  alone: 450 writes/s plus 444 reads/s, read p50 39 ms. Storage runs on the Raft thread, so reads queue
  behind fsyncs, and a read round must wait for followers that are themselves fsyncing. Moving storage to its
  own thread is the most valuable performance improvement (see [docs/HANDOVER.md](docs/HANDOVER.md)).

## Snapshot storm investigation

*2026-11-14, real processes on localhost (not simulation). Raw JSON: `benchmarks/results/snapshot-storm-20261114/`.*

**Trigger.** A 15-minute dashboard session on 2026-11-13 (8 clients, 50/50 load, linearizable reads,
faults injected for screenshots) averaged about 213 writes/s. One window dropped far below that with no
elections. Faults were being injected during that session, so a controlled run was needed.

**Controlled run.** `scripts/snapshot_stall.py`: fresh 3-node cluster, the same workload, no faults, 120 s,
100 ms timeline. In the first such run, four 100 ms buckets had almost no completed requests (longest gap
200 ms, slowest request 317 ms). That window began 0.2 s after the snapshot at index 10002 and was followed
by a follower going to PreVote (no term change). At the next snapshot the leader logged about 200
`SNAPSHOT_SENT` events for one follower within 30 ms, each carrying the whole 300 KB snapshot.

**Cause.** `RaftNode.onAppendEntriesReply` cleared the follower's in-flight flag on every reply, including
replies to heartbeats and ReadIndex read rounds. The next `sendAppend` saw that the follower needed
compacted entries and shipped the snapshot again. Linearizable reads trigger read rounds constantly, so
each snapshot became hundreds of 300 KB messages, queuing heartbeats behind them. Fix: the snapshot in
flight is tracked separately, cleared only by a reply that covers it, and resent only after an election
timeout. `RaftReplicationTest.snapshotIsNotResentWhileInFlightButIsRetransmittedAfterTimeout` failed on
the old code (101 sends instead of 1).

**Before/after measurements.** 120 s each, 2 snapshots per run, 18 runs:

- A: pre-fix first in each pair.
- B: fix first in each pair.
- C: both builds made in the same worktree, differing only in the RaftNode change, ABBA order.
- D: the shipped build (HEAD) against pre-fix, ABBA order, both started from fixed jar files.

| Exp. | Build | ops/s | write p99 | max write | snapshot sends |
|---|---|---:|---:|---:|---:|
| A | pre-fix | 420 | 32.0 ms | 75 ms | 511 |
| A | fix (main tree) | 407 | 33.2 ms | 55 ms | 2 |
| A | pre-fix | 442 | 30.9 ms | 104 ms | 962 |
| A | fix (main tree) | 398 | 34.4 ms | 66 ms | 2 |
| A | pre-fix | 431 | 31.7 ms | 80 ms | 762 |
| A | fix (main tree) | 386 | 34.5 ms | 64 ms | 2 |
| B | pre-fix | 430 | 31.5 ms | 110 ms | 786 |
| B | fix (main tree) | 415 | 33.1 ms | 56 ms | 2 |
| B | pre-fix | 436 | 31.1 ms | 73 ms | 546 |
| B | fix (main tree) | 396 | 34.4 ms | 49 ms | 2 |
| C | pre-fix | 423 | 32.0 ms | 772 ms | 1120 |
| C | pre-fix + RaftNode fix | 458 | 29.8 ms | 50 ms | 2 |
| C | pre-fix + RaftNode fix | 425 | 31.8 ms | 46 ms | 2 |
| C | pre-fix | 436 | 31.5 ms | 102 ms | 1079 |
| D | HEAD | 416 | 32.3 ms | 46 ms | 2 |
| D | pre-fix | 437 | 31.2 ms | 83 ms | 832 |
| D | pre-fix | 422 | 33.0 ms | 47 ms | 38 |
| D | HEAD | 422 | 32.0 ms | 43 ms | 2 |

**What the fix does and does not do.**

- **Removes the storm:** 2 snapshot sends per run (one per snapshot) instead of 38-1,120.
- **Removes latency outliers:** worst write 43-66 ms with the fix, against 47-772 ms without. The 772 ms
  outlier (C) is the storm itself.
- **Does not measurably change throughput or p99.** In A and B the fixed build was 3-10% slower and its p99
  1-3 ms higher. In C, where both builds come from the same tree, it was 3% faster on average. In D the
  difference was 2.5%. The same build differed by 3.6% between two runs (D, pre-fix). In A and B the
  fixed build was started without a pinned jar, so `scripts/cluster.sh` checked (and at times rebuilt) it
  with a Gradle daemon while code was being edited; C and D avoid this. The A/B gap is therefore attributed
  to that difference in procedure, not to the fix, but that attribution is not proven.
- **Stalls:** one pre-fix run (C, first row) had 600 ms with zero completed requests (at 116.5 s, worst write
  772 ms, 1,120 snapshot sends). Its snapshot timestamps were not recorded, so the overlap with a snapshot is
  likely but not shown. No run with the fix had a single 100 ms bucket without completions; the other 8
  pre-fix runs had none either.
- **Not reproduced:** the original 2026-11-13 drop, which lasted far longer than any stall measured here. The
  snapshot storm is a plausible contributor, not a demonstrated cause (see [FAILURE_MODES.md](FAILURE_MODES.md)).

Reproduce: `python3 scripts/snapshot_stall.py --label after --runs 3`, and with
`--jar <pre-fix quorumdb-server.jar> --label before` for a comparison build.

## Caveats

- The load generator and all nodes share one machine and talk over localhost: no real network latency,
  and clients compete with servers for CPU.
- One run per configuration in the matrix; no confidence intervals. In two separate sessions the same configurations differed by 8-11% (3 nodes / 16 clients: 706 vs 788 writes/s; 1 node / 16 clients: 2,189 vs 2,022).
- Disk usage after 66,860 entries: 14.6 MB per node. That is the latest snapshot plus WAL segments (16 MB
  each) not yet fully covered by a snapshot.
