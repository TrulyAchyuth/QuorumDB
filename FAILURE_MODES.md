# Failure modes

How QuorumDB behaves under each failure, what clients see, and where that behaviour is tested. "Sim"
means a FaultLab simulation test, "Real" a test against real processes (`RealClusterIT`).

| Failure | Expected behaviour | Client impact | Tested |
|---|---|---|---|
| Leader process crash (SIGKILL) | Followers time out (150-300 ms), PreVote, then election; new leader commits a no-op; acknowledged writes all survive | Writes fail or are unknown for about one election timeout | Real (measured 272-317 ms to a new leader across runs), Sim `leader-crash` |
| Follower crash | Majority keeps committing; on restart the follower recovers from its WAL and catches up from the leader (or via InstallSnapshot if compacted) | None while a quorum remains | Real, Sim `follower-crash`, `snapshot-catch-up` |
| Leader isolated (minority) | Old leader cannot commit; CheckQuorum makes it step down; majority elects a new leader; on heal the old leader truncates its uncommitted suffix | Requests to the old leader get `OUTCOME_UNKNOWN` / `NO_LEADER` | Real, Sim `leader-isolated` |
| 2/3 split of 5 nodes | Only the 3-node side commits | Minority side unavailable | Real, Sim `split-5` |
| Majority lost | No leader can be elected; nothing commits | Writes and linearizable reads unavailable; stale reads still work | Sim `no-quorum`, `ScenarioTest.losingTheMajority...` |
| Message loss, delay, duplication, reordering | Safety unaffected; heartbeats retransmit | Higher latency | Sim `flaky-network`, property tests |
| Isolated follower rejoins | With PreVote: no term inflation, leader keeps leading. Without PreVote: terms inflate and the leader is deposed | Brief disruption only without PreVote | Sim `rejoin-prevote` vs `rejoin-no-prevote` |
| Disk writes fail | Node fail-stops instead of acknowledging what it cannot persist; restarts when the disk works | If it was the leader: one election | Sim `storage-failure`, `RaftReplicationTest.storageFailureMakesTheNodeFailStop` |
| Crash mid-WAL-append (torn write) | Partial last record discarded on recovery (never acknowledged) | None | `FileStorageRecoveryTest` |
| WAL corruption elsewhere, corrupt hard state, missing log after a lost snapshot | Node refuses to start (`StorageException.Corrupted`) | That node stays down; the operator must restore it | `FileStorageRecoveryTest` |
| Crash during snapshot or hard-state write | Atomic write-temp-fsync-rename-fsync-dir: the old or new file is seen, never a mix | None | `FileStorageRecoveryTest.interruptedAtomicWrites...` |
| Full cluster restart | All acknowledged data recovered from disk; terms keep increasing | Unavailable while restarting | Real `fullClusterSigkillAndRestart...` (100/100 keys) |
| Malformed committed command | Rejected identically on every replica (no state change) instead of crashing all nodes repeatedly | That command fails | `KvStateMachineTest.malformedCommittedEntry...` |
| Disk loss (data directory erased) | **Outside Raft's assumptions.** Committed entries can be overwritten | Lost writes are possible | Sim `disk-loss`: FaultLab detects and reports the violations, labelled as a broken assumption |
| Client retries after an unknown outcome | With `X-Client-Id` + `X-Request-Seq`: applied at most once | None | `KvStateMachineTest.retriedCommandIsExecutedOnce` |
| Overload | Proposal queue (50,000) full: `429 BUSY`; inbound Raft messages shed past 100,000 queued tasks (treated as loss) | Retry later | `RaftReplicationTest.backPressure...` |
| Oversized request | `413` before the body is read; bodies must declare `Content-Length` | Request rejected | manual verification (see `docs/PROGRESS.md`) |
| Oversized FaultLab simulation | Run abandoned at its event budget (derived from the heap): `422 RUN_TOO_LARGE`; at most N simulations at once, then `429 BUSY`; stored runs evicted by total events. Before the fix, one request crashed a node with `OutOfMemoryError` | That request rejected; node unaffected | Real `oversizedSimulationIsRefused...`, `EventBudgetTest`, `SimulationLimitsTest`, [evidence](docs/evidence/simulation-limits.md) |
| Follower needs a snapshot while reads/heartbeats flow | One InstallSnapshot in flight; resent only after an election timeout | None | `RaftReplicationTest.snapshotIsNotResentWhileInFlight...` |
| Node restarted with a long log during a live FaultLab run | Audit pages through the whole durable log (earlier versions reported a false Commit Durability violation) | None | Real `liveAuditOfALongLogAcrossACrash...` |

## Operational notes

- **fsync.** `quorumdb.fsync=false` (or `QUORUMDB_FSYNC=false`) leaves flushing to the OS. A node can then
  lose acknowledged entries on power loss, which breaks Raft's assumptions. It exists for benchmarks only.
- **macOS.** `FileChannel.force` maps to `fsync(2)`, which on macOS does not force the drive's cache (that
  needs `F_FULLFSYNC`). Power-loss durability on macOS is therefore weaker than on Linux.
- **Clocks.** Not used for safety. The live FaultLab orders events from different hosts by wall-clock time,
  which is approximate across machines.
- **Throughput dip under steady load (investigated 2026-11-14).** During dashboard testing on 2026-11-13,
  one 15-second window showed write throughput on a healthy 3-node cluster dropping from about 210/s to
  about 10/s, with no errors, elections or term changes, and every node slowing down at once.
  - A controlled 120 s steady-state run then showed four 100 ms buckets with almost no completed requests
    (longest gap 200 ms, slowest request 317 ms) starting at a snapshot, followed by a follower losing
    contact with the leader for a moment (PreVote, no term change).
  - Cause found: while a snapshot was in flight, every reply to a heartbeat or ReadIndex read round made
    the leader send the whole snapshot again (511-962 copies of a 300 KB snapshot per 120 s run). Fixed in
    `RaftNode` (one snapshot in flight per follower, retransmitted only after a timeout);
    `RaftReplicationTest.snapshotIsNotResentWhileInFlight...` reproduces it.
  - The original 15-second window was not reproduced. The snapshot storm is a plausible contributor (it
    hit every node at once and caused no elections), but a 15-second drop is much longer than anything
    measured here, so its cause is not proven. Details: [BENCHMARKS.md](BENCHMARKS.md#snapshot-storm-investigation).
