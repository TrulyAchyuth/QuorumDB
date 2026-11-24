# Consistency model

This document states exactly what a client can rely on. It covers a cluster of `N` voting nodes with fixed
membership and a quorum of `⌊N/2⌋ + 1`.

## Assumptions

Guarantees hold only while these assumptions hold. FaultLab's `DISK_LOSS` scenario breaks the second one on
purpose and shows the consequence.

1. **Crash-stop with recovery.** Nodes may crash and restart, but never behave arbitrarily (no Byzantine faults).
2. **Stable storage.** Anything a node has fsynced survives its crashes. With `fsync=false` this assumption
   does not hold for power loss, and safety can be lost.
3. **Network.** The network may lose, delay, duplicate and reorder messages and may partition, but does not
   forge or corrupt them undetected. Without TLS, Raft messages are not authenticated, so run on a private
   network; with TLS on, the Raft port only accepts peers holding a certificate from the cluster CA.
4. **No clock assumptions for safety.** Clocks only drive timeouts. Liveness needs eventually timely
   communication within a majority.

## Writes (PUT, DELETE, batch)

| Response | Meaning |
|---|---|
| `2xx` | The command was **committed** (stored on a majority, in an entry of the leader's term or covered by one) and **applied** on the leader. It is durable under the assumptions above and will be visible to every later linearizable read. |
| `409 VERSION_MISMATCH` | Committed and applied, but its compare-and-set precondition failed, so it changed nothing. |
| `503 NO_LEADER`, `429 BUSY` | **Definitely not executed.** Safe to retry anywhere. |
| `503`/`504 OUTCOME_UNKNOWN` | **May or may not be executed.** Leadership changed or the deadline passed after the entry was appended. Retry with the same `X-Client-Id`/`X-Request-Seq` to get exactly-once execution. |
| `503 LEADER_UNREACHABLE` | A follower could not forward to the leader. Treat as unknown. |

**Batches are atomic:** all operations apply at one log index, or none do (any failed precondition aborts
the batch). Each key's `version` is the log index of the command that last wrote it.

**Exactly-once retries.** The state machine remembers the latest `(clientId, sequence)` and its result per
client, and the table is included in snapshots. A retry with the same pair returns the original result
(`"duplicate": true`) without re-applying. Limits:

- At most 10,000 client sessions are kept; the oldest are evicted first.
- A client must send increasing sequence numbers and retry only its latest request.

## Reads

| `consistency=` | Served by | Guarantee | Cost |
|---|---|---|---|
| `linearizable` (default) | leader (followers forward) | **Linearizable.** Reflects every write acknowledged before the read started. Implemented with ReadIndex: the leader confirms it is still leader via a heartbeat round acknowledged by a majority *after* the read arrived, then waits until it has applied the commit index observed at arrival. | one round trip to a majority (shared between concurrent reads) |
| `leader` | node that believes it is leader | **Not linearizable.** A deposed leader cut off by a partition can serve old data until CheckQuorum makes it step down (within about one election timeout). Monotonic only while leadership is stable. | local |
| `stale` | any node | **Not linearizable, arbitrarily stale.** Returns the node's applied state; `readIndex` in the response is that node's applied index. A follower never returns uncommitted data. | local |

QuorumDB never presents a `leader` or `stale` read as linearizable. FaultLab demonstrates the difference:
`ScenarioTest.unconfirmedLeaderReadsAreFlaggedExactlyWhenAStaleReadOccurred` shows leader-local reads
returning stale data during a partition (the checker flags exactly those runs), and
`readIndexReadsAreLinearizableUnderPartitionForEverySeed` shows ReadIndex reads staying linearizable across
25 seeds of the same scenario.

## Availability

- Writes and linearizable reads need a leader with a live majority. In a minority partition they fail
  (`NO_LEADER`, `OUTCOME_UNKNOWN`). This is the CP choice in CAP.
- `stale` reads keep working on any running node.
- After a leader failure, the cluster elects a new leader after an election timeout. In the benchmark, a
  SIGKILLed leader caused 300 ms without a successful write.

## What is verified, and how

| Claim | Evidence |
|---|---|
| Acknowledged writes survive leader crashes | `RealClusterIT.leaderSigkillFailsOver...` (real processes), `ScenarioTest`, soak |
| Minority cannot commit | `RealClusterIT.isolatedLeaderCannotCommit...`, `fiveNodeClusterSplitTwoThreeOnlyMajorityCommits`, `MinorityPartitionTest` |
| Linearizability of the client history | `LinearizabilityChecker` on every FaultLab run; 25,000 soak runs; live experiments |
| Exactly-once retries | `KvStateMachineTest.retriedCommandIsExecutedOnce`, manual curl check in `docs/PROGRESS.md` |

Testing finds violations; it does not prove their absence. See [TESTING.md](TESTING.md) for the scope of
what was exercised.
