# Raft design

`core/src/main/java/io/quorumdb/raft/RaftNode.java` implements Raft as described in Ongaro & Ousterhout,
*In Search of an Understandable Consensus Algorithm* (extended version), plus three extensions from
Ongaro's thesis: PreVote, CheckQuorum (leader stickiness) and ReadIndex reads.

## State

| Persistent (before replying) | Volatile | Leader only |
|---|---|---|
| `currentTerm`, `votedFor` (one atomic `hardstate` file) | `commitIndex`, `lastApplied`, role, known leader | `nextIndex[]`, `matchIndex[]`, pending proposals, read rounds |
| log entries (WAL), snapshot + its last index/term | election timer | |

## Rules as implemented (with the paper's references)

**Elections (§5.2, §5.4.1).**

- A follower that hears nothing for a randomized timeout (default 150-300 ms) first runs a **pre-vote**: it
  asks whether peers *would* vote for it in term+1, without changing any state.
- Only with a majority of pre-votes does it increment its term, vote for itself (durably) and request votes.
- A voter grants at most one vote per term, persisted before the reply, and only to a candidate whose log is
  at least as up to date (last term, then last index).
- A node that heard from a live leader within the minimum election timeout ignores vote requests entirely
  (*leader stickiness*), so a partitioned node cannot depose a healthy leader when it returns.

**Leader start.** A new leader appends a **no-op** entry of its term. Committing it commits all earlier
entries (§5.4.2) and lets ReadIndex know the current commit index.

**Replication (§5.3).**

- AppendEntries carries `prevLogIndex`/`prevLogTerm`. A follower rejects on mismatch and returns a hint: the
  first index of its conflicting term, or its log length. The leader skips a whole term per round trip.
- On the first conflicting entry the follower truncates its suffix. It never truncates entries it already
  matches, so stale or duplicated messages cannot delete newer entries.
- A follower refuses (and fail-stops) rather than truncate a committed entry.
- At most one data-carrying message is in flight per follower. Heartbeats always go out and double as
  retransmission.
- Followers advance their commit index only up to the last entry the current message verified, never into
  an unverified suffix.

**Commitment (§5.4.2, Figure 8).** The leader advances `commitIndex` to the highest index stored on a
majority **only if that entry is from its current term**. `RaftReplicationTest.leaderDoesNotCommitEntriesFromEarlierTermsByCountingReplicas`
reproduces Figure 8.

**CheckQuorum.** A leader that has not heard from a majority within the maximum election timeout steps
down, so a leader isolated in a minority stops accepting work.

**Snapshots (§7).**

- After `snapshotThreshold` applied entries, the state machine is serialized at `lastApplied` and the log
  prefix is compacted.
- A follower whose required entries are gone gets `InstallSnapshot`, in chunks of at most `maxBytesPerAppend` with one chunk in flight. It keeps its log suffix
  only if it contains the snapshot's last entry with the same term.

**Linearizable reads (thesis §6.4, ReadIndex).**

- A read records `readIndex = max(commitIndex, noOpIndex)` and tags itself with the next heartbeat round.
- It is served after a majority acknowledges a round sent after the read arrived, and the state machine has
  applied `readIndex`.
- Concurrent reads share rounds.

## Message handling

Requests and replies are independent one-way messages. Replies echo the request's term (`requestTerm` for
votes, `readSeq` for appends), so late replies from old rounds are recognized and ignored. Every handler is
idempotent, so the transport may drop, delay, duplicate or reorder messages. FaultLab's simulated network
does all four.

## Failure handling

| Failure | Behaviour |
|---|---|
| Storage write throws | Node fail-stops; pending proposals complete as `UNKNOWN`; no further messages |
| Leader loses leadership with pending proposals | Callers get `UNKNOWN` (the entry may still commit later), never a false failure |
| Proposal queue full | `BUSY` (definitely not appended) |
| Torn final WAL record after crash | Discarded on recovery (it was never acknowledged) |
| Any other WAL corruption | Startup refuses with `StorageException.Corrupted` |

## Deliberately not implemented

- **Membership changes.** Joint consensus is required for correctness and is substantial work.
- **Leader leases** (clock-based reads). ReadIndex is used instead because it does not depend on bounded clock drift.
- **Pipelined AppendEntries, chunked snapshots, an asynchronous storage thread.** These are performance
  work; see the future work list in [docs/HANDOVER.md](docs/HANDOVER.md).
