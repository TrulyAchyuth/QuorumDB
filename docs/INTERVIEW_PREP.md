# Interview preparation: the hardest decisions in QuorumDB

Each section: the question an interviewer is likely to ask, the answer, and where in the code or evidence
to point.

## 1. "Why is your Raft node single-threaded, and what did that cost you?"

**Decision.** `RaftNode` is a deterministic state machine: every input (message, timer, proposal) runs on one
thread, and it has no clock, threads or I/O of its own. Everything external goes through `Scheduler`,
`Transport`, `RaftStorage` and `StateMachine`.

**Why.**

- Raft's rules are much easier to get right without locks.
- It makes the deterministic simulator possible: the same class runs on virtual time with a seeded network.
  Without it there would be two Raft implementations, and the tested one would not be the shipped one.

**Cost.**

- Storage runs on that thread, so an fsync blocks everything else on the node.
- The benchmark shows it: under 50/50 mixed load, read p50 rises to 39 ms because reads queue behind fsyncs
  and read rounds wait for followers that are fsyncing.
- The fix is etcd's approach: Raft produces "entries to persist" and "messages to send", a separate storage
  thread persists them, and messages that depend on persistence are released after the fsync.

## 2. "When exactly is a write acknowledged?"

Only after it is **committed** and **applied on the leader** (`RaftNode.applyCommitted` completes the
pending proposal).

- Committed means stored on a majority *and* the entry (or a later one) is from the leader's current term.
  This is Figure 8: counting replicas of an old-term entry is not enough
  (`RaftReplicationTest.leaderDoesNotCommitEntriesFromEarlierTermsByCountingReplicas`).
- If the leader loses leadership before it knows the outcome, the client gets `OUTCOME_UNKNOWN`, not a
  failure, because a later leader may still commit the entry.
- Clients make retries safe with `X-Client-Id` + `X-Request-Seq`; the state machine de-duplicates them,
  and that table is part of snapshots.

## 3. "How do you serve linearizable reads without writing to the log?"

**ReadIndex.**

- The read records `readIndex = max(commitIndex, leader's no-op index)`.
- It waits for a heartbeat round sent *after* the read arrived to be acknowledged by a majority. Replies
  echo a `readSeq`, so an acknowledgement of an earlier heartbeat does not count
  (`readIsNotServedOnAcknowledgementsThatPredateIt`).
- It then serves once the state machine has applied `readIndex`.
- Concurrent reads share rounds.

**Not leases.** Leases depend on bounded clock drift, and QuorumDB makes no clock assumptions for safety.

**Evidence.** FaultLab shows the difference across 25 seeds: during a partition, leader-local reads return
stale data (the checker flags exactly the runs where that happened, compared against ground truth from the
trace), while ReadIndex reads stay linearizable in every seed.

## 4. "What happens when a node is partitioned away and comes back?"

- **Without PreVote,** it keeps timing out and incrementing its term. On return its higher term forces the
  healthy leader to step down. The built-in `rejoin-no-prevote` scenario shows the term going to 25 and a
  leader change.
- **With PreVote,** it first asks whether it *could* win, which changes no state. Peers that heard from a
  live leader recently refuse (leader stickiness), so the term stays at 1 and nothing is disrupted.
- **CheckQuorum** handles the other side: an isolated leader steps down on its own, so it stops accepting
  writes it can never commit. The real-process test shows its commit index frozen and its role changing to
  follower.

## 5. "How do you know your safety checker is not just saying PASS?"

Three layers:

1. **Hand-written histories.** Every property has a test where a violation must be caught and a legal
   history must pass, including subtle legal cases: divergent *uncommitted* suffixes, compacted entries,
   restarts resetting the apply position.
2. **Seeded bugs in real Raft.** `SeededBugTest` runs the real implementation on storage that forgets votes
   (caught as two leaders in one term) or ignores truncation (caught as log and commit violations).
3. **Non-vacuity.** The same harness asserts that unmodified Raft actually committed thousands of entries
   and truncated logs, and checks with nothing to examine report `NOT_EXERCISED`, not `PASS`.

**War story.** The first version of the seeded-bug harness passed trivially. It sent bytes that were not
valid commands, which (before a fix) made every replica halt after the first commit. Both the harness and
that poison-pill bug were fixed.

## 6. "What would break your durability guarantees?"

- **Losing an fsynced write.** Running with `fsync=false`, a lying disk cache, or macOS's `fsync`, which
  doesn't flush the drive cache without `F_FULLFSYNC`.
- **Losing a disk.** The `disk-loss` scenario deliberately wipes a node's disk while it is in a majority
  with a stale node. The checker reports two leaders in term 2, different entries committed at the same
  index, and non-linearizable reads, all labelled as consequences of a broken assumption.
- **Torn writes** are handled: an invalid final WAL record is discarded because it was never acknowledged.
  An invalid record *followed by valid ones* means corruption, and the node refuses to start rather than
  silently dropping acknowledged entries.

## 7. "Why is your write latency 20 ms when Raft is supposed to be fast?"

Because it is fsync-bound, and I measured that rather than guessing:

- A raw `FileChannel.force` costs **3.8 ms** on the test machine.
- A replicated write needs the leader's and a follower's fsync in sequence.
- At 64 clients, 3 nodes do 2,870 writes/s at p50 22 ms with fsync, and 26,517/s at p50 2 ms without.

Group commit is what makes throughput scale with concurrency. Next steps would be asynchronous storage,
pipelined AppendEntries, and sending to followers in parallel with the leader's own fsync (thesis §10.2.1).

## 8. "How deterministic is the simulator really?"

- Single thread; tasks ordered by `(virtual time, insertion order)`; one seeded `Random`; `RaftNode` takes
  time and randomness only through injected interfaces.
- Every run produces a SHA-256 over its full event trace. Tests and a property (40 random scenarios, each
  run twice) assert identical hashes; the dashboard's *Verify replay* re-runs and compares.
- One subtle bug to avoid: anything iterating a `HashMap` in a way that affects behaviour. The simulator
  uses `LinkedHashMap` where order matters.

## 9. "What is not done?"

- Membership changes.
- TLS and authentication between nodes.
- An asynchronous storage thread.
- Streaming or chunked snapshots.
- A Jepsen run against the real cluster.
- One unexplained 15-second slowdown observed once during manual dashboard testing (no errors, no
  elections) that did not recur; documented in FAILURE_MODES.md.

Being clear about these is part of the answer.
