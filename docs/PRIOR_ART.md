# Prior art and what is (and is not) distinctive about QuorumDB

QuorumDB does not claim to be the first or the best at anything. Raft, deterministic simulation,
linearizability checking and fault injection are all established. This document records what already
exists, how QuorumDB relates to it, and the narrow claim that remains defensible.

## Consensus implementations

| Project | What it is | Relation to QuorumDB |
|---|---|---|
| Raft paper (Ongaro & Ousterhout, 2014) and Ongaro's thesis | The algorithm, plus PreVote, CheckQuorum, ReadIndex, membership changes | QuorumDB implements the paper's core and the thesis' PreVote, CheckQuorum and ReadIndex; not membership changes. |
| etcd/raft (Go) | Production Raft library designed as a deterministic state machine: the application drives ticks and I/O | Same structural idea: `RaftNode` is single-threaded with I/O behind interfaces. QuorumDB's code is independent and far less complete. |
| HashiCorp Raft (Go) | Production Raft library with pluggable transport and storage | Same separation of transport and storage; no shared code. |

## Testing distributed systems

| Project | What it does | Overlap | Difference |
|---|---|---|---|
| **Jepsen** (Clojure) | Black-box testing of real databases: clients, nemesis faults (partitions, kills, clock skew), history checkers (Knossos for linearizability, Elle for transactional isolation) | QuorumDB's live experiments follow the same shape: clients, injected faults, history check. The linearizability checker uses the same algorithm family as Knossos. | Jepsen is far more powerful and general. QuorumDB's version is built into one system, adds white-box checks of Raft internals from emitted events, and runs from a web UI. |
| **Maelstrom** (jepsen-io) | Workbench for learning distributed systems: you write nodes speaking JSON on stdin/stdout, and Maelstrom simulates the network, injects faults and checks workloads such as `lin-kv`; its course includes a Raft chapter | Closest in spirit: learning by injecting faults into a Raft implementation and checking linearizability. | Maelstrom tests *your* implementation against its workloads. QuorumDB ships one implementation with deeper Raft-specific checks (log matching, leader completeness, commit durability) and an integrated timeline. Maelstrom is language-agnostic; QuorumDB is not. |
| **FoundationDB simulation** | The canonical deterministic simulation testing: the whole cluster runs single-threaded on simulated network, disk and time, with "buggification" | QuorumDB's simulator uses the same principle: no real clock or threads, one seeded PRNG, the production code paths. | FoundationDB simulates a far larger system and runs at enormous scale. QuorumDB's simulator is small and covers only Raft + KV. |
| **TigerBeetle VOPR** | Deterministic simulator for TigerBeetle's Viewstamped Replication with fault injection, invariant checks, protocol-aware liveness checks and a browser visualisation | Very similar: seeded simulation, faults, invariant checking, visual replay. | VOPR is more rigorous (protocol-aware liveness checking, storage-fault models) and production-grade. QuorumDB is a Raft teaching and experimentation tool. |
| **Porcupine** (Go) | Fast linearizability checker (P-compositionality) with a history visualizer; used by etcd's robustness tests | Same problem as `LinearizabilityChecker`. | Porcupine is faster and more general; QuorumDB's checker is a straightforward Wing-Gong/Lowe search, per key. |
| **etcd robustness tests** | Fault injection into real etcd clusters with Porcupine-based linearizability checking and visualisation | Real-cluster fault injection plus a history check plus visualisation. | Production project testing etcd; not an interactive environment. |

Sources consulted: the Raft paper; github.com/jepsen-io/maelstrom; tigerbeetle.com (VOPR and protocol-aware
deterministic simulation testing); github.com/anishathalye/porcupine, which lists etcd's robustness tests
among its users; FoundationDB's published simulation-testing material.

## What remains defensible

None of FaultLab's techniques is new. Its distinguishing feature is **integration and developer experience
for learning how Raft fails**:

1. **One consensus implementation, two engines, one checker.** The exact `RaftNode` class runs in the
   deterministic simulator and in the real gRPC cluster. Both feed the same independent safety checker,
   linearizability checker and analysis, so a simulated and a live run of the same scenario can be compared
   in the same report format.
2. **White-box and black-box together.** Each run is judged both on Raft's internal invariants (from emitted
   events and sampled logs) and on what clients observed (linearizability).
3. **Safety, availability and observation gaps kept apart.** Losing a quorum is reported as expected
   unavailability, not failure. Incomplete live observations make a verdict INCONCLUSIVE instead of passing.
4. **Explanations grounded in events.** A deterministic summariser turns the event trace into an
   episode-by-episode account, every claim linked to the events it rests on.
5. **Checker strength is itself tested.** Seeded storage bugs must be caught, and passes must be shown not
   to be vacuous.

These are engineering and presentation choices, not research contributions. In particular, FaultLab is not
a model checker, explores only the interleavings its seeded simulator generates, and is not a replacement for
Jepsen, Maelstrom or a production simulator.

## Open challenges

- Membership changes, the part of Raft where implementation bugs are most common, are not implemented, so
  FaultLab cannot test them.
- No clock-skew faults (QuorumDB's safety does not depend on clocks, but leases would).
- Simulated disk faults are coarse: write failure and total loss, but no torn or misdirected writes or
  latent sector errors (the recovery code handles torn writes and is unit-tested separately).
- Liveness checking is end-state based ("did it recover after faults healed?"), not protocol-aware.
