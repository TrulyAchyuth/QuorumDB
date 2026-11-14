# Development progress log

Newest entries last. "Verified" means executed in this repository with the evidence linked.

## Phase 0: environment and architecture (done)

- Repository was empty (no commits). Toolchain: Temurin JDK 21, Gradle 9.9.1 (wrapper, SHA-256 pinned),
  Node 24. Docker was not installed on the development machine at this point.
- Decision: one Raft implementation behind four small interfaces (`Scheduler`, `Transport`,
  `RaftStorage`, `StateMachine`), so the identical `RaftNode` class runs on real threads/gRPC/disk
  and inside the deterministic simulator. No second "simulation Raft".
- Decision: the core is single-threaded and message-driven (requests and replies are independent
  one-way messages), which makes drop/delay/duplicate/reorder tolerance a property of the design
  rather than of the transport.

## Phase 1: storage foundation (done, verified)

- `FileStorage`: segmented CRC32C write-ahead log, atomic hard-state and snapshot files, process lock.
  Recovery truncates a torn final record and refuses to start on any other corruption.
- `KvStateMachine`: put / delete / atomic batch, versioned compare-and-set, per-client request
  de-duplication, snapshot/restore, content digest.
- Fix found by testing: a committed entry that was not a decodable command made every replica
  fail-stop (a "poison pill" replayed on each restart). It is now rejected deterministically on all
  replicas instead. Regression test: `malformedCommittedEntryIsRejectedDeterministicallyWithoutChangingState`.

## Phases 2, 3, 5: Raft consensus, replication, durability (core done, verified)

- Election, PreVote, CheckQuorum, leader stickiness, log replication with fast backtracking,
  current-term commit rule (Figure 8), snapshots + InstallSnapshot, ReadIndex reads, fail-stop on
  storage errors, back-pressure.
- 76 unit tests in `core` (see `docs/evidence/test-summary.md`).

## Phase 6: FaultLab (simulation side done, verified)

- Deterministic simulator (virtual clock, seeded network with loss/delay/duplication/partitions,
  crash/restart/disk faults), scenario model with time- and condition-based triggers, 13 built-in
  scenarios, independent safety checker (8 properties), black-box linearizability checker,
  liveness assessment kept separate from safety, metrics, evidence-linked explanations, run comparison.
- Test corrections made during verification (assertions were strengthened, none removed):
  - *Stale reads.* The first version asserted "leader-local reads are flagged as non-linearizable"
    for one seed and failed. Diagnosis from the trace: in some seeds no stale read actually happens
    (every client has left the deposed leader before the first write is acknowledged by the new
    one), so "LINEARIZABLE" was the correct verdict there. The test now derives ground truth from
    the trace per seed and requires the checker's verdict to match it exactly for 25 seeds, and
    separately requires ReadIndex reads to be linearizable for all 25 seeds.
  - *Seeded bugs.* The harness proposed bytes that were not valid KV commands, which (before the
    poison-pill fix) halted every node after the first commit, so "unmodified Raft passes" was
    vacuous. The harness now sends real commands and asserts minimum commits, elections and
    truncations so it cannot pass without exercising the system.
- Evidence: 68 tests in `simulation`; property tests ran 300 + 150 + 40 generated fault schedules;
  soak of 25,000 generated scenarios with 0 failures (`docs/evidence/soak.md`).

## Verification snapshot (clean build)

`./gradlew clean test`: **144 tests, 144 passed, 0 failed, 0 skipped** (`docs/evidence/test-summary.md`).

| Requirement | Where it is verified |
|---|---|
| Leader election | `RaftElectionTest` (18), `MinorityPartitionTest.exactlyOneLeaderIsElectedAndAllNodesAgreeOnIt` (20 seeds) |
| Leader failure | `ScenarioTest.leaderCrashElectsNewLeaderAndOldLeaderRejoins`, scenarios `leader-crash`, `storage-failure`, `rolling-restart` |
| Minority partition cannot commit | `MinorityPartitionTest.isolatedLeaderCannotCommit...`, `twoOfFiveCannotCommitThreeOfFiveCan`, `ScenarioTest.isolatedLeaderCommitsNothing...` |
| Persistent recovery | `FileStorageRecoveryTest` (12), `RaftElectionTest.voteSurvivesRestart`, `MinorityPartitionTest.committedDataSurvivesFullClusterRestart` |
| Raft safety invariants | `SafetyCheckerTest` (19), every scenario test, `RandomFaultProperties`, soak |
| Checker detects real bugs | `SeededBugTest` (forgotten votes, broken truncation), `disk-loss` scenario |
| Deterministic replay | `ScenarioTest.sameScenarioAndSeedReproduceTheExactEventTrace`, `RandomFaultProperties.everyRunIsReproducible...`; two soak invocations with the same master seed printed identical counts |

## Phase 4: real cluster (done, verified on real processes)

- `backend` module: one node per process. Raft over gRPC (`proto/raft.proto`, one-way envelopes, per-peer
  in-flight limits, inbound validation of cluster id, recipient, sender and message shape), REST API with
  leader forwarding, group commit, idempotent writes, three read consistency levels, Prometheus metrics,
  event and operation logs, cluster view.
- Live fault injection (`/api/v1/faults`, admin token required, disabled when no token is configured):
  crash/restart, isolate, partition, delay, drop, disk-write failure. Faults are applied on both ends of a
  link and each node expires its own faults, so they heal even if the coordinating node dies.
- Live FaultLab experiments: real HTTP clients + scheduled real faults + event/log collection from every
  node, audited with the same independent checkers as the simulator. Missed events become observation
  gaps (INCONCLUSIVE), never a silent pass. All faults are healed at the end of a run, also on error.
- `scripts/cluster.sh`: local multi-process clusters (start, stop, SIGKILL a node, revive, status).
- Test fix during verification: the 5-node partition test read the leader from the cluster view right
  after the split. The management plane (HTTP) is not partitioned, so the view correctly still showed the
  old leader until check-quorum deposed it. The test now waits for the majority side's own leader.
- Evidence (`backend (integrationTest)` in `docs/evidence/test-summary.md`), real processes on localhost:
  leader SIGKILL to new leader observed in 272-293 ms across runs; writes kept succeeding on the surviving
  majority during failover; every acknowledged write readable afterwards; killed node recovered from its
  WAL and caught up; isolated leader's commit index did not move and its write was never acknowledged;
  5-node 2|3 split: only the majority committed; full-cluster SIGKILL + restart recovered 100/100 keys.

`./gradlew clean test :backend:integrationTest`: **157 tests, 157 passed, 0 failed, 0 skipped.**

## Not yet implemented at this point (end of phase 4)

- Docker deployment.
- Dashboard, benchmarks, remaining documentation.
- Cluster membership changes: deliberately out of scope so far (fixed membership).

## Phase 6: verification and hardening (2026-11-14)

Resumed after an interrupted session; every earlier claim was re-checked against evidence first.

- Found and fixed with regression tests: false Commit Durability violations in live audits of long logs; the
  snapshot storm (snapshot re-sent on every heartbeat/read-round reply); an OutOfMemoryError from one
  unauthenticated simulation request; false NOT_LINEARIZABLE verdicts for back-to-back live runs.
- Found on Docker and fixed: wall-clock steps in live-run timing, event timestamps drifting behind wall
  time, dashboard proxy 502s and node-hopping, load generator unable to reach a Docker leader.
- Docker Compose deployment verified end to end three times in a row; nine consecutive live FaultLab runs on
  Docker all SAFE and LINEARIZABLE. Frontend unit tests added. Screenshots replaced with captures of the
  final build (the previous live report showed the false durability violation).
- `./gradlew test :backend:integrationTest`: **171 tests, 171 passed**; `npm test`: **16 passed**; soak of
  10,000 scenarios: 0 failures. Details and remaining limitations: [HANDOVER.md](HANDOVER.md).

