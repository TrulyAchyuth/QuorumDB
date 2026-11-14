# Testing

## How to run

```bash
./gradlew test                          # unit, simulation and property tests (about 30 s)
./gradlew :backend:integrationTest      # starts real server processes (about 40 s)
./gradlew clean test :backend:integrationTest && python3 scripts/test_summary.py   # evidence file
./gradlew :simulation:soak -Pruns=5000 -PmasterSeed=1                              # random fault soak
cd frontend && npm test && npm run build   # Vitest unit tests, then typecheck + production build
python3 scripts/verify_docker.py        # against `docker compose up -d` (see docs/evidence/docker.md)
python3 scripts/verify_sim_limits.py --heap 256m   # FaultLab memory limits on a real node
```

Pin the randomness of property tests with `-Dquorumdb.seed=...`. jqwik prints the seed of every property
run, and a failing property prints the complete scenario, which reproduces the failure exactly.

## What is tested

| Layer | Suites | Focus |
|---|---|---|
| Raft rules | `RaftElectionTest` (18), `RaftReplicationTest` (28) | Driven message by message with a manual clock: voting rules, persistence before reply, PreVote, CheckQuorum, log matching, conflict truncation, stale and duplicated messages, Figure 8, ReadIndex, snapshots (including one snapshot in flight per follower), fail-stop |
| Storage | `StorageContractTest` (10), `FileStorageRecoveryTest` (12) | Same contract for memory and file storage; crash recovery, torn writes, corruption, atomic files, locking |
| State machine | `KvStateMachineTest` (9) | Operations, CAS, atomic batches, idempotency, snapshots, codecs |
| Checkers | `SafetyCheckerTest` (19), `LinearizabilityCheckerTest` (11) | Each property violated by a hand-written history must be caught; legal histories must pass |
| Checker strength | `SeededBugTest` (3) | Real Raft on deliberately broken storage must be flagged; unmodified Raft must pass a non-vacuous harness |
| Scenarios | `ScenarioTest` (27), `MinorityPartitionTest` (5) | Every built-in scenario safe and linearizable; determinism; minority cannot commit; restarts recover; read-consistency semantics across 25 seeds |
| Resource limits | `EventBudgetTest` (4), `SimulationLimitsTest` (4) | Over-budget simulations stop early with a typed error that is not mistaken for a node failure; heap-derived limits fit the heap for 256 MB-4 GB and 1-64 CPUs; every built-in scenario fits; run store evicts by total events |
| Properties | `RandomFaultProperties` (3) | 300 random fault schedules must stay safe and linearizable, 150 must recover after healing, 40 must be reproducible |
| Server | `ProtoCodecTest`, `ClusterConfigTest`, `EventLogTest` | Wire validation, configuration validation, event paging |
| Real cluster | `RealClusterIT` (9) | Real processes: election, leader SIGKILL with writes during failover and catch-up, isolated leader, 5-node 2/3 split, full-cluster SIGKILL and restart, audited live experiment with clean-up, live audit of a >5,000-entry log across a crash (no false violation), consecutive live runs each linearizable, oversized simulation refused with the node staying healthy |
| Dashboard | Vitest: `format`, `events`, `api/client` (16) | Formatting, event metadata, API routing (pinned node / per node), error mapping including non-JSON proxy errors and unreachable API, admin token only on admin calls and only in sessionStorage |
| Deployment | `scripts/verify_docker.py` | Real containers: election, replication, dashboard proxy (sticky to one node, FaultLab runs readable through it, failover with a node stopped, per-node routes after IP changes), leader SIGKILL failover, restart from volume, live fault and cleanup, token enforcement, full stop/start |

The exact counts and timings of the latest run are in [docs/evidence/test-summary.md](docs/evidence/test-summary.md),
generated from the JUnit XML reports. Soak results are in [docs/evidence/soak.md](docs/evidence/soak.md).

## Principles

- **No vacuous passes.** Tests that check "nothing bad happened" also assert that the interesting thing
  happened (elections, commits, truncations, enough operations), and the safety checker reports
  `NOT_EXERCISED` rather than `PASS` when it saw nothing.
- **Verdicts against ground truth.** The stale-read test computes from the trace whether a stale read
  actually happened in each seed, and requires the checker's verdict to match exactly in both directions.
- **Real measurements are labelled.** Integration tests print `EVIDENCE` lines with real wall-clock
  measurements. Simulated times are never reported as real-cluster results.

## Limits

- Simulation explores the interleavings its seeded network and scheduler produce, not all interleavings;
  it is not a model checker.
- Live experiments run on one machine; cross-host clock skew and real network partitions (as opposed to
  injected message drops) are not exercised.
- No Jepsen run against the real cluster.
