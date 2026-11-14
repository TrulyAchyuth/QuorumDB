# Contributing

## Layout

```
core/         Raft, storage, KV state machine (plain Java, no dependencies)
simulation/   FaultLab: simulator, scenarios, checkers, analysis, soak runner
backend/      Spring Boot node: gRPC transport, REST API, live faults, live experiments
benchmarks/   Load generator
frontend/     React dashboard
proto/        gRPC / protobuf schema
docker/       Dockerfiles, nginx and Prometheus configuration
scripts/      cluster.sh (local cluster), bench.py, test_summary.py
docs/         evidence, screenshots, prior art, progress log, handover
```

## Rules

- **Keep `core` free of clocks, threads and I/O.** Anything that needs them goes through `Scheduler`,
  `Transport`, `RaftStorage` or `StateMachine`. Otherwise determinism and the simulator break.
- **Never weaken a test to make it pass.** If a test fails, find out whether the system or the test is
  wrong, and record the reasoning in the commit message.
- Changes to `RaftNode` must keep `./gradlew test` green and should be followed by a soak run
  (`./gradlew :simulation:soak -Pruns=5000`).
- New FaultLab checks need a test that shows they catch a seeded bug.
- Benchmark numbers in the docs must come from `scripts/bench.py` output, never be edited by hand.

## Before a pull request

```bash
./gradlew clean test :backend:integrationTest
(cd frontend && npm run build)
```
