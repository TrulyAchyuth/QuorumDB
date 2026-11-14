# Handover

State of QuorumDB on 2026-11-14: what works, how it was verified, what is known to be missing, and where to
continue. Every claim below links to evidence produced by a test or script in this repository.

## Run it

```bash
# Local processes (JDK 21): 3 nodes on 8081-8083, dashboard dev server on 5173
scripts/cluster.sh start 3
cd frontend && npm ci && npm run dev

# Docker (3 nodes on 8081-8083, dashboard on http://localhost:3000)
export QUORUMDB_ADMIN_TOKEN=$(openssl rand -hex 24)   # enables fault injection; paste it in Settings
docker compose up --build -d
python3 scripts/verify_docker.py                      # end-to-end checks against the running stack
```

Tests: `./gradlew test :backend:integrationTest` (backend, about 2 minutes), `cd frontend && npm test`.

## Verified (2026-11-14)

| What | Result | Evidence |
|---|---|---|
| Backend tests | 171/171 (162 unit/simulation/property, 9 real-process) | [evidence/test-summary.md](evidence/test-summary.md) |
| Frontend tests | 16/16 Vitest; strict typecheck + build; `npm audit` 0 vulnerabilities | `frontend/src/**/*.test.ts` |
| FaultLab soak | 10,000 random fault scenarios, 0 failures (22.2 h simulated, 56 M events audited) | `./gradlew :simulation:soak -Pruns=10000 -PmasterSeed=20261114` |
| Docker deployment | election, replication, SIGKILL failover (~530 ms to first acknowledged write), restart from volume, live fault + cleanup, token enforcement, proxy failover, full stop/start; 3 consecutive passes | [evidence/docker.md](evidence/docker.md) |
| Live FaultLab on Docker | 9 consecutive runs (leader crash + follower isolation): all SAFE, LINEARIZABLE, liveness OK, no observation gaps | [evidence/docker.md](evidence/docker.md) |
| Simulation limits | oversized run refused with 422 in 130 ms; stress at 256 MB and 512 MB heaps without OutOfMemoryError | [evidence/simulation-limits.md](evidence/simulation-limits.md) |
| Snapshot fix | 2 snapshot sends per run instead of 38-1,120; worst write 43-66 ms instead of 47-772 ms; no measurable throughput change | [BENCHMARKS.md](../BENCHMARKS.md#snapshot-storm-investigation) |

Simulation results (FaultLab simulator, soak) and real-cluster results (integration tests, Docker, benchmarks)
are kept apart throughout; simulated times are never reported as real measurements.

## Fixed in this round (all with a test that failed before the fix, unless noted)

1. Live audit reported a false Commit Durability violation for logs longer than 5,000 entries after a restart.
2. Leader re-sent the whole snapshot on every heartbeat/read-round reply (snapshot storm).
3. One unauthenticated simulation request crashed a node with OutOfMemoryError.
4. Back-to-back live runs were reported NOT_LINEARIZABLE (shared key names).
5. Live runs timed client operations with the wall clock, which stepped backwards in Docker (no automated
   test: a clock step cannot be produced in a test without a clock seam; verified with 9 live runs on Docker).
6. Raft event timestamps drifted minutes behind wall-clock time in Docker (verified on Docker, not in a test).
7. Dashboard proxy 502s while nodes were up, and `/api` spread over nodes losing node-local FaultLab runs
   (verified with `scripts/verify_docker.py` and an availability-correlated poller).
8. Load generator could not reach the leader of a Docker cluster.

## Known limitations

- Fixed membership (no joint consensus); no TLS or authentication on the client and Raft ports.
- Storage runs on the Raft thread: reads queue behind fsync under mixed load (the weakest benchmark).
- Snapshots compact the log up to the applied index, so a follower a few entries behind gets a whole
  snapshot; whole-state snapshots are held in memory and sent as one message.
- Simulated FaultLab runs need no token (bounded, but slots can be kept busy).
- Live FaultLab: event buffers of 20,000 per node can wrap under heavy external load (INCONCLUSIVE); the
  Elections panel and "replication lag max" use the run start as baseline (reporting only).
- The 2026-11-13 15-second throughput drop was not reproduced; the snapshot storm is a plausible contributor.
- All measurements come from one laptop; the Docker VM's fsync is not comparable with the host's.

## Where to continue (in order of value)

1. Move storage I/O off the Raft thread (largest performance gain, see the mixed-load benchmark).
2. Keep a trailing window of log entries after a snapshot (avoids snapshots for slightly lagging followers).
3. Give the live analyzer the pre-run leader and commit index as a baseline.
4. A clock seam in `LiveExperimentService` so the monotonic-timing fix gets a regression test.
5. Authentication for the client API, or at least for simulated runs.
