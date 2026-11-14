# Changelog

## 0.1.1 (2026-11-14)

- **Fix (Raft performance):** while a snapshot was in flight, every reply to a heartbeat or ReadIndex
  read round made the leader send the whole snapshot again: 511-962 copies of a 300 KB snapshot per
  120 s run under mixed load. Now one snapshot is in flight per follower, resent only after a timeout.
- **Fix (security):** an unauthenticated oversized FaultLab simulation could exhaust a node's heap
  (reproduced: `OutOfMemoryError` in 1.2 s, node stopped answering HTTP). Simulations now have heap-derived
  limits on events per run, concurrent runs and stored events; oversized runs get `422 RUN_TOO_LARGE`.
- **Fix (FaultLab):** live audits of a node whose log held more than 5,000 entries past its snapshot
  reported a false Commit Durability violation after the node restarted; the audit now reads the whole log.
- **Fix (FaultLab):** live runs reused key names, so a second run on the same cluster was reported NOT_LINEARIZABLE;
  keys are now private to each run. Client operations are timed with a monotonic clock (a wall-clock step
  backwards in Docker produced another false NOT_LINEARIZABLE).
- **Fix (observability):** Raft event timestamps were monotonic time plus a start-up offset and drifted 7.5
  minutes behind wall-clock time in Docker; they are now stamped with the wall clock when recorded.
- **Fix (deployment):** the dashboard proxy answered 502 while nodes were up (stale container addresses, all
  nodes marked down at once) and spread `/api` over all nodes, which lost FaultLab runs kept on one node. Nodes
  now have fixed addresses, nodes are never marked down, `/api` sticks to one node with failover, and writes
  are retried on another node only after a connection error.
- **Fix (benchmarks):** the load generator located the leader by its advertised URL, which is not reachable
  from outside Docker; it now matches the leader's id against its targets.
- **Deployment:** Docker Compose deployment verified end to end (`scripts/verify_docker.py`, also run in CI).
- **Tests:** frontend unit tests (Vitest) in CI; regression tests for all three fixes.
- **Tools:** `scripts/snapshot_stall.py` (steady-state dip experiment), `scripts/verify_sim_limits.py`,
  `scripts/MeasureEventSize.java`; `QUORUMDB_JAR` in `scripts/cluster.sh` to run a specific build.

## 0.1.0 (2026-11-13)

First complete version.

- **Raft core:** election with PreVote and CheckQuorum, replication with fast backtracking, current-term
  commit rule, snapshots and InstallSnapshot, ReadIndex reads, fail-stop on storage errors.
- **Storage:** segmented CRC32C write-ahead log, atomic hard-state and snapshot files, torn-write recovery,
  corruption detection.
- **KV state machine:** put, get, delete, atomic batch, compare-and-set, scan, idempotent retries.
- **Server:** gRPC transport with validation and back-pressure, REST API with leader forwarding and group
  commit, three read consistency levels, Prometheus metrics, event and operation logs.
- **FaultLab:**
  - deterministic simulator and 13 scenarios;
  - safety checker (8 properties), linearizability checker, liveness assessment;
  - evidence-linked explanations, replay verification, run comparison;
  - live fault injection and audited live experiments.
- **Dashboard:** overview, cluster explorer, operations console, FaultLab, event timeline with replay,
  benchmarks, settings; light and dark themes.
- **Benchmarks:** closed-loop load generator and matrix runner with measured results.
- **Deployment:** Docker images and docker-compose (3 nodes + dashboard + optional Prometheus), plus
  `scripts/cluster.sh` for plain processes.
- **Fixes found by testing:**
  - a malformed committed command no longer halts every replica;
  - the dashboard no longer loses rare events under write load;
  - cluster health now reports DEGRADED while a member has no leader.
