# FaultLab

FaultLab is QuorumDB's built-in environment for answering one question with evidence: **when this
combination of failures happens, does the database still keep its promises, and why did it behave the way
it did?**

It has two engines that share the same checkers and analysis:

| | Simulation | Live cluster |
|---|---|---|
| What runs | The real `RaftNode` and `KvStateMachine` classes on a virtual clock, simulated network and in-memory disks | The real server processes over gRPC |
| Determinism | Same scenario + seed gives the identical event trace (SHA-256 checked) | No; wall-clock time, real scheduling |
| Speed | 10-20 s of cluster time in roughly 15-150 ms (measured on the development machine) | Real time |
| Faults | crash, restart, isolate, partition, delay, drop, duplicate (network-wide), disk-write failure, disk loss | crash, restart, isolate, partition, delay, drop, disk-write failure |
| Use it for | Exploring and reproducing failure modes, regression tests, soak testing | Checking the deployed system end to end |

## Scenario model

A scenario fixes the cluster size, Raft settings (timeouts, PreVote, CheckQuorum, snapshot threshold),
network latency, a client workload (clients, operation mix, read consistency mode, timeouts) and a fault
schedule. Each fault has:

- a **type**,
- **targets**: node ids or the selectors `leader`, `follower`, `random`, resolved when the fault fires,
- a **start**: a time, or a condition such as `COMMIT_INDEX_AT_LEAST 20` or `LEADER_ELECTED`,
- an **end**: a duration, a condition, or none.

```json
{ "name": "leader crash", "nodes": 3, "seed": 7, "durationMs": 10000,
  "faults": [ { "type": "CRASH", "targets": ["leader"],
                "start": { "when": "COMMIT_INDEX_AT_LEAST", "value": 20 }, "durationMs": 3000 } ] }
```

Thirteen built-in scenarios cover leader and follower crash, minority isolation, 2/3 splits of a five-node
cluster, losing the majority, lossy networks, snapshot catch-up, disk failure, rolling restart, combined
chaos, disruptive rejoin with and without PreVote, and deliberate disk loss.

## Determinism

- The simulator is single-threaded and event-driven. Tasks are ordered by `(virtual time, insertion
  order)`, and the only randomness comes from one `java.util.Random` seeded by the scenario.
- `RaftNode` never reads a clock or creates threads; it receives a `Scheduler` and a `RandomSource`.
- A run's trace hash covers every event (sequence number, time, node, type, term, index, entry checksum, peer, detail).
- `ScenarioTest.sameScenarioAndSeedReproduceTheExactEventTrace` and the property
  `everyRunIsReproducibleFromItsScenarioAndSeed` check this; the dashboard's *Verify replay* button does
  the same on demand.

## The safety checker

`SafetyChecker` never looks inside `RaftNode`. It consumes the recorded event stream (votes, elections,
appends, commits, applies, each carrying index, term and a CRC of the entry) plus periodic samples of every
node's durable log, and checks:

| Property | Violation means |
|---|---|
| Election Safety | two nodes became leader in the same term |
| Log Matching | same (index, term) with different content, or matching entries with different prefixes |
| Leader Completeness | a leader of a later term lacks an entry committed earlier |
| Commit Agreement | two different entries committed at the same index |
| Commit Durability | a committed entry disappeared from or changed in a node that considers it committed |
| Commit Progression | a node's commit index skipped or went backwards |
| State Machine Safety | applied out of order, applied before commit, different entries applied at one index, or equal applied prefixes with different data |
| Term Monotonicity | a node's term went backwards, including across restarts |

Each check reports:

- **PASS**: examined, nothing wrong, with the amount of evidence examined.
- **VIOLATED**: with the sequence numbers of the events that prove it.
- **NOT_EXERCISED**: nothing in the run could have violated it.
- **INCONCLUSIVE**: the history is known to be incomplete, for example a live node's event buffer overflowed.

Incomplete observation never turns into a pass.

**Does it actually catch bugs?** `SeededBugTest` runs real Raft on deliberately broken storage. Storage
that forgets votes is caught as two leaders in one term, and storage that ignores truncation is caught as
Log Matching or Commit violations. The unmodified code passes the same harness, which also asserts it really
committed thousands of entries and truncated logs, so the pass is not vacuous. The `disk-loss` scenario
shows the checker reporting genuine violations when Raft's durability assumption is broken, and labels
them as such.

## Linearizability checker

`LinearizabilityChecker` is black-box: it sees only what clients saw. That is invocation time, completion
time, operation and result, where operations whose outcome the client never learned are *indeterminate*.
It runs a Wing-Gong search with Lowe's memoisation per key, the same family of algorithm as Knossos and
Porcupine.

## Safety versus availability

FaultLab classifies every run:

| Headline | Meaning |
|---|---|
| `HEALTHY` | safe, linearizable, and every request outside fault windows succeeded |
| `EXPECTED_UNAVAILABILITY` | safe; requests failed only while a fault removed the quorum or leader, which is correct CP behaviour |
| `LIVENESS_ISSUE` | safe, but the cluster did not recover or requests failed with no fault to blame |
| `SAFETY_VIOLATION` | a safety property or linearizability was violated |
| `INCONCLUSIVE` | observations were incomplete |

## Explanations

The explanation for a run is produced by a rule-based summariser (`Explainer`), **not a language model**:

- The run is cut into episodes at every fault injection and heal.
- Each sentence is computed from events in that episode: who timed out first, why votes were refused, who
  won which term, who stepped down and why, which entries were truncated, how far the commit index moved
  and what clients experienced.
- Every finding lists the events it rests on, and in the dashboard each is a link into the timeline.
- If the events are not there, the sentence is not generated.

## Live experiments

`POST /api/v1/faultlab/live-runs` (admin token) runs the same scenario format against the real cluster:

1. Real HTTP clients send requests to random nodes.
2. Faults are injected on schedule through the live fault API.
3. A collector pulls every node's event log and samples every node's durable log.
4. At the end, all faults are healed (also on error), the cluster settles, and the run goes through the same
   checkers.

Differences, stated in each live report:

- Times are wall-clock.
- Events from different nodes are ordered by their local clocks.
- Replay re-runs the scenario in the simulator for comparison instead of reproducing it.
- Condition triggers and `DISK_LOSS` are simulator-only.

`RealClusterIT.liveFaultLabExperimentIsAuditedAndLeavesNoFaultsBehind` verifies that a live run is
audited and that no fault remains afterwards.

How the live audit avoids false verdicts (each was a real false verdict before it was fixed):

- **Whole-log sampling.** Durable logs are read page by page up to their end. A single 5,000-entry window
  made committed entries of a long log look missing after a restart (false Commit Durability violation;
  `RealClusterIT.liveAuditOfALongLogAcrossACrash...`).
- **Keys private to each run** (`faultlab-<run start>-<n>`). The linearizability model starts every key
  empty; shared key names made a second run on the same cluster read the first run's values
  (`RealClusterIT.consecutiveLiveRunsOnTheSameClusterAreEachLinearizable`).
- **Monotonic timing of client operations.** Real-time order decides linearizability, and a wall clock can
  step backwards (observed in Docker); operations are timed with `System.nanoTime` anchored at the run start.
- **Gaps are reported, not hidden.** If a node's event buffer (20,000 events) wraps before the collector
  reads it, the audit is INCONCLUSIVE. This happens under heavy external load during a run (observed with
  about 3,000 extra requests/s); run experiments without other traffic for a complete audit.

Known reporting issues in live runs (no effect on any verdict):

- The Elections panel counts the cluster as leaderless from the start of the run until the first election
  it observes, because the leader that existed before the run is not in the run's events. The Recovery
  panel shows the real time to a new leader.
- "Replication lag max" can show the whole commit index at the start of the run for the same reason.

Nine consecutive live runs on the Docker deployment, after these fixes: all SAFE, LINEARIZABLE, liveness
OK, no observation gaps ([docs/evidence/docker.md](docs/evidence/docker.md)).

## Comparing runs

`GET /api/v1/faultlab/compare?a=&b=` (and the dashboard) puts two runs side by side: verdicts,
availability, failed requests, latency, elections, leaderless windows, recovery times, replication lag,
truncations and message counts. It also notes differing seeds and settings. The built-in pair
`rejoin-no-prevote` / `rejoin-prevote` shows the effect of PreVote. Without it, an isolated follower
inflates its term to about 25 and deposes the leader when it returns; with it, the term stays at 1.

## Using it

- Dashboard: FaultLab page (simulate, runs and comparison, live cluster) and Event Timeline (swimlanes,
  replay, state at the playhead).
- API: `/api/v1/faultlab/scenarios`, `/runs`, `/runs/{id}`, `/runs/{id}/events`, `/runs/{id}/replay`,
  `/runs/{id}/export`, `/compare`, `/live-runs`.
- Command line: `./gradlew :simulation:soak -Pruns=5000 -PmasterSeed=1` runs thousands of generated scenarios.

What is and is not new compared with Jepsen, Maelstrom, FoundationDB and TigerBeetle is discussed in
[docs/PRIOR_ART.md](docs/PRIOR_ART.md).
