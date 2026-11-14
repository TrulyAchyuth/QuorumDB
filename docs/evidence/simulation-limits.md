# Simulated FaultLab runs: memory limits

## The problem (reproduced 2026-11-14)

`POST /api/v1/faultlab/runs` runs a scenario in the deterministic simulator. It needs no admin token
(it does not touch the real cluster). The finished run is kept in memory, with its full event trace.
Scenario validation alone bounds a run only loosely: up to 9 nodes, 600 s, 32 clients, 1 ms between
operations, 5 ms heartbeats. That allows traces of tens of millions of events.

One request against the **pre-fix** server (commit `cc6eb98`), a single node with `-Xmx256m`:

```json
{"scenario":{"name":"too large","nodes":9,"durationMs":600000,
  "cluster":{"electionTimeoutMinMs":10,"electionTimeoutMaxMs":20,"heartbeatIntervalMs":5},
  "workload":{"clients":32,"opIntervalMs":1,"keys":1000,"readRatio":0.5,"deleteRatio":0.0}}}
```

```
HTTP 500 after 1.15 s: {"error":"INTERNAL_ERROR","message":"internal error; see node log",...}
Exception in thread "http-nio-8091-Poller" java.lang.OutOfMemoryError: Java heap space
health after: unreachable        (process still alive)
```

The `OutOfMemoryError` killed Tomcat's poller thread, so the process stayed up but stopped answering
HTTP. Any client that could reach the API could take a node out this way.

## The fix

| Limit | Where | Value |
|---|---|---|
| Events per run | `ScenarioRunner.run(scenario, recordMessages, maxEvents)` stops the simulation as soon as the trace passes the budget. The API answers **422 `RUN_TOO_LARGE`** with advice on what to change. | 40% of the heap / simulations at once / 400 B, between 100,000 and 3,000,000 |
| Simulations at once | `FaultLabController` semaphore; more requests wait up to 30 s, then get 429 `BUSY` | min(CPUs / 2, 4), reduced on small heaps so each run still gets ≥ 100,000 events |
| Stored events | `RunStore` evicts the oldest runs while the total exceeds the budget (and keeps at most 40 runs). The newest run is always kept. | 25% of the heap / 400 B |

The limits are derived from the JVM's maximum heap at start-up (`SimulationLimits.forThisJvm()`) and
logged, for example: `FaultLab simulation limits: 134217 events per run, 4 at once, 335544 events stored`
for a 512 MB heap on a 10-core machine.

The budget is detected inside Raft event callbacks, where an exception would be caught by the node's
fail-stop guard and turn into a simulated node failure. So the runner halts the event loop and throws
only once control is back outside the simulation (`EventBudgetTest.budgetIsNotMistakenForANodeFailure`).
Library, test and soak runs keep an unlimited budget, and a run within its budget is identical to an
unlimited one (same trace hash).

## Sizing: bytes per event

`scripts/MeasureEventSize.java` runs scenarios in one JVM, keeps each result reachable, and divides the
post-GC heap growth by the number of events (JDK 21):

```
chaos            messages=false events=   24,903 retained=  2,445 KB  bytes/event=101
chaos            messages=true  events=   87,023 retained= 13,081 KB  bytes/event=154
leader-crash     messages=false events=    4,438 retained=    462 KB  bytes/event=107
leader-crash     messages=true  events=   13,018 retained=  1,920 KB  bytes/event=151
rolling-restart  messages=false events=    4,948 retained=    507 KB  bytes/event=105
rolling-restart  messages=true  events=   14,144 retained=  2,064 KB  bytes/event=149
big-9n-120s      messages=false events=2,601,285 retained=238,001 KB  bytes/event=94
big-9n-120s      messages=true  -> OutOfMemoryError with a 4 GB heap (while running)
```

Finished runs retain 94-154 bytes per event, but a run in progress needs much more than it finally
keeps: the last line ran out of a 4 GB heap. The limits therefore budget 400 bytes per event, about 2.6
times the largest retained figure, and the figure was validated end to end rather than trusted.

## End-to-end validation (fixed server)

`scripts/verify_sim_limits.py --heap <size>` starts one node, sends 3 rounds of 2 x (simulations at
once) oversized runs concurrently, then 45 runs of the largest built-in scenario (`chaos` with message
events, 87,023 events each), and checks health, responses and the log:

```
heap 256m: 134,217 events per run, 2 at once, 167,772 stored
oversized: {'422 RUN_TOO_LARGE': 12} in 0.6s
normal runs: 45/45 created in 4.9s; store holds 1 runs, 87,023 events
OutOfMemoryError in node log: 0
PASSED

heap 512m: 134,217 events per run, 4 at once, 335,544 stored
oversized: {'422 RUN_TOO_LARGE': 24} in 0.7s
normal runs: 45/45 created in 4.9s; store holds 3 runs, 261,069 events
OutOfMemoryError in node log: 0
PASSED
```

A first version used a floor of 50,000 events per run. On the 256 MB heap that gave 67,108 events per
run and refused the built-in `chaos` scenario (87,023 events) with 422. The floor is now 100,000, and
`SimulationLimitsTest.everyBuiltInScenarioFitsOneRunFromA256MbHeapUp` keeps it that way.

## Regression tests

- `EventBudgetTest` (simulation): over-budget runs stop early with a typed error, are not mistaken for
  node failures, and in-budget runs are identical to unlimited ones.
- `SimulationLimitsTest` (backend): the worst case fits the heap shares for 256 MB to 4 GB heaps and 1 to
  64 CPUs; every built-in scenario fits one run; the store evicts by total events and keeps the newest run.
- `RealClusterIT.oversizedSimulationIsRefusedAndTheNodeStaysHealthy`: the hostile request above gets
  422 from a real node (256 MB heap), which then still serves writes and normal simulations.

## Remaining limits

- Simulated runs still need no token. They are bounded now, but any client can keep the simulation slots
  busy (429 for others). Put the API behind a private network or proxy, as for the rest of QuorumDB.
- Live runs (admin token) are bounded by their 120 s maximum duration, not by an event budget.
