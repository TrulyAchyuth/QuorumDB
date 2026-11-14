# Docker Compose deployment: verification

Real containers, real container network, real named volumes. Produced by `scripts/verify_docker.py`,
which uses only the public HTTP API and the docker CLI. Not a simulation.

Environment: 2026-11-14, Apple M5 host, Colima 4 vCPU / 6 GiB VM, Docker Engine 29.5 (client 29.9.3),
Docker Compose 5.7.1, images built from this repository with `docker compose build`.

```bash
export QUORUMDB_ADMIN_TOKEN=$(openssl rand -hex 24)
docker compose up --build -d
python3 scripts/verify_docker.py            # --compose docker-compose for the standalone binary
```

## Result (second run, from a clean `docker compose down -v`)

```
ok   all 3 nodes agree on leader n2
ok   write via n1 acknowledged
ok   write via n2 acknowledged
ok   write via n3 acknowledged
ok   all 3 writes readable from every node (stale local reads, so each node holds the data)
ok   linearizable read on the leader
ok   dashboard proxy /api/v1/cluster: HEALTHY
ok   dashboard serves index.html
ok   dashboard per-node route /nodes/n1 reaches n1
ok   dashboard per-node route /nodes/n2 reaches n2
ok   dashboard per-node route /nodes/n3 reaches n3
ok   new leader n3 elected among survivors
EVIDENCE docker kill -s KILL node2 (leader n2): first acknowledged write after 536 ms; 50 writes acknowledged, 1 rejected or failed
EVIDENCE n2 restarted from its volume and applied index 55 after 1.2s
ok   every write acknowledged during failover is on restarted n2
ok   ISOLATE n1 accepted (HTTP 201)
ok   cluster reports DEGRADED while a node is isolated
ok   majority keeps committing while a follower is isolated
ok   no fault left on n1
ok   no fault left on n2
ok   no fault left on n3
ok   fault injection without the admin token is rejected (HTTP 401)
ok   dashboard proxy answers with node1 stopped (health DEGRADED)
EVIDENCE full cluster stop/start: leader n3 after 2.8s, earlier writes intact on all nodes
ALL CHECKS PASSED
```

The first run (on a cluster that had been up for a few minutes) also passed every check: leader n3 killed,
first acknowledged write after 531 ms, restarted node caught up after 1.2 s, full restart elected a leader
after 2.9 s.

### Run after the snapshot, live-audit and simulation-limit fixes

After the snapshot, live-audit and simulation-limit fixes, the images were rebuilt and the stack redeployed
from `docker compose down -v`. Every check passed again:

```
EVIDENCE leader n1 visible on all nodes 2.7s after the check started
EVIDENCE docker kill -s KILL node1 (leader n1): first acknowledged write after 525 ms; 50 writes acknowledged, 1 rejected or failed
EVIDENCE n1 restarted from its volume and applied index 55 after 2.3s
EVIDENCE full cluster stop/start: leader n1 after 2.8s, earlier writes intact on all nodes
ALL CHECKS PASSED
```

In the containers (512 MB heap, 4 vCPUs) the nodes log
`FaultLab simulation limits: 268435 events per run, 2 at once, 335544 events stored`. The oversized
simulation from [simulation-limits.md](simulation-limits.md), sent through the dashboard proxy, got
`HTTP 422`, and the cluster stayed `HEALTHY`.

## What each step shows

| Step | Shows |
|---|---|
| Leader agreement | All three containers elected one leader and agree on it. |
| Writes via every node | Followers forward writes to the leader; all are acknowledged after a majority commit. |
| Stale reads on every node | Each container's own state machine holds the data (replication, not forwarding). |
| Dashboard routes | nginx serves the built dashboard, proxies `/api` and reaches each node via `/nodes/nX`. |
| `docker kill -s KILL` of the leader | A real process crash; the survivors elect a leader and keep acknowledging writes. The "first acknowledged write" time includes the client's own retries against the survivors, polled as fast as requests return. |
| Restart from the volume | The killed node recovers from its WAL and snapshot on the named volume and catches up; every write acknowledged during the failover is present on it. The time is measured from `docker compose start`. |
| Live ISOLATE fault | Fault injection through the API works in containers; the cluster reports DEGRADED, the majority keeps committing, the fault expires on its own and no fault state remains on any node. |
| No token, no faults | Fault injection is refused without the admin token. |
| node1 stopped | The dashboard's `/api` proxy falls back to node2/node3. |
| Full stop/start | All data survives on the volumes; a leader is elected again. |

## Notes

- One measurement per step, on a laptop. Failover time depends on the randomized election timeout
  (150-300 ms by default) plus detection; the process-level integration tests measure 270-293 ms.
- If `docker compose` reports "unknown command", the Compose plugin is not installed for the Docker CLI;
  the standalone `docker-compose` binary works the same (`--compose docker-compose`).


## Problems found by repeating the verification, and their fixes

Running the verification several times in a row (and with the dashboard open) exposed four problems
that a single pass had not shown. Each was diagnosed from logs before being fixed.

1. **Dashboard proxy answered 502 while nodes were up.** nginx's error log showed two mechanisms:
   - Node addresses were resolved once at nginx start-up, but container addresses change on restart
     (measured: node1 and node2 swapped `172.18.0.2` and `172.18.0.3` after a kill/stop/start sequence).
   - With `max_fails=1`, failed connects during start-up or while one node was stopped marked **every** node
     down, and nginx answered `no live upstreams` for the next seconds.
   A first fix with dynamic re-resolution (`resolve`) still produced `no live upstreams` once. The final
   configuration gives the nodes fixed addresses (`10.89.73.11-13`, docker-compose.yml), never marks nodes
   down (`max_fails=0`), retries reads on another node after any failure but writes only after a connection
   error, and sends `/api` to one node at a time (`hash` on a constant key) so that node-local data such as
   FaultLab runs is found again on the next request.
2. **Raft event timestamps drifted 7.5 minutes behind wall-clock time** inside the containers. They were
   computed as monotonic time plus an offset taken at start-up, and the VM's monotonic clock had fallen
   behind its (NTP-corrected) wall clock. Events are now stamped with the wall clock when they are recorded;
   after the fix the newest event on each node was 0.5 s old when read.
3. **False NOT_LINEARIZABLE verdicts in live FaultLab runs.**
   - Every run used the same key names, so a second run read the first run's values ("a value that no
     client ever wrote"). Keys are now private to each run.
   - A sequential client's operations were recorded out of order (op 1132 at 9,242 ms after op 1128 at
     9,311 ms), so the wall clock of the coordinating node stepped backwards during the run. The VM runs
     `systemd-timesyncd`; the individual adjustment was not captured. Client operations are now timed with a
     monotonic clock.
4. **The load generator could not benchmark the Docker cluster.** It used the leader's advertised URL
   (`http://node1:8080`, only resolvable inside the compose network). It now matches the leader's id against
   the targets it was given.

## Final state (all fixes, images rebuilt, clean volumes)

Three consecutive verification runs passed every check, including:

```
ok   dashboard /api sticks to one node while it is up (served by ['n1'])
ok   the run is readable through the proxy every time (HTTP [200, 200, 200, 200, 200])
ok   dashboard proxy answers 10/10 requests with node1 stopped (HTTP [200, 200, 200, 200, 200, 200, 200, 200, 200, 200])
ok   per-node route /nodes/n1 reports the stopped node as unreachable (HTTP 504)
ALL CHECKS PASSED
```

A poller requested `/api/v1/cluster` through the proxy every 0.25 s during those runs and recorded which
nodes answered their own health check at that moment: 98 requests while at least one node was up, **all
200**; 37 failures, all while no node was up (start-up and the deliberate full stop/start).

Then nine live FaultLab experiments ran back to back on the same cluster (leader crash at 3 s for 2.5 s,
follower isolation at 7.5 s for 2 s, 4 clients, linearizable reads), eight through the API and one started
from the dashboard:

| Run | Safety | Client history | Liveness | Ops | Availability | Observation gaps |
|---:|---|---|---|---:|---:|---:|
| 1 | SAFE | LINEARIZABLE | OK | 2,003 | 86.2% | 0 |
| 2 | SAFE | LINEARIZABLE | OK | 2,116 | 86.9% | 0 |
| 3 | SAFE | LINEARIZABLE | OK | 2,134 | 85.8% | 0 |
| 4 | SAFE | LINEARIZABLE | OK | 2,144 | 86.6% | 0 |
| 5 | SAFE | LINEARIZABLE | OK | 2,169 | 85.9% | 0 |
| 6 | SAFE | LINEARIZABLE | OK | 2,181 | 86.7% | 0 |
| 7 | SAFE | LINEARIZABLE | OK | 2,176 | 86.5% | 0 |
| 8 | SAFE | LINEARIZABLE | OK | 2,177 | 85.3% | 0 |
| 9 (dashboard) | SAFE | LINEARIZABLE | OK | 1,265 | 85.5% | 0 |

Every crash hit the current leader. The failed requests are the expected unavailability during the faults.

## Throughput seen in Docker is not a benchmark

The overview screenshot shows about 1,000 writes/s plus 1,000 reads/s (6 clients, 50/50). That is several
times the local-process benchmarks, because fsync inside the Colima VM is far cheaper than on the macOS host
(and may not reach the physical disk). Only `BENCHMARKS.md` numbers are benchmarks. A 100 s run on the final
images had 0 errors, no gap, and a worst write of 28 ms. Earlier builds showed one ~1 s dip to about 40% of
the rate per minute in two screenshots; it did not line up with snapshots and was not investigated further.
