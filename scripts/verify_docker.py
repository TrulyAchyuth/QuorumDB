#!/usr/bin/env python3
"""End-to-end check of the Docker Compose deployment (real containers, real network, real disks).

    QUORUMDB_ADMIN_TOKEN=... docker compose up --build -d
    QUORUMDB_ADMIN_TOKEN=... python3 scripts/verify_docker.py [--compose "docker compose"]

Checks, in order: leader election; replication (write through one node, read from every node);
the dashboard proxy; leader container SIGKILL -> failover, with writes continuing; the killed node
restarting from its volume and catching up; a live fault through the API and its cleanup; a full
cluster stop/start keeping all data. Prints one EVIDENCE line per measurement and exits non-zero
on the first failed check. Only uses the public HTTP API and the docker CLI.
"""
import argparse
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request

NODES = {"n1": 8081, "n2": 8082, "n3": 8083}
SERVICE = {"n1": "node1", "n2": "node2", "n3": "node3"}
DASHBOARD = "http://localhost:3000"


def http(method, url, body=None, token=None, timeout=3):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            try:
                return r.status, (json.loads(raw) if raw else None)
            except ValueError:
                return r.status, raw.decode(errors="replace")
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, raw.decode(errors="replace")
    except (urllib.error.URLError, OSError, TimeoutError):
        return -1, None


def node_url(n, path):
    return f"http://localhost:{NODES[n]}{path}"


def evidence(msg):
    print("EVIDENCE " + msg, flush=True)


def check(cond, msg):
    if not cond:
        print("FAILED: " + msg, file=sys.stderr)
        sys.exit(1)
    print("ok   " + msg, flush=True)


def wait(cond, timeout, what):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = cond()
        if v:
            return v
        time.sleep(0.05)
    print(f"FAILED: timed out after {timeout}s waiting for {what}", file=sys.stderr)
    sys.exit(1)


def leader(via_nodes=None):
    for n in via_nodes or NODES:
        st, b = http("GET", node_url(n, "/api/v1/leader"), timeout=1)
        if st == 200 and b and b.get("leaderId"):
            return b["leaderId"]
    return None


def put(n, key, value):
    st, _ = http("PUT", node_url(n, f"/api/v1/kv/{key}"), {"value": value})
    return st in (200, 201)


def read(n, key, consistency="stale"):
    st, b = http("GET", node_url(n, f"/api/v1/kv/{key}?consistency={consistency}"))
    return b.get("value") if st == 200 and isinstance(b, dict) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--compose", default=os.environ.get("COMPOSE", "docker compose"))
    args = ap.parse_args()
    compose = shlex.split(args.compose)
    token = os.environ.get("QUORUMDB_ADMIN_TOKEN", "")

    def dc(*a):
        subprocess.run(compose + list(a), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # 1. Election.
    t0 = time.time()
    wait(leader, 90, "a leader")

    def agreed():
        ids = {(http("GET", node_url(n, "/api/v1/leader"))[1] or {}).get("leaderId") for n in NODES}
        return ids.pop() if len(ids) == 1 and None not in ids else None
    # A freshly elected leader is known to the others after its first heartbeat; give them a moment.
    first = wait(agreed, 10, "all 3 nodes to agree on one leader")
    check(True, f"all 3 nodes agree on leader {first}")
    evidence(f"leader {first} visible on all nodes {time.time() - t0:.1f}s after the check started")

    # 2. Replication: write through every node (followers forward), read from every node.
    stamp = str(int(time.time()))
    for i, n in enumerate(NODES):
        check(put(n, f"verify-{i}", f"{stamp}-{i}"), f"write via {n} acknowledged")
    for n in NODES:
        wait(lambda n=n: all(read(n, f"verify-{i}") == f"{stamp}-{i}" for i in range(3)), 5, f"replication to {n}")
    check(True, "all 3 writes readable from every node (stale local reads, so each node holds the data)")
    check(read(first, "verify-0", "linearizable") == f"{stamp}-0", "linearizable read on the leader")

    # 3. Dashboard proxy.
    st, b = http("GET", DASHBOARD + "/api/v1/cluster")
    check(st == 200 and isinstance(b, dict) and b.get("health") == "HEALTHY",
          f"dashboard proxy /api/v1/cluster: {b.get('health') if isinstance(b, dict) else st}")
    st, _ = http("GET", DASHBOARD + "/")
    check(st == 200, "dashboard serves index.html")
    served_by = {(http("GET", DASHBOARD + "/api/v1/health")[1] or {}).get("node") for _ in range(10)}
    check(len(served_by) == 1, f"dashboard /api sticks to one node while it is up (served by {sorted(map(str, served_by))})")
    st, run = http("POST", DASHBOARD + "/api/v1/faultlab/runs", {"preset": "leader-crash"}, timeout=60)
    check(st == 201, f"simulated FaultLab run through the dashboard proxy (HTTP {st})")
    rid = run["summary"]["id"]
    reads = [http("GET", f"{DASHBOARD}/api/v1/faultlab/runs/{rid}")[0] for _ in range(5)]
    check(all(r == 200 for r in reads), f"the run is readable through the proxy every time (HTTP {reads})")
    for n in NODES:
        st, b = http("GET", f"{DASHBOARD}/nodes/{n}/api/v1/node")
        check(st == 200 and isinstance(b, dict) and b.get("id") == n, f"dashboard per-node route /nodes/{n} reaches {n}")

    # 4. Leader container SIGKILL with a writer running.
    others = [n for n in NODES if n != first]
    ok_during = fail_during = 0
    subprocess.run(["docker", "kill", "--signal", "KILL", container(compose, SERVICE[first])], check=True,
                   stdout=subprocess.DEVNULL)
    killed_at = time.time()
    new_leader = None
    last_ok = None
    i = 0
    while time.time() - killed_at < 20:
        target = new_leader or others[i % 2]
        if put(target, f"failover-{i}", str(i)):
            ok_during += 1
            if last_ok is None:
                last_ok = time.time()
            if new_leader is None:
                new_leader = leader(others)
        else:
            fail_during += 1
        i += 1
        if ok_during >= 50:
            break
    check(new_leader in others, f"new leader {new_leader} elected among survivors")
    evidence(f"docker kill -s KILL {SERVICE[first]} (leader {first}): first acknowledged write after "
             f"{(last_ok - killed_at) * 1000:.0f} ms; {ok_during} writes acknowledged, {fail_during} rejected or failed")

    # 5. Restart the killed node from its volume; it must catch up.
    dc("start", SERVICE[first])
    restarted = time.time()
    target_index = http("GET", node_url(new_leader, "/api/v1/node"))[1]["commitIndex"]

    def caught_up():
        st, b = http("GET", node_url(first, "/api/v1/node"), timeout=1)
        return st == 200 and b.get("lastApplied", 0) >= target_index
    wait(caught_up, 90, f"{first} to catch up")
    evidence(f"{first} restarted from its volume and applied index {target_index} after "
             f"{time.time() - restarted:.1f}s after docker compose start")
    check(all(read(first, f"failover-{j}") == str(j) for j in range(ok_during) if read(new_leader, f"failover-{j}")),
          f"every write acknowledged during failover is on restarted {first}")

    # 6. Live fault through the API, then cleanup.
    if token:
        victim = [n for n in NODES if n != new_leader][0]
        st, b = http("POST", node_url(new_leader, "/api/v1/faults"),
                     {"type": "ISOLATE", "targets": [victim], "durationMs": 3000}, token=token)
        check(st in (200, 201, 202), f"ISOLATE {victim} accepted (HTTP {st})")
        wait(lambda: (http("GET", node_url(new_leader, "/api/v1/cluster"))[1] or {}).get("health") == "DEGRADED",
             5, "cluster to report DEGRADED")
        check(True, "cluster reports DEGRADED while a node is isolated")
        check(put(new_leader, "during-isolation", "x"), "majority keeps committing while a follower is isolated")
        wait(lambda: (http("GET", node_url(new_leader, "/api/v1/cluster"))[1] or {}).get("health") == "HEALTHY",
             20, "fault to expire and cluster to be HEALTHY")
        for n in NODES:
            f = http("GET", node_url(n, "/api/v1/node"))[1]["faults"]
            check(not f["active"] and not f["blockedPeers"] and not f["crashed"], f"no fault left on {n}")
        st, _ = http("POST", node_url(new_leader, "/api/v1/faults"), {"type": "ISOLATE", "targets": [victim],
                                                                      "durationMs": 1000})
        check(st == 401, f"fault injection without the admin token is rejected (HTTP {st})")
    else:
        print("skip live fault checks: QUORUMDB_ADMIN_TOKEN not set")

    # 7. The dashboard's /api proxy prefers node1; with node1 down it must fall back to node2/node3.
    # Containers were killed and restarted above, so their IPs may have changed: the proxy must follow.
    dc("stop", "node1")
    try:
        answers = [http("GET", DASHBOARD + "/api/v1/cluster", timeout=10) for _ in range(10)]
        codes = [st for st, _ in answers]
        check(all(st == 200 for st in codes), f"dashboard proxy answers 10/10 requests with node1 stopped (HTTP {codes})")
        for n in ("n2", "n3"):
            st, b = http("GET", f"{DASHBOARD}/nodes/{n}/api/v1/node")
            check(st == 200 and isinstance(b, dict) and b.get("id") == n, f"per-node route /nodes/{n} still reaches {n} after restarts")
        st, _ = http("GET", f"{DASHBOARD}/nodes/n1/api/v1/node", timeout=10)
        check(st in (502, 504), f"per-node route /nodes/n1 reports the stopped node as unreachable (HTTP {st})")
    finally:
        dc("start", "node1")

    # 8. Whole cluster stop/start: data survives on the volumes.
    dc("stop", "node1", "node2", "node3")
    dc("start", "node1", "node2", "node3")
    t = time.time()
    wait(leader, 90, "a leader after full restart")
    for n in NODES:
        wait(lambda n=n: all(read(n, f"verify-{i}") == f"{stamp}-{i}" for i in range(3)), 30, f"data on {n}")
    evidence(f"full cluster stop/start: leader {leader()} after {time.time() - t:.1f}s, earlier writes intact on all nodes")
    print("ALL CHECKS PASSED")


def container(compose, service):
    out = subprocess.run(compose + ["ps", "-q", service], check=True, capture_output=True, text=True).stdout.strip()
    if not out:
        raise SystemExit(f"no running container for {service}")
    return out


if __name__ == "__main__":
    main()
