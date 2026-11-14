#!/usr/bin/env python3
"""Stress test of the FaultLab simulation limits on a real node.

Starts one node with a given heap, then (1) fires many oversized simulations at once, several rounds,
(2) fills the run store with large normal runs, and (3) checks that the node is still healthy, that
oversized runs were refused with 422 RUN_TOO_LARGE (or 429 BUSY when all slots were taken), and that
the node log has no OutOfMemoryError.

    python3 scripts/verify_sim_limits.py --heap 256m [--jar backend/build/libs/quorumdb-server.jar]
"""
import argparse
import concurrent.futures as cf
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = 8095
HOSTILE = {"name": "too large", "nodes": 9, "durationMs": 600000,
           "cluster": {"electionTimeoutMinMs": 10, "electionTimeoutMaxMs": 20, "heartbeatIntervalMs": 5},
           "workload": {"clients": 32, "opIntervalMs": 1, "keys": 1000, "readRatio": 0.5, "deleteRatio": 0.0}}


def post(path, body, timeout=300):
    req = urllib.request.Request(f"http://localhost:{PORT}{path}", data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except ValueError:
            return e.code, None
    except Exception as e:  # noqa: BLE001 - any transport failure counts as "node not answering"
        return -1, str(e)


def healthy():
    try:
        with urllib.request.urlopen(f"http://localhost:{PORT}/api/v1/health", timeout=5) as r:
            return json.load(r).get("status") == "UP"
    except Exception:  # noqa: BLE001
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--heap", default="256m")
    ap.add_argument("--jar", default=os.path.join(ROOT, "backend", "build", "libs", "quorumdb-server.jar"))
    ap.add_argument("--rounds", type=int, default=3)
    a = ap.parse_args()
    work = tempfile.mkdtemp(prefix="quorumdb-simlimits-")
    log_path = os.path.join(work, "node.log")
    log = open(log_path, "w")
    node = subprocess.Popen(["java", f"-Xmx{a.heap}", "-jar", a.jar, f"--server.port={PORT}", "--quorumdb.node-id=n1",
                             "--quorumdb.grpc-port=9195", f"--quorumdb.data-dir={work}/data"], stdout=log,
                            stderr=subprocess.STDOUT)
    failures = []
    try:
        for _ in range(120):
            if healthy():
                break
            time.sleep(0.5)
        else:
            raise SystemExit("node did not start: " + open(log_path).read()[-2000:])
        m = re.search(r"FaultLab simulation limits: (\d+) events per run, (\d+) at once, (\d+) events stored",
                      open(log_path).read())
        per_run, concurrent, stored = (int(x) for x in m.groups())
        print(f"heap {a.heap}: {per_run:,} events per run, {concurrent} at once, {stored:,} stored")

        statuses = {}
        t0 = time.time()
        for _ in range(a.rounds):
            with cf.ThreadPoolExecutor(concurrent * 2) as pool:
                for st, body in pool.map(lambda _: post("/api/v1/faultlab/runs", {"scenario": HOSTILE}),
                                         range(concurrent * 2)):
                    key = f"{st} {body.get('error') if isinstance(body, dict) else body}"
                    statuses[key] = statuses.get(key, 0) + 1
        print(f"oversized: {statuses} in {time.time() - t0:.1f}s")
        if any(not k.startswith(("422 RUN_TOO_LARGE", "429 BUSY")) for k in statuses):
            failures.append(f"unexpected responses to oversized runs: {statuses}")

        # Fill the store with the largest built-in run (chaos with message events), more than it may keep.
        t0 = time.time()
        ok = 0
        for _ in range(45):
            st, _ = post("/api/v1/faultlab/runs", {"preset": "chaos", "recordMessages": True})
            ok += st == 201
        with urllib.request.urlopen(f"http://localhost:{PORT}/api/v1/faultlab/runs", timeout=10) as r:
            kept = json.load(r)
        held = sum(s["events"] for s in kept)
        if not kept:
            failures.append("the store kept no runs")
        print(f"normal runs: {ok}/45 created in {time.time() - t0:.1f}s; store holds {len(kept)} runs, {held:,} events")
        if ok != 45:
            failures.append(f"only {ok}/45 normal runs succeeded")
        if kept and held > max(stored, max(s["events"] for s in kept)):
            failures.append(f"store holds {held} events, above its budget of {stored}")
        if not healthy():
            failures.append("node not healthy afterwards")
    finally:
        node.terminate()
        node.wait(30)
        log.close()
    ooms = open(log_path).read().count("OutOfMemoryError")
    print(f"OutOfMemoryError in node log: {ooms}")
    if ooms:
        failures.append(f"{ooms} OutOfMemoryError(s) in the node log ({log_path})")
    if failures:
        print("FAILED: " + "; ".join(failures))
        return 1
    shutil.rmtree(work, ignore_errors=True)
    print("PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
