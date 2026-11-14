#!/usr/bin/env python3
"""Steady-state throughput-dip experiment (real processes, localhost).

Starts a fresh 3-node cluster, runs a fixed mixed workload (8 closed-loop clients, 50% writes,
linearizable reads, 120 s) with a 100 ms timeline, then pulls snapshot and leadership events from
every node and reports what happened around each snapshot.

    python3 scripts/snapshot_stall.py --label after  [--jar path/to/quorumdb-server.jar] [--runs 3]

Writes benchmarks/results/stall-<label>-<n>.json per run and prints a summary line per run.
Used to investigate the periodic dips seen under steady load (see BENCHMARKS.md).
"""
import argparse
import json
import os
import statistics
import subprocess
import sys
import urllib.request
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "benchmarks", "build", "install", "benchmarks", "bin", "benchmarks")
TYPES = "SNAPSHOT_CREATED,SNAPSHOT_SENT,SNAPSHOT_INSTALLED,STEPPED_DOWN,LEADER_ELECTED,ELECTION_STARTED"


def sh(*args, env=None):
    subprocess.run(list(args), cwd=ROOT, check=True, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def events(port):
    with urllib.request.urlopen(f"http://localhost:{port}/api/v1/events?limit=5000&types={TYPES}", timeout=5) as r:
        return json.load(r)["events"]


def one_run(label, n, jar, duration):
    env = dict(os.environ)
    if jar:
        env["QUORUMDB_JAR"] = os.path.abspath(jar)
    sh("scripts/cluster.sh", "clean")
    sh("scripts/cluster.sh", "start", "3", env=env)
    subprocess.run(["sleep", "6"])
    out = os.path.join(ROOT, "benchmarks", "results", f"stall-{label}-{n}.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    subprocess.run([GEN, "--targets", "http://localhost:8081,http://localhost:8082,http://localhost:8083",
                    "--concurrency", "8", "--duration", str(duration), "--warmup", "2", "--read-ratio", "0.5",
                    "--consistency", "linearizable", "--timeline", "--label", f"stall-{label}", "--out", out],
                   check=True, stdout=subprocess.DEVNULL)
    report = json.load(open(out))
    evs = [e for p in (8081, 8082, 8083) for e in events(p)]
    sh("scripts/cluster.sh", "stop")

    end = datetime.fromisoformat(report["timestamp"].replace("Z", "+00:00")).timestamp() * 1000
    start = end - duration * 1000
    tl = [b[0] for b in report["timeline100ms"]]
    per_s = [sum(tl[i:i + 10]) for i in range(0, len(tl) - 9, 10)]
    in_window = [e for e in evs if start <= e["timeMs"] <= end]
    snaps_created = sorted({e["index"] for e in in_window if e["type"] == "SNAPSHOT_CREATED"})
    sent = [e for e in in_window if e["type"] == "SNAPSHOT_SENT"]
    leader_loss = [e for e in in_window if e["type"] == "STEPPED_DOWN" and "PRE_CANDIDATE" in (e.get("detail") or "")
                   or e["type"] in ("ELECTION_STARTED", "LEADER_ELECTED")]
    summary = {
        "label": label, "run": n,
        "opsPerSecond": round(report["totalOpsPerSecond"], 1),
        "writesPerSecond": round(report["writes"]["opsPerSecond"], 1),
        "medianOpsPerSecond": statistics.median(per_s), "minOpsPerSecond": min(per_s),
        "zero100msBuckets": sum(1 for b in tl if b == 0),
        "longestUnavailableMs": report["longestUnavailableMs"],
        "writeMaxMs": round(report["writes"]["maxMs"]), "writeP99Ms": round(report["writes"]["p99Ms"], 1),
        "snapshotsCreatedAt": snaps_created, "snapshotSends": len(sent),
        # Seconds into the measured window, so stalls in the timeline can be matched to snapshots.
        "snapshotTimesS": sorted({round((e["timeMs"] - start) / 1000, 2) for e in in_window
                                  if e["type"] == "SNAPSHOT_CREATED"}),
        "zeroBucketTimesS": [i / 10 for i, b in enumerate(tl) if b == 0],
        "leaderLossEvents": len(leader_loss),
    }
    report["stallAnalysis"] = summary
    json.dump(report, open(out, "w"), indent=1)
    print(json.dumps(summary), flush=True)
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--jar")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--duration", type=int, default=120)
    a = ap.parse_args()
    if not os.path.exists(GEN):
        subprocess.run(["./gradlew", "-q", ":benchmarks:installDist"], cwd=ROOT, check=True)
    for n in range(1, a.runs + 1):
        one_run(a.label, n, a.jar, a.duration)


if __name__ == "__main__":
    sys.exit(main())
