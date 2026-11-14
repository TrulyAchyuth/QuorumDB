#!/usr/bin/env python3
"""Run the QuorumDB benchmark matrix against real local clusters and record the results.

Usage: scripts/bench.py [--quick]

Every number written by this script is measured: each configuration starts a fresh cluster with
scripts/cluster.sh, drives it with the closed-loop load generator in benchmarks/, and samples
node metrics over HTTP. Results go to benchmarks/results/<timestamp>/ (raw JSON per run) and a
Markdown summary to docs/evidence/benchmarks.md.
"""
import json
import os
import platform
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLUSTER = os.path.join(ROOT, "scripts", "cluster.sh")
GEN = os.path.join(ROOT, "benchmarks", "build", "install", "benchmarks", "bin", "benchmarks")
QUICK = "--quick" in sys.argv
DURATION = 6 if QUICK else 15
WARMUP = 2 if QUICK else 3
STAMP = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
OUT = os.path.join(ROOT, "benchmarks", "results", STAMP)
os.makedirs(OUT, exist_ok=True)


def sh(*args, env=None, check=True):
    return subprocess.run(args, cwd=ROOT, env={**os.environ, **(env or {})}, check=check,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True).stdout


def get(url, timeout=2):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def targets(n):
    return ",".join(f"http://localhost:{8080 + i}" for i in range(1, n + 1))


def start_cluster(n, fsync=True):
    sh(CLUSTER, "clean")
    sh(CLUSTER, "start", str(n), env={"QUORUMDB_FSYNC": "true" if fsync else "false"})
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            get("http://localhost:8081/api/v1/leader")
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("no leader")


def leader_port():
    url = get("http://localhost:8081/api/v1/leader")["httpUrl"]
    return int(url.rsplit(":", 1)[1])


def sample_resources(n, stop, samples):
    """Every second: CPU load and heap of every node process."""
    while not stop.is_set():
        row = {}
        for i in range(1, n + 1):
            try:
                m = get(f"http://localhost:{8080 + i}/api/v1/metrics?window=5", timeout=1)
                row[f"n{i}"] = {"cpu": m["processCpuLoad"], "heapMB": m["heapUsedBytes"] / 1e6}
            except Exception:
                pass
        samples.append(row)
        stop.wait(1.0)


def load(label, n, concurrency, read_ratio=0.0, consistency="linearizable", value_bytes=128, spread=False,
         timeline=False, during=None):
    args = [GEN, "--targets", targets(n), "--concurrency", str(concurrency), "--duration", str(DURATION),
            "--warmup", str(WARMUP), "--read-ratio", str(read_ratio), "--consistency", consistency,
            "--value-bytes", str(value_bytes), "--label", label, "--out", os.path.join(OUT, label + ".json")]
    if spread:
        args.append("--spread")
    if timeline:
        args.append("--timeline")
    stop, samples = threading.Event(), []
    sampler = threading.Thread(target=sample_resources, args=(n, stop, samples), daemon=True)
    sampler.start()
    proc = subprocess.Popen(args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    extra = during() if during else None
    output, _ = proc.communicate()
    stop.set()
    sampler.join()
    result = json.loads(output.strip().splitlines()[-1])
    cpu = [v["cpu"] for row in samples[2:] for v in row.values() if v["cpu"] >= 0]
    heap = [v["heapMB"] for row in samples[2:] for v in row.values()]
    result["nodeCpuMeanPct"] = round(100 * sum(cpu) / len(cpu), 1) if cpu else None
    result["nodeCpuMaxPct"] = round(100 * max(cpu), 1) if cpu else None
    result["nodeHeapMaxMB"] = round(max(heap), 1) if heap else None
    result["nodes"] = n
    if extra:
        result.update(extra)
    with open(os.path.join(OUT, label + ".json"), "w") as f:
        json.dump(result, f, indent=2)
    w, r = result["writes"], result["reads"]
    print(f"{label:34s} n={n} c={concurrency:3d} writes/s={w['opsPerSecond']:>10} p50={w['p50Ms']} p99={w['p99Ms']} "
          f"reads/s={r['opsPerSecond']:>10} p50={r['p50Ms']} p99={r['p99Ms']} errors={result['errors']}", flush=True)
    return result


def storage_overhead(n, label):
    rows = []
    for i in range(1, n + 1):
        node = get(f"http://localhost:{8080 + i}/api/v1/node")
        rows.append({"node": node["id"], "diskBytes": node["storage"]["diskBytes"], "keys": node["storage"]["keys"],
                     "lastIndex": node["storage"]["lastIndex"], "snapshotIndex": node["storage"]["snapshotIndex"]})
    with open(os.path.join(OUT, label + "-storage.json"), "w") as f:
        json.dump(rows, f, indent=2)
    return rows


def main():
    sh(os.path.join(ROOT, "gradlew"), "-q", ":backend:bootJar", ":benchmarks:installDist")
    machine = {
        "cpu": sh("sysctl", "-n", "machdep.cpu.brand_string", check=False).strip() or platform.processor(),
        "cores": os.cpu_count(),
        "memoryGB": round(int(sh("sysctl", "-n", "hw.memsize", check=False).strip() or 0) / 2**30, 1),
        "os": f"{platform.system()} {platform.release()}",
        "java": sh("java", "-version", check=False).splitlines()[0],
        "jvmFlags": "-Xmx512m (per node), defaults otherwise",
        "note": "load generator and all nodes share one machine; networking is localhost",
    }
    results = []
    concurrencies = [1, 16, 64]
    for n in (1, 3, 5):
        start_cluster(n)
        for c in concurrencies:
            results.append(load(f"write-n{n}-c{c}", n, c))
        if n == 3:
            results[-1]["storage"] = storage_overhead(3, "write-n3-c64")
            results.append(load("read-linearizable-n3-c32", 3, 32, read_ratio=1.0))
            results.append(load("read-leader-n3-c32", 3, 32, read_ratio=1.0, consistency="leader"))
            results.append(load("read-stale-n3-c32", 3, 32, read_ratio=1.0, consistency="stale", spread=True))
            results.append(load("mixed-50-50-n3-c32", 3, 32, read_ratio=0.5))
            results.append(load("write-1KiB-n3-c32", 3, 32, value_bytes=1024))
            results.append(load("write-via-followers-n3-c32", 3, 32, spread=True))
    sh(CLUSTER, "stop")

    start_cluster(3, fsync=False)
    results.append(load("write-n3-c64-no-fsync", 3, 64))
    sh(CLUSTER, "stop")

    # Fault: SIGKILL the leader mid-run; measure the longest window with zero successful requests.
    start_cluster(3)

    def kill_leader():
        time.sleep(WARMUP + DURATION / 2)
        port = leader_port()
        victim = f"n{port - 8080}"
        t = time.time()
        sh(CLUSTER, "kill", victim)
        return {"fault": f"SIGKILL leader {victim} at {round(t, 3)}"}
    results.append(load("write-n3-c32-leader-killed", 3, 32, timeline=True, during=kill_leader))
    sh(CLUSTER, "stop")

    def isolate_leader():
        time.sleep(WARMUP + DURATION / 2)
        token = open(os.path.join(ROOT, ".run", "admin-token")).read().strip()
        req = urllib.request.Request("http://localhost:8081/api/v1/faults", method="POST",
                                     data=json.dumps({"type": "ISOLATE", "targets": ["leader"], "durationMs": 3000}).encode(),
                                     headers={"Content-Type": "application/json", "Authorization": "Bearer " + token})
        with urllib.request.urlopen(req, timeout=5) as r:
            return {"fault": json.loads(r.read())["description"]}
    start_cluster(3)
    results.append(load("write-n3-c32-leader-isolated", 3, 32, timeline=True, during=isolate_leader))
    sh(CLUSTER, "clean")

    summary = {"timestamp": STAMP, "machine": machine, "durationSeconds": DURATION, "warmupSeconds": WARMUP,
               "results": results}
    with open(os.path.join(OUT, "results.json"), "w") as f:
        json.dump(summary, f, indent=2)
    write_markdown(summary)
    # The dashboard's Benchmark Centre shows the latest measured results.
    with open(os.path.join(ROOT, "frontend", "src", "data", "benchmarks.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print("results:", os.path.relpath(OUT, ROOT))


def write_markdown(s):
    m = s["machine"]
    lines = ["# Benchmark results", "",
             f"Measured {s['timestamp']} UTC by `scripts/bench.py`; raw JSON in `benchmarks/results/{s['timestamp']}/`.", "",
             f"- Machine: {m['cpu']}, {m['cores']} cores, {m['memoryGB']} GB RAM, {m['os']}",
             f"- JVM: {m['java']}; {m['jvmFlags']}",
             f"- Setup: {m['note']}. Closed-loop clients (no think time), {s['warmupSeconds']} s warm-up, "
             f"{s['durationSeconds']} s measured per configuration, fresh cluster per node count.",
             "- Durability: fsync on every WAL append unless the label says `no-fsync`.",
             "- Latency is client-side round trip in milliseconds; throughput counts successful requests only.", "",
             "| Configuration | Nodes | Clients | Writes/s | Write p50 | p95 | p99 | Reads/s | Read p50 | p95 | p99 | Errors | Node CPU mean / max % | Max heap MB |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|"]
    for r in s["results"]:
        w, rd = r["writes"], r["reads"]
        lines.append(f"| {r['label']} | {r['nodes']} | {r['concurrency']} | {w['opsPerSecond']} | {w['p50Ms']} | {w['p95Ms']} | "
                     f"{w['p99Ms']} | {rd['opsPerSecond']} | {rd['p50Ms']} | {rd['p95Ms']} | {rd['p99Ms']} | "
                     f"{json.dumps(r['errors']) if r['errors'] else 0} | {r['nodeCpuMeanPct']} / {r['nodeCpuMaxPct']} | {r['nodeHeapMaxMB']} |")
    lines += ["", "## Fault runs", ""]
    for r in s["results"]:
        if "fault" in r:
            lines.append(f"- **{r['label']}**: {r['fault']}. Longest window with zero successful requests: "
                         f"**{r['longestUnavailableMs']} ms** (100 ms buckets). Errors: {json.dumps(r['errors'])}.")
    for r in s["results"]:
        if "storage" in r:
            lines += ["", "## Storage after `" + r["label"] + "`", "",
                      "| Node | Disk bytes | Keys | Last index | Snapshot index |", "|---|---:|---:|---:|---:|"]
            for st in r["storage"]:
                lines.append(f"| {st['node']} | {st['diskBytes']} | {st['keys']} | {st['lastIndex']} | {st['snapshotIndex']} |")
    with open(os.path.join(ROOT, "docs", "evidence", "benchmarks.md"), "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
