import { useMemo, useState } from "react";
import data from "../data/benchmarks.json";
import { Card, Callout } from "../components/ui";
import { GroupedBarChart, TimeSeriesChart } from "../components/charts";
import { fmtInt, fmtMs, fmtNum } from "../lib/format";

interface Stats { ok: number; errors: number; opsPerSecond: number; p50Ms: number; p95Ms: number; p99Ms: number; maxMs: number }
interface Result {
  label: string; nodes: number; concurrency: number; readRatio: number; valueBytes: number; consistency: string; spread: boolean;
  writes: Stats; reads: Stats; totalOpsPerSecond: number; longestUnavailableMs: number; errors: Record<string, number>;
  nodeCpuMeanPct: number | null; nodeCpuMaxPct: number | null; nodeHeapMaxMB: number | null; fault?: string; timeline100ms?: number[][];
  storage?: { node: string; diskBytes: number; keys: number; lastIndex: number; snapshotIndex: number }[];
}
const bench = data as unknown as { timestamp: string; machine: Record<string, string | number>; durationSeconds: number; warmupSeconds: number; results: Result[] };

const SIZE_SERIES = [
  { key: "n1", label: "1 node", color: "var(--series-1)" },
  { key: "n3", label: "3 nodes", color: "var(--series-2)" },
  { key: "n5", label: "5 nodes", color: "var(--series-3)" },
];

export function Benchmarks() {
  const results = bench.results;
  const scaling = useMemo(() => [1, 16, 64].map((c) => {
    const row: Record<string, number | string> = { c: String(c) };
    for (const n of [1, 3, 5]) {
      const r = results.find((x) => x.label === `write-n${n}-c${c}`);
      if (r) row[`n${n}`] = r.writes.opsPerSecond;
    }
    return row;
  }), [results]);
  const latency = useMemo(() => [1, 16, 64].map((c) => {
    const row: Record<string, number | string> = { c: String(c) };
    for (const n of [1, 3, 5]) {
      const r = results.find((x) => x.label === `write-n${n}-c${c}`);
      if (r) row[`n${n}`] = r.writes.p99Ms;
    }
    return row;
  }), [results]);
  const reads = results.filter((r) => r.label.startsWith("read-"));
  const faults = results.filter((r) => r.fault);
  const [a, setA] = useState(results[4]?.label ?? results[0].label);
  const [b, setB] = useState(results.find((r) => r.label.includes("no-fsync"))?.label ?? results[1].label);
  const ra = results.find((r) => r.label === a)!;
  const rb = results.find((r) => r.label === b)!;
  const m = bench.machine;

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Benchmark centre</h1>
          <p>Measured with <code>scripts/bench.py</code> against fresh local clusters of real processes. Re-run it to replace these numbers; nothing here is estimated.</p>
        </div>
      </div>
      <Callout>
        <b>Recorded {bench.timestamp.replace(/(\d{4})(\d\d)(\d\d)-(\d\d)(\d\d)(\d\d)/, "$1-$2-$3 $4:$5 UTC")}</b> on {String(m.cpu)}, {String(m.cores)} cores, {String(m.memoryGB)} GB, {String(m.os)}; {String(m.java)}.
        {" "}{String(m.note)}. Closed-loop clients, {bench.warmupSeconds} s warm-up, {bench.durationSeconds} s measured. Every write is fsynced unless labelled no-fsync.
      </Callout>

      <div className="grid cols-2 mt">
        <Card title="Write throughput by cluster size" sub="successful writes per second">
          <GroupedBarChart ariaLabel="Write throughput by concurrency and cluster size" data={scaling} category="c" series={SIZE_SERIES}
            categoryLabel={(c) => `${c} client${c === "1" ? "" : "s"}`} valueFormat={(v) => `${fmtInt(v)} /s`} yFormat={(v) => fmtInt(v)} />
        </Card>
        <Card title="Write latency p99 by cluster size" sub="milliseconds, client round trip">
          <GroupedBarChart ariaLabel="Write p99 latency by concurrency and cluster size" data={latency} category="c" series={SIZE_SERIES}
            categoryLabel={(c) => `${c} client${c === "1" ? "" : "s"}`} valueFormat={(v) => fmtMs(v)} yFormat={(v) => `${v}`} />
        </Card>
      </div>

      <Card title="What the numbers say" sub="computed from the results above" className="mt">
        <ul className="small secondary" style={{ margin: 0, paddingLeft: 18, lineHeight: 1.7 }}>
          {insights(results).map((t) => <li key={t}>{t}</li>)}
        </ul>
      </Card>

      <div className="grid cols-2 mt">
        <Card title="Reads by consistency level" sub="3 nodes, 32 clients">
          <GroupedBarChart ariaLabel="Read throughput by consistency level" data={reads.map((r) => ({ c: r.consistency, ops: r.reads.opsPerSecond }))} category="c"
            series={[{ key: "ops", label: "Reads per second", color: "var(--series-2)" }]} categoryLabel={(c) => c} valueFormat={(v) => `${fmtInt(v)} /s`} yFormat={(v) => fmtInt(v)} />
        </Card>
        <Card title="Throughput during a leader failure" sub="successful writes per 100 ms; line marks the fault">
          {faults[0]?.timeline100ms ? (
            <TimeSeriesChart ariaLabel="Writes per 100 ms during leader failure" height={220}
              data={faults[0].timeline100ms.map((p, i) => ({ t: i * 100, killed: p[0], isolated: faults[1]?.timeline100ms?.[i]?.[0] ?? null }))}
              x="t" xFormat={(t) => `${(t / 1000).toFixed(1)}s`} markers={[{ x: (bench.durationSeconds / 2) * 1000, label: "fault" }]}
              series={[{ key: "killed", label: "Leader SIGKILL", color: "var(--series-1)" }, { key: "isolated", label: "Leader isolated", color: "var(--series-2)" }]}
              valueFormat={(v) => `${fmtInt(v)} writes`} />
          ) : <p className="small secondary">No fault runs in this result set.</p>}
          <div className="stack small" style={{ marginTop: 8 }}>
            {faults.map((f) => <div key={f.label}><b>{f.fault?.replace(/ at [\d.]+$/, "")}</b>: longest window with zero successful writes <b>{f.longestUnavailableMs} ms</b> (100 ms resolution); errors {JSON.stringify(f.errors)}</div>)}
          </div>
        </Card>
      </div>

      <Card title="All configurations" className="mt" flush>
        <div className="table-wrap">
          <table className="table">
            <thead><tr><th>Configuration</th><th className="num">Nodes</th><th className="num">Clients</th><th className="num">Writes/s</th><th className="num">p50</th><th className="num">p95</th><th className="num">p99</th>
              <th className="num">Reads/s</th><th className="num">p50</th><th className="num">p99</th><th className="num">Errors</th><th className="num">CPU mean / max</th><th className="num">Heap max</th></tr></thead>
            <tbody>
              {results.map((r) => (
                <tr key={r.label}>
                  <td className="mono xs">{r.label}</td><td className="num">{r.nodes}</td><td className="num">{r.concurrency}</td>
                  <td className="num">{r.writes.ok ? fmtInt(r.writes.opsPerSecond) : "–"}</td><td className="num">{r.writes.ok ? fmtMs(r.writes.p50Ms) : "–"}</td>
                  <td className="num">{r.writes.ok ? fmtMs(r.writes.p95Ms) : "–"}</td><td className="num">{r.writes.ok ? fmtMs(r.writes.p99Ms) : "–"}</td>
                  <td className="num">{r.reads.ok ? fmtInt(r.reads.opsPerSecond) : "–"}</td><td className="num">{r.reads.ok ? fmtMs(r.reads.p50Ms) : "–"}</td>
                  <td className="num">{r.reads.ok ? fmtMs(r.reads.p99Ms) : "–"}</td>
                  <td className="num">{fmtInt(r.writes.errors + r.reads.errors)}</td>
                  <td className="num">{r.nodeCpuMeanPct ?? "–"}% / {r.nodeCpuMaxPct ?? "–"}%</td><td className="num">{fmtNum(r.nodeHeapMaxMB, 0)} MB</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>

      <Card title="Compare two configurations" className="mt">
        <div className="row wrap" style={{ marginBottom: 12 }}>
          <select className="select" value={a} onChange={(e) => setA(e.target.value)} aria-label="Configuration A">{results.map((r) => <option key={r.label}>{r.label}</option>)}</select>
          <span className="muted">vs</span>
          <select className="select" value={b} onChange={(e) => setB(e.target.value)} aria-label="Configuration B">{results.map((r) => <option key={r.label}>{r.label}</option>)}</select>
        </div>
        <table className="table">
          <thead><tr><th>Metric</th><th className="num">{a}</th><th className="num">{b}</th><th className="num">B / A</th></tr></thead>
          <tbody>
            {([["Total ops/s", (r: Result) => r.totalOpsPerSecond], ["Write p50 (ms)", (r: Result) => r.writes.p50Ms], ["Write p99 (ms)", (r: Result) => r.writes.p99Ms],
              ["Read p50 (ms)", (r: Result) => r.reads.p50Ms], ["Read p99 (ms)", (r: Result) => r.reads.p99Ms], ["Node CPU mean (%)", (r: Result) => r.nodeCpuMeanPct ?? 0]] as const)
              .map(([name, f]) => (
                <tr key={name}><td>{name}</td><td className="num">{fmtNum(f(ra), 2)}</td><td className="num">{fmtNum(f(rb), 2)}</td>
                  <td className="num muted">{f(ra) ? `${fmtNum(f(rb) / f(ra), 2)}×` : "–"}</td></tr>
              ))}
          </tbody>
        </table>
      </Card>

      {results.filter((r) => r.storage).map((r) => (
        <Card key={r.label} title="Storage overhead" sub={`after ${r.label}`} className="mt" flush>
          <table className="table">
            <thead><tr><th>Node</th><th className="num">Disk</th><th className="num">Keys</th><th className="num">Last index</th><th className="num">Snapshot index</th></tr></thead>
            <tbody>{r.storage!.map((s) => <tr key={s.node}><td className="mono">{s.node}</td><td className="num">{(s.diskBytes / 1e6).toFixed(1)} MB</td><td className="num">{fmtInt(s.keys)}</td><td className="num">{fmtInt(s.lastIndex)}</td><td className="num">{fmtInt(s.snapshotIndex)}</td></tr>)}</tbody>
          </table>
          <p className="small secondary" style={{ padding: "0 16px" }}>Disk holds the latest snapshot plus WAL segments (16 MB each) that still contain entries after it; whole segments are deleted once a snapshot covers them.</p>
        </Card>
      ))}
    </>
  );
}

/** Plain-language observations, each computed from the loaded results (omitted if its inputs are missing). */
function insights(results: Result[]): string[] {
  const get = (label: string) => results.find((r) => r.label === label);
  const out: string[] = [];
  const n1c1 = get("write-n1-c1"), n3c1 = get("write-n3-c1"), n1c64 = get("write-n1-c64"), n3c64 = get("write-n3-c64"), n5c64 = get("write-n5-c64");
  const n3c16 = get("write-n3-c16"), noFsync = get("write-n3-c64-no-fsync");
  const lin = get("read-linearizable-n3-c32"), leader = get("read-leader-n3-c32");
  if (n1c1 && n3c1) out.push(`Replication costs latency: one client's write takes ${fmtMs(n3c1.writes.p50Ms)} (p50) on 3 nodes vs ${fmtMs(n1c1.writes.p50Ms)} on 1 node (${fmtNum(n3c1.writes.p50Ms / n1c1.writes.p50Ms, 1)}×): the leader's fsync, a follower's fsync and two network hops.`);
  if (n3c1 && n3c64) out.push(`Group commit: on 3 nodes, 64 concurrent clients reach ${fmtInt(n3c64.writes.opsPerSecond)} writes/s vs ${fmtInt(n3c1.writes.opsPerSecond)} with one client (${fmtNum(n3c64.writes.opsPerSecond / n3c1.writes.opsPerSecond, 0)}×), because concurrent writes share one WAL append and fsync.`);
  if (n3c64 && n5c64) out.push(`Five nodes vs three at 64 clients: ${fmtInt(n5c64.writes.opsPerSecond)} vs ${fmtInt(n3c64.writes.opsPerSecond)} writes/s; the leader replicates to four followers and needs three acknowledgements.`);
  if (n1c64 && n3c64) out.push(`One node vs three at 64 clients: ${fmtInt(n1c64.writes.opsPerSecond)} vs ${fmtInt(n3c64.writes.opsPerSecond)} writes/s. A single node is faster but tolerates no failures.`);
  if (noFsync && n3c64) out.push(`Disabling fsync gives ${fmtInt(noFsync.writes.opsPerSecond)} writes/s (${fmtNum(noFsync.writes.opsPerSecond / n3c64.writes.opsPerSecond, 1)}× the fsync run), so the disk is the bottleneck, not the protocol. That mode is not crash-safe.`);
  if (lin && leader) out.push(`Linearizable reads (${fmtInt(lin.reads.opsPerSecond)}/s) cost a heartbeat round to a majority; leader-local reads skip it (${fmtInt(leader.reads.opsPerSecond)}/s) but can be stale during a partition.`);
  if (n3c16) out.push(`Node CPU stayed low (mean ${n3c16.nodeCpuMeanPct ?? "–"}% at 16 clients): writes wait on the disk, not the CPU.`);
  return out;
}
