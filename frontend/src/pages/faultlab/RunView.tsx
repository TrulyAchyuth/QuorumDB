import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { ArrowLeft, Download, GitCompare, ListTree, RotateCcw } from "lucide-react";
import { api, base } from "../../api/client";
import type { Replay, RunDetail, RunSummary } from "../../api/types";
import { usePoll } from "../../hooks/usePoll";
import { Async, Badge, Callout, Card, Spinner, Stat, VerdictBadge } from "../../components/ui";
import { TimeSeriesChart } from "../../components/charts";
import { fmtInt, fmtMs, fmtPct } from "../../lib/format";

const secs = (ms: number) => `${(ms / 1000).toFixed(ms % 1000 === 0 ? 0 : 1)}s`;

export function RunView() {
  const { id = "" } = useParams();
  const run = usePoll((s) => api<RunDetail>(`/faultlab/runs/${id}`, { signal: s }), [id], 0);
  return (
    <>
      <div className="row" style={{ marginBottom: 12 }}>
        <Link to="/faultlab?tab=runs" className="row small"><ArrowLeft size={14} />All runs</Link>
      </div>
      <Async state={run} rows={8}>{(d) => <Report d={d} />}</Async>
    </>
  );
}

function Report({ d }: { d: RunDetail }) {
  const navigate = useNavigate();
  const [replay, setReplay] = useState<Replay | null>(null);
  const [replaying, setReplaying] = useState(false);
  const [replayError, setReplayError] = useState<string | null>(null);
  const [compareWith, setCompareWith] = useState("");
  const others = usePoll((s) => api<RunSummary[]>("/faultlab/runs", { signal: s }), [], 0);
  const s = d.summary;
  const m = d.metrics;
  const sim = s.mode === "SIMULATION";
  const bands = d.faults.filter((f) => f.healedAtMs !== f.injectedAtMs)
    .map((f) => ({ from: f.injectedAtMs, to: f.healedAtMs < 0 ? m.durationMs : f.healedAtMs }));
  const timeline = m.timeline.map((x) => ({ t: x.timeMs, commit: x.commitIndex, ok: x.opsOk, notOk: x.opsNotOk, lag: x.replicationLag }));

  async function doReplay() {
    setReplaying(true);
    setReplayError(null);
    try {
      setReplay(await api<Replay>(`/faultlab/runs/${s.id}/replay`, { method: "POST" }));
    } catch (e) {
      setReplayError((e as Error).message);
    } finally {
      setReplaying(false);
    }
  }

  return (
    <>
      <div className="page-head">
        <div>
          <div className="row" style={{ marginBottom: 6 }}>
            {sim ? <Badge>Simulated</Badge> : <Badge tone="accent">Live cluster</Badge>}
            <VerdictBadge value={s.headline} />
          </div>
          <h1>{s.name}</h1>
          <p className="small">
            <span className="mono">{s.id}</span> · {s.nodes} nodes · {sim ? `seed ${s.seed} · ` : ""}{secs(s.durationMs)} · {fmtInt(s.events)} events ·
            trace <span className="mono" title={s.traceHash}>{s.traceHash.slice(0, 12)}…</span>
          </p>
        </div>
        <div className="row wrap">
          <button className="btn" onClick={() => navigate(`/timeline?run=${s.id}`)}><ListTree />Timeline &amp; replay</button>
          <button className="btn" onClick={doReplay} disabled={replaying} title={sim ? "Re-run with the same scenario and seed and verify the trace hash" : "Run this scenario in the simulator"}>
            {replaying ? <Spinner /> : <RotateCcw />}{sim ? "Verify replay" : "Re-run in simulator"}</button>
          <a className="btn" href={`${base()}/faultlab/runs/${s.id}/export`} download><Download />Export</a>
        </div>
      </div>

      {replay && (
        <div style={{ marginBottom: 16 }}>
          <Callout tone={replay.mode === "SIMULATION" && !replay.identical ? "critical" : undefined}>
            <b>{replay.mode === "SIMULATION" ? (replay.identical ? "Replay identical. " : "Replay diverged. ") : "Simulated re-run created. "}</b>
            {replay.note}. Original <span className="mono">{replay.originalHash.slice(0, 16)}</span> ({fmtInt(replay.originalEvents)} events),
            replay <span className="mono">{replay.replayHash.slice(0, 16)}</span> ({fmtInt(replay.replayEvents)} events).{" "}
            <Link to={`/faultlab/runs/${replay.replayId}`}>Open replay</Link> · <Link to={`/faultlab/compare?a=${s.id}&b=${replay.replayId}`}>Compare</Link>
          </Callout>
        </div>
      )}
      {replayError && <div className="callout critical" style={{ marginBottom: 16 }}>{replayError}</div>}
      {d.safety.assumptionBreaches.length > 0 && (
        <div style={{ marginBottom: 16 }}>
          <Callout tone="warning"><b>This run deliberately broke an assumption Raft relies on.</b> {d.safety.assumptionBreaches.join("; ")}. Violations reported below are real, and are the expected consequence.</Callout>
        </div>
      )}

      <div className="grid cols-4">
        <Stat label="Safety audit" value={<VerdictBadge value={d.safety.verdict} />} hint={`${d.safety.checks.filter((c) => c.status === "PASS").length} of ${d.safety.checks.length} properties passed with evidence`} />
        <Stat label="Client history" value={<VerdictBadge value={d.linearizability?.result ?? "NOT_CHECKED"} />} hint={d.linearizability ? `${fmtInt(d.linearizability.operationsChecked)} ops on ${d.linearizability.keysChecked} keys` : "no workload"} />
        <Stat label="Liveness" value={<VerdictBadge value={d.liveness.status} />} hint={d.liveness.leaderAtEnd ? `leader ${d.liveness.leaderAtEnd} at end` : "no leader at end"} />
        <Stat label="Availability" value={fmtPct(m.availability)} hint={`${fmtInt(m.opsOk)} ok · ${fmtInt(m.opsFailed)} failed · ${fmtInt(m.opsIndeterminate)} unknown`} />
      </div>

      <div className="grid cols-3 mt">
        <Card title="What happened" sub="generated from recorded events; numbers link to the timeline" className="span-2" flush>
          <div>
            {d.explanation.map((f, i) => (
              <div key={i} style={{ padding: "12px 16px", borderBottom: "1px solid var(--border)" }}>
                <div className="row">
                  <span className="mono xs muted" style={{ width: 54 }}>{secs(f.timeMs)}</span>
                  <b className="small">{f.title}</b>
                </div>
                <p className="small secondary" style={{ margin: "4px 0 0 62px" }}>{f.detail}</p>
                {f.evidence.length > 0 && (
                  <div className="row wrap xs" style={{ margin: "6px 0 0 62px", gap: 4 }}>
                    <span className="muted">evidence</span>
                    {f.evidence.map((seq) => <Link key={seq} to={`/timeline?run=${s.id}&seq=${seq}`} className="mono badge outline" style={{ height: 18 }}>#{seq}</Link>)}
                  </div>
                )}
              </div>
            ))}
          </div>
        </Card>
        <div className="stack">
          <Card title="Faults injected" flush>
            {d.faults.length === 0 ? <p className="small secondary" style={{ padding: 16, margin: 0 }}>None: baseline run.</p> : (
              <table className="table">
                <tbody>
                  {d.faults.map((f) => (
                    <tr key={f.step}>
                      <td className="mono xs nowrap">{secs(f.injectedAtMs)}–{f.healedAtMs < 0 ? "end" : secs(f.healedAtMs)}</td>
                      <td className="small">{f.description}{f.hitLeader && <div><Badge tone="critical">hit the leader</Badge></div>}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </Card>
          <Card title="Compare with another run">
            <div className="row">
              <select className="select" style={{ flex: 1 }} value={compareWith} onChange={(e) => setCompareWith(e.target.value)} aria-label="Run to compare with">
                <option value="">Choose a run…</option>
                {(others.data ?? []).filter((r) => r.id !== s.id).map((r) => <option key={r.id} value={r.id}>{r.name} · {r.id}</option>)}
              </select>
              <button className="btn" disabled={!compareWith} onClick={() => navigate(`/faultlab/compare?a=${s.id}&b=${compareWith}`)}><GitCompare />Compare</button>
            </div>
          </Card>
        </div>
      </div>

      <div className="grid cols-2 mt">
        <Card title="Commit index" sub="highest committed log index; shaded = fault active">
          <TimeSeriesChart ariaLabel="Commit index over time" data={timeline} x="t" height={210} bands={bands} xFormat={secs}
            series={[{ key: "commit", label: "Commit index", color: "var(--series-1)" }]} valueFormat={(v) => fmtInt(v)} />
        </Card>
        <Card title="Client outcomes" sub="cumulative; shaded = fault active">
          <TimeSeriesChart ariaLabel="Cumulative client operations" data={timeline} x="t" height={210} bands={bands} xFormat={secs}
            series={[{ key: "ok", label: "Succeeded", color: "var(--series-1)" }, { key: "notOk", label: "Failed or unknown", color: "var(--series-2)" }]}
            valueFormat={(v) => fmtInt(v)} />
        </Card>
      </div>

      <Card title="Safety audit" sub="independent checker: reads events and sampled logs, never Raft internals" className="mt" flush>
        <div className="table-wrap">
          <table className="table">
            <thead><tr><th>Property</th><th>Result</th><th className="num">Evidence examined</th><th>Definition</th></tr></thead>
            <tbody>
              {d.safety.checks.map((c) => (
                <tr key={c.id}>
                  <td className="nowrap"><b>{c.name}</b></td>
                  <td><VerdictBadge value={c.status} /></td>
                  <td className="num nowrap">{fmtInt(c.examined)} <span className="muted xs">{c.examinedWhat}</span></td>
                  <td className="small secondary">
                    {c.property}
                    {c.violations.slice(0, 3).map((v, i) => (
                      <div key={i} style={{ color: "var(--critical-text)", marginTop: 4 }}>
                        {secs(v.timeMs)}: {v.message}{" "}
                        {v.evidence.map((seq) => <Link key={seq} className="mono xs" to={`/timeline?run=${s.id}&seq=${seq}`}>#{seq} </Link>)}
                      </div>
                    ))}
                    {c.violations.length > 3 && <div className="xs muted">and {c.violations.length - 3} more</div>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {d.safety.observationGaps.length > 0 && <div style={{ padding: 16 }}><Callout tone="warning">Incomplete observation: {d.safety.observationGaps.join("; ")}</Callout></div>}
      </Card>

      <div className="grid cols-2 mt">
        <Card title="Elections" sub={`${m.elections.length} leaders · mean leaderless window ${fmtMs(m.meanElectionMs)}`} flush>
          <table className="table">
            <thead><tr><th className="num">Term</th><th>Leader</th><th className="num">Leaderless since</th><th className="num">Elected at</th><th className="num">Window</th></tr></thead>
            <tbody>{m.elections.map((e) => (
              <tr key={e.term}><td className="num">{e.term}</td><td className="mono">{e.leader}</td><td className="num">{secs(e.leaderlessSinceMs)}</td>
                <td className="num">{secs(e.electedAtMs)}</td><td className="num">{fmtMs(e.durationMs)}</td></tr>))}</tbody>
          </table>
        </Card>
        <Card title="Recovery" sub="per fault" flush>
          {m.recoveries.length === 0 ? <p className="small secondary" style={{ padding: 16, margin: 0 }}>No faults injected.</p> : (
            <table className="table">
              <thead><tr><th>Fault</th><th className="num">New leader after</th><th className="num">First commit after</th><th className="num">Node caught up after</th></tr></thead>
              <tbody>{m.recoveries.map((r) => (
                <tr key={r.step}><td className="small">{r.fault}</td><td className="num">{fmtMs(r.newLeaderAfterMs)}</td>
                  <td className="num">{fmtMs(r.firstCommitAfterMs)}</td><td className="num">{fmtMs(r.catchUpMs)}</td></tr>))}</tbody>
            </table>
          )}
        </Card>
      </div>

      <div className="grid cols-2 mt">
        <Card title="Measurements" sub={sim ? "virtual time" : "wall-clock time"}>
          <dl className="kv">
            <dt>Throughput</dt><dd>{m.throughputOpsPerSec.toFixed(1)} ops/s</dd>
            <dt>Latency p50 / p95 / p99</dt><dd>{fmtMs(m.latencyP50Ms)} / {fmtMs(m.latencyP95Ms)} / {fmtMs(m.latencyP99Ms)}</dd>
            <dt>Failed during faults / outside faults</dt><dd>{fmtInt(m.failedDuringFaults)} / {fmtInt(m.failedOutsideFaults)}</dd>
            <dt>Elections started / leader changes</dt><dd>{fmtInt(m.electionsStarted)} / {fmtInt(m.leaderChanges)}</dd>
            <dt>Final term / commit index</dt><dd>{m.finalTerm} / {fmtInt(m.finalCommitIndex)}</dd>
            <dt>Replication lag mean / max</dt><dd>{m.meanReplicationLag.toFixed(1)} / {fmtInt(m.maxReplicationLag)} entries</dd>
            <dt>Log truncations</dt><dd>{fmtInt(m.logTruncations)}</dd>
            <dt>Snapshots created / installed</dt><dd>{fmtInt(m.snapshotsCreated)} / {fmtInt(m.snapshotsInstalled)}</dd>
            <dt>Messages sent / dropped</dt><dd>{fmtInt(m.messagesSent)} / {fmtInt(m.messagesDropped)}</dd>
          </dl>
        </Card>
        <Card title="Final state" sub="after the settle period" flush>
          <table className="table">
            <thead><tr><th>Node</th><th>Role</th><th className="num">Term</th><th className="num">Last index</th><th className="num">Applied</th><th className="num">Snapshot</th><th>State digest</th></tr></thead>
            <tbody>{d.finalState.map((n) => (
              <tr key={n.node}><td className="mono"><b>{n.node}</b></td><td className="xs">{n.role}</td><td className="num">{n.term}</td><td className="num">{fmtInt(n.lastIndex)}</td>
                <td className="num">{fmtInt(n.lastApplied)}</td><td className="num">{fmtInt(n.snapshotIndex)}</td>
                <td className="mono xs">{n.stateDigest ? n.stateDigest.toString(16) : "–"}</td></tr>))}</tbody>
          </table>
          <p className="small secondary" style={{ padding: "0 16px" }}>{d.liveness.findings.join(" ")}</p>
        </Card>
      </div>
    </>
  );
}
