import { useState } from "react";
import { Link } from "react-router-dom";
import { Bandage, KeyRound, Play, Zap } from "lucide-react";
import { api } from "../../api/client";
import type { ClusterSummary, Config, LiveStatus, Scenario } from "../../api/types";
import { usePoll } from "../../hooks/usePoll";
import { useSettings } from "../../hooks/useSettings";
import { Async, Badge, Callout, Card, ConfirmDialog, Empty, Spinner, VerdictBadge } from "../../components/ui";
import { FAULT_TYPES, ScenarioEditor } from "./ScenarioEditor";
import { fmtDuration } from "../../lib/format";

const LIVE_DEFAULT: Scenario = {
  name: "Live: leader crash and follower isolation", description: "", nodes: 3, seed: 1, durationMs: 12000,
  cluster: { electionTimeoutMinMs: 150, electionTimeoutMaxMs: 300, heartbeatIntervalMs: 50, preVote: true, checkQuorum: true, snapshotThreshold: 0, netLatencyMinMs: 1, netLatencyMaxMs: 5 },
  workload: { clients: 4, opIntervalMs: 30, keys: 5, readRatio: 0.5, deleteRatio: 0.05, opTimeoutMs: 1500, readMode: "LINEARIZABLE" },
  faults: [
    { type: "CRASH", targets: ["leader"], start: { atMs: 3000 }, durationMs: 2500 },
    { type: "ISOLATE", targets: ["follower"], start: { atMs: 7500 }, durationMs: 2000 },
  ],
};

export function LiveTab() {
  const settings = useSettings();
  const config = usePoll((s) => api<Config>("/config", { signal: s }), [], 0);
  const cluster = usePoll((s) => api<ClusterSummary>("/cluster", { signal: s }), [], 1500);
  const enabled = config.data?.faultInjectionEnabled;
  const hasToken = settings.adminToken.length > 0;

  return (
    <div className="stack gap-lg">
      {config.data && !enabled && (
        <Callout tone="warning" icon={<KeyRound />}>
          Fault injection is <b>disabled</b> on this cluster (secure default). Start the nodes with <code>quorumdb.admin-token</code> set
          (<code>scripts/cluster.sh</code> does this automatically; for Docker, export <code>QUORUMDB_ADMIN_TOKEN</code>).
        </Callout>
      )}
      {enabled && !hasToken && (
        <Callout icon={<KeyRound />}>Live faults need the cluster's admin token. Add it in <Link to="/settings">Settings</Link>; it is kept only for this browser tab.</Callout>
      )}
      <div className="grid cols-2">
        <InjectFault disabled={!enabled || !hasToken} nodes={cluster.data?.nodes.map((n) => n.id) ?? []} />
        <ActiveFaults cluster={cluster} disabled={!enabled || !hasToken} />
      </div>
      <LiveExperiment disabled={!enabled || !hasToken} clusterSize={cluster.data?.size ?? 3} />
    </div>
  );
}

function InjectFault({ disabled, nodes }: { disabled: boolean; nodes: string[] }) {
  const [type, setType] = useState("ISOLATE");
  const [targets, setTargets] = useState<string[]>(["leader"]);
  const [duration, setDuration] = useState<number | "">(3000);
  const [delayMs, setDelayMs] = useState(100);
  const [drop, setDrop] = useState(0.3);
  const [confirm, setConfirm] = useState(false);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; text: string } | null>(null);
  const types = FAULT_TYPES.filter((t) => !t.simOnly && t.id !== "HEAL");

  async function inject() {
    setBusy(true);
    try {
      const r = await api<{ description: string; unreachable: string[] }>("/faults", {
        method: "POST", admin: true,
        body: { type, targets, durationMs: duration === "" || type === "RESTART" ? null : duration,
          delayMs: type === "DELAY" ? delayMs : null, dropProbability: type === "DROP" ? drop : null },
      });
      setResult({ ok: true, text: `Injected: ${r.description}${r.unreachable.length ? ` (could not reach ${r.unreachable.join(", ")})` : ""}` });
    } catch (e) {
      setResult({ ok: false, text: (e as Error).message });
    } finally {
      setBusy(false);
      setConfirm(false);
    }
  }

  return (
    <Card title="Inject a fault" sub="affects the real cluster">
      <div className="stack">
        <div className="grid cols-2">
          <div className="field"><label htmlFor="lf-type">Fault</label>
            <select id="lf-type" className="select" value={type} onChange={(e) => setType(e.target.value)}>
              {types.map((t) => <option key={t.id} value={t.id}>{t.label}</option>)}
            </select>
            <span className="help">{types.find((t) => t.id === type)?.help}</span></div>
          <div className="field"><label htmlFor="lf-dur">Auto-heal after (ms)</label>
            <input id="lf-dur" className="input num" type="number" min={100} step={500} placeholder="never" disabled={type === "RESTART"}
              value={duration} onChange={(e) => setDuration(e.target.value === "" ? "" : Number(e.target.value))} />
            <span className="help">Each node heals its part on its own when this expires.</span></div>
        </div>
        <div className="field"><label>Targets</label>
          <div className="row wrap" style={{ gap: 4 }}>
            {["leader", "follower", "random", ...nodes].map((t) => {
              const on = targets.includes(t);
              return <button key={t} className={`btn sm ${on ? "accent" : ""}`} aria-pressed={on}
                onClick={() => setTargets(on ? targets.filter((x) => x !== t) : [...targets, t])}>{t}</button>;
            })}
          </div></div>
        {type === "DELAY" && <div className="field"><label htmlFor="lf-delay">Extra latency (ms)</label>
          <input id="lf-delay" className="input num" type="number" min={1} max={60000} value={delayMs} onChange={(e) => setDelayMs(Number(e.target.value))} /></div>}
        {type === "DROP" && <div className="field"><label htmlFor="lf-drop">Drop probability</label>
          <input id="lf-drop" className="input num" type="number" min={0.01} max={1} step={0.05} value={drop} onChange={(e) => setDrop(Number(e.target.value))} /></div>}
        <div className="row">
          <button className="btn danger" disabled={disabled || targets.length === 0} onClick={() => setConfirm(true)}><Zap />Inject</button>
          {result && <span className="small" style={{ color: result.ok ? "var(--good-text)" : "var(--critical-text)" }}>{result.text}</span>}
        </div>
      </div>
      <ConfirmDialog open={confirm} title={`Inject ${type.toLowerCase().replace("_", " ")} on ${targets.join(", ")}?`} danger busy={busy}
        confirmLabel="Inject fault" onCancel={() => setConfirm(false)} onConfirm={inject}>
        This disrupts the running cluster. Client requests may fail while the fault is active.
        {duration === "" && type !== "RESTART" ? " With no duration it stays until you heal it." : ""}
      </ConfirmDialog>
    </Card>
  );
}

function ActiveFaults({ cluster, disabled }: { cluster: ReturnType<typeof usePoll<ClusterSummary>>; disabled: boolean }) {
  const [confirm, setConfirm] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  async function heal() {
    setBusy(true);
    setError(null);
    try {
      await api("/faults", { method: "DELETE", admin: true });
      cluster.refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
      setConfirm(false);
    }
  }
  return (
    <Card title="Active faults" sub="as reported by each node"
      actions={<button className="btn sm" disabled={disabled} onClick={() => setConfirm(true)}><Bandage />Heal all</button>} flush>
      <Async state={cluster}>
        {(c) => {
          const rows = c.nodes.flatMap((n) => (n.faults?.active ?? []).map((f) => ({ node: n.id, f })));
          const down = c.nodes.filter((n) => !n.reachable);
          return rows.length === 0 && down.length === 0 ? <Empty title="No active faults">Every node reports a clean network and disk.</Empty> : (
            <table className="table">
              <tbody>
                {rows.map((r, i) => <tr key={i}><td className="mono"><b>{r.node}</b></td><td className="small">{r.f}</td></tr>)}
                {down.map((n) => <tr key={n.id}><td className="mono"><b>{n.id}</b></td><td><Badge tone="critical">unreachable</Badge> <span className="small muted">{n.failure}</span></td></tr>)}
              </tbody>
            </table>
          );
        }}
      </Async>
      {error && <p className="small" style={{ color: "var(--critical-text)", padding: "0 16px" }}>{error}</p>}
      <ConfirmDialog open={confirm} title="Heal all faults?" confirmLabel="Heal all" busy={busy} onCancel={() => setConfirm(false)} onConfirm={heal}>
        Removes every injected network fault, restarts crashed Raft nodes and re-enables disk writes on all reachable nodes.
      </ConfirmDialog>
    </Card>
  );
}

function LiveExperiment({ disabled, clusterSize }: { disabled: boolean; clusterSize: number }) {
  const [scenario, setScenario] = useState<Scenario>(LIVE_DEFAULT);
  const [confirm, setConfirm] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const status = usePoll((s) => api<LiveStatus>("/faultlab/live-runs/current", { signal: s }), [], 1000);
  const st = status.data;
  const running = st?.state === "RUNNING";

  async function start() {
    setError(null);
    try {
      await api("/faultlab/live-runs", { method: "POST", admin: true, body: { scenario: { ...scenario, nodes: clusterSize } } });
      status.refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setConfirm(false);
    }
  }

  return (
    <Card title="Live experiment" sub="real clients and real faults, audited by the same checkers as the simulator"
      actions={<button className="btn accent" disabled={disabled || running} onClick={() => setConfirm(true)}>{running ? <Spinner /> : <Play />}Start experiment</button>}>
      {st && st.state !== "IDLE" && (
        <div className="callout" style={{ marginBottom: 16, alignItems: "center" }}>
          <div style={{ flex: 1 }}>
            <div className="row">
              <b>{st.scenario}</b>
              {running ? <Badge tone="accent"><Spinner />Running</Badge> : st.state === "COMPLETE" ? <VerdictBadge value="OK" /> : <Badge tone="critical">Failed</Badge>}
              <span className="muted xs">{fmtDuration(st.elapsedMs)} / {fmtDuration(st.durationMs)} · {st.opsCompleted} ops · {st.faultsInjected} faults</span>
            </div>
            {running && <div style={{ height: 4, background: "var(--surface-sunken)", borderRadius: 2, marginTop: 8 }}>
              <div style={{ width: `${Math.min(100, (100 * st.elapsedMs) / st.durationMs)}%`, height: "100%", background: "var(--accent)", borderRadius: 2 }} /></div>}
            {st.error && <div className="small" style={{ color: "var(--critical-text)" }}>{st.error}</div>}
          </div>
          {st.runId && <Link className="btn" to={`/faultlab/runs/${st.runId}`}>Open report</Link>}
        </div>
      )}
      {error && <div className="callout critical" style={{ marginBottom: 16 }}>{error}</div>}
      <ScenarioEditor value={scenario} onChange={setScenario} live />
      <ConfirmDialog open={confirm} title="Start a live experiment?" confirmLabel="Start" danger onCancel={() => setConfirm(false)} onConfirm={start}>
        For {fmtDuration(scenario.durationMs)} QuorumDB will send real requests to the cluster and inject {scenario.faults.length} real fault(s).
        All faults are healed at the end.
      </ConfirmDialog>
    </Card>
  );
}
