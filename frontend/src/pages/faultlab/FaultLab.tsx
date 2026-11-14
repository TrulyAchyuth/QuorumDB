import { useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { FlaskConical, History, Play, Radio } from "lucide-react";
import { api } from "../../api/client";
import type { RunDetail, RunSummary, Scenario } from "../../api/types";
import { usePoll } from "../../hooks/usePoll";
import { Async, Card, Empty, Spinner, Tabs, VerdictBadge } from "../../components/ui";
import { ScenarioEditor } from "./ScenarioEditor";
import { LiveTab } from "./LiveTab";
import { ago, fmtInt, fmtPct } from "../../lib/format";

type Tab = "simulate" | "runs" | "live";

export function FaultLab() {
  const [params, setParams] = useSearchParams();
  const tab = (params.get("tab") as Tab) ?? "simulate";
  return (
    <>
      <div className="page-head">
        <div>
          <h1>FaultLab</h1>
          <p>Inject failures, then let an independent checker decide whether Raft kept its promises. Simulated runs are deterministic: the same scenario and seed always produce the identical event trace. Live runs use the real cluster.</p>
        </div>
      </div>
      <Tabs value={tab} onChange={(t) => setParams({ tab: t })} items={[
        { id: "simulate", label: "Simulate", icon: <FlaskConical /> },
        { id: "runs", label: "Runs & comparison", icon: <History /> },
        { id: "live", label: "Live cluster", icon: <Radio /> },
      ]} />
      {tab === "simulate" && <SimulateTab />}
      {tab === "runs" && <RunsTab />}
      {tab === "live" && <LiveTab />}
    </>
  );
}

function SimulateTab() {
  const presets = usePoll((s) => api<Record<string, Scenario>>("/faultlab/scenarios", { signal: s }), [], 0);
  const [selected, setSelected] = useState<string | null>(null);
  const [scenario, setScenario] = useState<Scenario | null>(null);
  const [recordMessages, setRecordMessages] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  const choose = (key: string, s: Scenario) => {
    setSelected(key);
    setScenario(structuredClone(s));
    setError(null);
  };

  async function run() {
    if (!scenario) return;
    setBusy(true);
    setError(null);
    try {
      const d = await api<RunDetail>("/faultlab/runs", { method: "POST", body: { scenario, recordMessages } });
      navigate(`/faultlab/runs/${d.summary.id}`);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="grid" style={{ gridTemplateColumns: "minmax(240px, 300px) minmax(0, 1fr)" }}>
      <Card title="Scenarios" sub="built-in" flush>
        <Async state={presets} rows={8}>
          {(p) => (
            <div role="listbox" aria-label="Scenarios">
              {Object.entries(p).map(([key, s]) => (
                <button key={key} role="option" aria-selected={selected === key} onClick={() => choose(key, s)}
                  style={{ all: "unset", display: "block", width: "100%", boxSizing: "border-box", padding: "10px 16px", cursor: "pointer",
                    borderBottom: "1px solid var(--border)", background: selected === key ? "var(--selected)" : undefined }}>
                  <div style={{ fontWeight: 600, fontSize: "var(--fs-sm)" }}>{s.name}</div>
                  <div className="xs muted">{s.nodes} nodes · {s.faults.length} fault{s.faults.length === 1 ? "" : "s"} · {s.durationMs / 1000}s</div>
                </button>
              ))}
            </div>
          )}
        </Async>
      </Card>
      {scenario ? (
        <Card title={scenario.name} sub="edit, then run in the deterministic simulator"
          actions={
            <>
              <label className="row small secondary"><input type="checkbox" checked={recordMessages} onChange={(e) => setRecordMessages(e.target.checked)} />record every message</label>
              <button className="btn accent" onClick={run} disabled={busy}>{busy ? <Spinner /> : <Play />}Run simulation</button>
            </>
          }>
          {scenario.description && <p className="secondary small" style={{ marginTop: 0, marginBottom: 20, maxWidth: 820 }}>{scenario.description}</p>}
          {error && <div className="callout critical" style={{ marginBottom: 16 }}>{error}</div>}
          <ScenarioEditor value={scenario} onChange={setScenario} />
        </Card>
      ) : (
        <Card title="No scenario selected">
          <Empty title="Pick a scenario to start" icon={<FlaskConical />}>
            Each preset exercises one failure mode. You can edit its nodes, workload and fault schedule before running it.
          </Empty>
        </Card>
      )}
    </div>
  );
}

function RunsTab() {
  const runs = usePoll((s) => api<RunSummary[]>("/faultlab/runs", { signal: s }), [], 3000);
  const [pick, setPick] = useState<string[]>([]);
  const navigate = useNavigate();
  const toggle = (id: string) => setPick((p) => (p.includes(id) ? p.filter((x) => x !== id) : [...p.slice(-1), id]));

  return (
    <Card title="Recent runs" sub="kept in memory on the node that ran them, oldest dropped first when its budget is full; export a run to keep it"
      actions={<button className="btn primary" disabled={pick.length !== 2} onClick={() => navigate(`/faultlab/compare?a=${pick[0]}&b=${pick[1]}`)}>
        Compare {pick.length}/2</button>} flush>
      <Async state={runs} rows={5}>
        {(list) => list.length === 0 ? <Empty title="No runs yet">Run a scenario in the Simulate tab, or a live experiment.</Empty> : (
          <div className="table-wrap">
            <table className="table">
              <thead><tr><th style={{ width: 32 }} /><th>Run</th><th>Mode</th><th>Outcome</th><th>Safety</th><th>Client history</th>
                <th className="num">Nodes</th><th className="num">Seed</th><th className="num">Ops</th><th className="num">Availability</th><th className="num">Elections</th><th>When</th></tr></thead>
              <tbody>
                {list.map((r) => (
                  <tr key={r.id} className={`clickable ${pick.includes(r.id) ? "selected" : ""}`} onClick={() => navigate(`/faultlab/runs/${r.id}`)}>
                    <td onClick={(e) => e.stopPropagation()}>
                      <input type="checkbox" aria-label={`Select ${r.id} for comparison`} checked={pick.includes(r.id)} onChange={() => toggle(r.id)} />
                    </td>
                    <td><b>{r.name}</b><div className="mono xs muted">{r.id}{r.replayOf ? ` · replay of ${r.replayOf}` : ""}</div></td>
                    <td>{r.mode === "LIVE" ? <span className="badge accent">Live</span> : <span className="badge">Simulated</span>}</td>
                    <td><VerdictBadge value={r.headline} /></td>
                    <td><VerdictBadge value={r.safety} /></td>
                    <td><VerdictBadge value={r.linearizability} /></td>
                    <td className="num">{r.nodes}</td>
                    <td className="num">{r.mode === "LIVE" ? "–" : r.seed}</td>
                    <td className="num">{fmtInt(r.opsTotal)}</td>
                    <td className="num">{fmtPct(r.availability)}</td>
                    <td className="num">{r.elections}</td>
                    <td className="xs muted nowrap">{ago(r.createdAtMs)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Async>
    </Card>
  );
}
