import { useEffect, useState } from "react";
import { Braces, Plus, Trash2 } from "lucide-react";
import type { FaultStep, FaultType, Scenario } from "../../api/types";

export const FAULT_TYPES: { id: FaultType; label: string; simOnly?: boolean; help: string }[] = [
  { id: "CRASH", label: "Crash", help: "Kill the process; restarted when the fault ends" },
  { id: "RESTART", label: "Restart", help: "Kill and immediately restart" },
  { id: "ISOLATE", label: "Isolate", help: "Cut the node off from all peers" },
  { id: "PARTITION", label: "Partition", help: "Split the targets from the rest of the cluster" },
  { id: "DELAY", label: "Delay", help: "Add latency to the node's messages" },
  { id: "DROP", label: "Drop", help: "Drop a fraction of the node's messages" },
  { id: "STORAGE_FAILURE", label: "Disk failure", help: "Disk writes fail; the node fail-stops" },
  { id: "DISK_LOSS", label: "Disk loss", simOnly: true, help: "Erase the node's disk (breaks Raft's assumptions)" },
  { id: "HEAL", label: "Heal", help: "Remove all network faults" },
];

const SELECTORS = ["leader", "follower", "random"];

/** Structured editor for the parts people change most, with a raw JSON mode for everything else. */
export function ScenarioEditor(props: { value: Scenario; onChange: (s: Scenario) => void; live?: boolean }) {
  const s = props.value;
  const [json, setJson] = useState(false);
  const [text, setText] = useState("");
  const [jsonError, setJsonError] = useState<string | null>(null);
  useEffect(() => {
    if (!json) setText(JSON.stringify(s, null, 2));
  }, [s, json]);

  const nodeIds = Array.from({ length: s.nodes }, (_, i) => `n${i + 1}`);
  const set = (patch: Partial<Scenario>) => props.onChange({ ...s, ...patch });
  const setFault = (i: number, patch: Partial<FaultStep>) =>
    set({ faults: s.faults.map((f, j) => (j === i ? { ...f, ...patch } : f)) });

  if (json) {
    return (
      <div className="stack">
        <div className="row"><span className="small secondary">Raw scenario JSON. Omitted fields take defaults; the server validates everything.</span>
          <span className="spacer" style={{ flex: 1 }} />
          <button className="btn sm" onClick={() => {
            try {
              props.onChange(JSON.parse(text));
              setJsonError(null);
              setJson(false);
            } catch (e) {
              setJsonError((e as Error).message);
            }
          }}>Apply and close</button>
        </div>
        <textarea className="textarea" rows={22} value={text} onChange={(e) => setText(e.target.value)} spellCheck={false} aria-label="Scenario JSON" />
        {jsonError && <span className="small" style={{ color: "var(--critical-text)" }}>Invalid JSON: {jsonError}</span>}
      </div>
    );
  }

  return (
    <div className="stack gap-lg">
      <div className="grid cols-4">
        <div className="field span-2"><label htmlFor="sc-name">Name</label>
          <input id="sc-name" className="input" value={s.name} onChange={(e) => set({ name: e.target.value })} /></div>
        {!props.live && (
          <div className="field"><label htmlFor="sc-nodes">Nodes</label>
            <select id="sc-nodes" className="select" value={s.nodes} onChange={(e) => set({ nodes: Number(e.target.value) })}>
              {[1, 3, 5, 7].map((n) => <option key={n} value={n}>{n}</option>)}
            </select></div>
        )}
        {!props.live && (
          <div className="field"><label htmlFor="sc-seed">Seed</label>
            <input id="sc-seed" className="input num" type="number" value={s.seed} onChange={(e) => set({ seed: Number(e.target.value) })} /></div>
        )}
        <div className="field"><label htmlFor="sc-dur">Duration (ms)</label>
          <input id="sc-dur" className="input num" type="number" min={100} step={500} value={s.durationMs} onChange={(e) => set({ durationMs: Number(e.target.value) })} /></div>
        <div className="field"><label htmlFor="sc-clients">Clients</label>
          <input id="sc-clients" className="input num" type="number" min={0} max={32} value={s.workload.clients}
            onChange={(e) => set({ workload: { ...s.workload, clients: Number(e.target.value) } })} /></div>
        <div className="field"><label htmlFor="sc-read">Read mode</label>
          <select id="sc-read" className="select" value={s.workload.readMode}
            onChange={(e) => set({ workload: { ...s.workload, readMode: e.target.value as Scenario["workload"]["readMode"] } })}>
            <option value="LINEARIZABLE">Linearizable (ReadIndex)</option>
            <option value="LEADER_LOCAL">Leader-local (unconfirmed)</option>
            <option value="STALE">Stale (any node)</option>
          </select></div>
        {!props.live && (
          <div className="field"><label>Raft extensions</label>
            <div className="row wrap" style={{ minHeight: 32, columnGap: 14, rowGap: 2 }}>
              <label className="row small"><input type="checkbox" checked={s.cluster.preVote} onChange={(e) => set({ cluster: { ...s.cluster, preVote: e.target.checked } })} />PreVote</label>
              <label className="row small"><input type="checkbox" checked={s.cluster.checkQuorum} onChange={(e) => set({ cluster: { ...s.cluster, checkQuorum: e.target.checked } })} />CheckQuorum</label>
            </div></div>
        )}
      </div>

      <div className="stack">
        <div className="row">
          <strong className="small">Fault schedule</strong>
          <span className="muted xs">{s.faults.length} step{s.faults.length === 1 ? "" : "s"} · times relative to run start</span>
          <span style={{ flex: 1 }} />
          <button className="btn sm" onClick={() => setJson(true)}><Braces />Edit JSON</button>
          <button className="btn sm" onClick={() => set({ faults: [...s.faults, { type: "CRASH", targets: ["leader"], start: { atMs: Math.round(s.durationMs / 4) }, durationMs: 2000 }] })}>
            <Plus />Add fault</button>
        </div>
        {s.faults.length === 0 && <div className="callout small">No faults: the run measures the healthy baseline.</div>}
        {s.faults.map((f, i) => {
          const conditional = f.start.atMs == null;
          return (
            <div key={i} className="card" style={{ padding: 12 }}>
              <div className="fault-row">
                <div className="field"><label>Type</label>
                  <select className="select" value={f.type} onChange={(e) => {
                    const type = e.target.value as FaultType;
                    setFault(i, { type, delayMs: type === "DELAY" ? (f.delayMs ?? 50) : null, dropProbability: type === "DROP" ? (f.dropProbability ?? 0.3) : null,
                      durationMs: type === "RESTART" || type === "HEAL" ? null : f.durationMs });
                  }}>
                    {FAULT_TYPES.filter((t) => !(props.live && t.simOnly)).map((t) => <option key={t.id} value={t.id}>{t.label}</option>)}
                  </select></div>
                <div className="field"><label>Targets</label>
                  {f.type === "HEAL" ? <span className="muted small" style={{ lineHeight: "32px" }}>whole cluster</span> : (
                    <div className="row wrap" style={{ gap: 4, minHeight: 32 }}>
                      {[...SELECTORS, ...nodeIds].map((t) => {
                        const on = f.targets?.includes(t) ?? false;
                        return (
                          <button key={t} type="button" className={`btn sm ${on ? "accent" : ""}`} aria-pressed={on}
                            onClick={() => setFault(i, { targets: on ? (f.targets ?? []).filter((x) => x !== t) : [...(f.targets ?? []), t] })}>{t}</button>
                        );
                      })}
                    </div>
                  )}</div>
                <div className="field"><label>Start (ms)</label>
                  {conditional ? <span className="small secondary" title={JSON.stringify(f.start)}>when {String(f.start.when).toLowerCase().replace(/_/g, " ")} {f.start.value ?? ""}</span> :
                    <input className="input num" type="number" min={0} step={250} value={f.start.atMs ?? 0} onChange={(e) => setFault(i, { start: { atMs: Number(e.target.value) } })} />}</div>
                <div className="field"><label>Duration (ms)</label>
                  <input className="input num" type="number" min={0} step={250} placeholder="until end" disabled={f.type === "RESTART" || f.type === "HEAL"}
                    value={f.durationMs ?? ""} onChange={(e) => setFault(i, { durationMs: e.target.value === "" ? null : Number(e.target.value) })} /></div>
                <div className="row" style={{ justifyContent: "flex-end", gap: 6 }}>
                  {f.type === "DELAY" && <input className="input num" style={{ width: 80 }} type="number" min={1} aria-label="Delay ms" title="Delay ms"
                    value={f.delayMs ?? 50} onChange={(e) => setFault(i, { delayMs: Number(e.target.value) })} />}
                  {f.type === "DROP" && <input className="input num" style={{ width: 80 }} type="number" min={0.01} max={1} step={0.05} aria-label="Drop probability" title="Drop probability"
                    value={f.dropProbability ?? 0.3} onChange={(e) => setFault(i, { dropProbability: Number(e.target.value) })} />}
                  <button className="btn icon ghost" aria-label={`Remove fault ${i + 1}`} onClick={() => set({ faults: s.faults.filter((_, j) => j !== i) })}><Trash2 /></button>
                </div>
              </div>
              <div className="xs muted" style={{ marginTop: 6 }}>{FAULT_TYPES.find((t) => t.id === f.type)?.help}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
