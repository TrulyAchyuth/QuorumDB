import { useMemo, useState } from "react";
import { Send } from "lucide-react";
import { api, rawRequest } from "../api/client";
import type { ClusterSummary, Operation } from "../api/types";
import { usePoll } from "../hooks/usePoll";
import { Async, Badge, Card, Empty, Segmented, Spinner } from "../components/ui";
import { fmtClock, fmtInt, fmtMs } from "../lib/format";

const STATUS_TONE: Record<string, "good" | "warning" | "critical" | "neutral" | "accent"> = {
  OK: "good", NOT_FOUND: "neutral", CONFLICT: "warning", FORWARDED: "accent", NOT_LEADER: "warning", BUSY: "warning",
  UNKNOWN: "critical", TIMEOUT: "critical", LEADER_UNREACHABLE: "critical",
};

export function Operations() {
  const cluster = usePoll((s) => api<ClusterSummary>("/cluster", { signal: s }), [], 5000);
  const nodes = cluster.data?.nodes.map((n) => n.id) ?? [];
  const [type, setType] = useState<"ALL" | "PUT" | "GET" | "DELETE">("ALL");
  const ops = usePoll(async (signal) => {
    const all = await Promise.all(nodes.map((n) =>
      api<Operation[]>(`/operations?limit=300${type === "ALL" ? "" : `&type=${type}`}`, { node: n, signal }).catch(() => [] as Operation[])));
    return all.flat().sort((a, b) => b.timeMs - a.timeMs).slice(0, 300);
  }, [nodes.join(","), type], 1500);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Operations</h1>
          <p>Every client request handled by any node, newest first. A follower that forwards a write to the leader logs it as <em>forwarded</em>; the leader logs the commit with its log index.</p>
        </div>
      </div>
      <Console nodes={nodes} onDone={ops.refresh} />
      <Card title="Recent operations" sub="last 300 across all nodes" className="mt"
        actions={<Segmented label="Operation type" value={type} onChange={setType}
          items={[{ id: "ALL", label: "All" }, { id: "PUT", label: "PUT" }, { id: "GET", label: "GET" }, { id: "DELETE", label: "DELETE" }]} />} flush>
        <Async state={ops} rows={6}>
          {(list) => list.length === 0 ? <Empty title="No operations yet">Use the console above or run a workload.</Empty> : (
            <div className="table-wrap" style={{ maxHeight: 520, overflowY: "auto" }}>
              <table className="table">
                <thead>
                  <tr><th>Time</th><th>Op</th><th>Key</th><th>Status</th><th className="num">Latency</th><th className="num">Log index</th>
                    <th className="num">Term</th><th>Committed</th><th>Read</th><th>Served by</th></tr>
                </thead>
                <tbody>
                  {list.map((o) => (
                    <tr key={`${o.servedBy}-${o.id}-${o.timeMs}`}>
                      <td className="mono xs nowrap">{fmtClock(o.timeMs)}</td>
                      <td><b>{o.type}</b></td>
                      <td className="mono xs" style={{ maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis" }}>{o.key}</td>
                      <td><Badge tone={STATUS_TONE[o.status] ?? "neutral"}>{o.status.replace(/_/g, " ").toLowerCase()}</Badge></td>
                      <td className="num">{fmtMs(o.latencyMicros / 1000)}</td>
                      <td className="num">{o.logIndex ? fmtInt(o.logIndex) : "–"}</td>
                      <td className="num">{o.term || "–"}</td>
                      <td>{o.type === "GET" || o.type === "SCAN" ? <span className="muted">–</span> : o.committed ? "yes" : <span className="muted">no</span>}</td>
                      <td className="xs">{o.consistency ? `${o.consistency.toLowerCase()} @ ${o.readIndex}` : <span className="muted">–</span>}</td>
                      <td className="mono xs">{o.servedBy}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Async>
      </Card>
    </>
  );
}

function Console({ nodes, onDone }: { nodes: string[]; onDone: () => void }) {
  const [method, setMethod] = useState<"PUT" | "GET" | "DELETE">("PUT");
  const [key, setKey] = useState("user:42");
  const [value, setValue] = useState("Ada Lovelace");
  const [consistency, setConsistency] = useState("linearizable");
  const [node, setNode] = useState<string>("");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{ status: number; headers: Record<string, string>; body: string; ms: number } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const pretty = useMemo(() => {
    if (!result) return "";
    try {
      return JSON.stringify(JSON.parse(result.body), null, 2);
    } catch {
      return result.body;
    }
  }, [result]);

  async function send() {
    if (!key.trim()) return;
    setBusy(true);
    setError(null);
    try {
      const path = `/kv/${encodeURIComponent(key)}${method === "GET" ? `?consistency=${consistency}` : ""}`;
      const r = await rawRequest(method, path, method === "PUT" ? JSON.stringify({ value }) : undefined, node || undefined);
      setResult(r);
      onDone();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <Card title="Request console" sub="sends a real request through the REST API">
      <form className="row wrap" style={{ alignItems: "flex-end", gap: 12 }} onSubmit={(e) => { e.preventDefault(); send(); }}>
        <div className="field">
          <label>Method</label>
          <Segmented label="Method" value={method} onChange={setMethod}
            items={[{ id: "PUT", label: "PUT" }, { id: "GET", label: "GET" }, { id: "DELETE", label: "DELETE" }]} />
        </div>
        <div className="field" style={{ flex: "1 1 180px" }}>
          <label htmlFor="op-key">Key</label>
          <input id="op-key" className="input" value={key} onChange={(e) => setKey(e.target.value)} maxLength={256} required />
        </div>
        {method === "PUT" && (
          <div className="field" style={{ flex: "2 1 240px" }}>
            <label htmlFor="op-value">Value</label>
            <input id="op-value" className="input" value={value} onChange={(e) => setValue(e.target.value)} />
          </div>
        )}
        {method === "GET" && (
          <div className="field">
            <label htmlFor="op-cons">Consistency</label>
            <select id="op-cons" className="select" value={consistency} onChange={(e) => setConsistency(e.target.value)}>
              <option value="linearizable">linearizable (ReadIndex)</option>
              <option value="leader">leader (unconfirmed)</option>
              <option value="stale">stale (local node)</option>
            </select>
          </div>
        )}
        <div className="field">
          <label htmlFor="op-node">Send to</label>
          <select id="op-node" className="select" value={node} onChange={(e) => setNode(e.target.value)}>
            <option value="">any node</option>
            {nodes.map((n) => <option key={n} value={n}>{n}</option>)}
          </select>
        </div>
        <button className="btn primary" type="submit" disabled={busy}>{busy ? <Spinner /> : <Send />}Send</button>
      </form>
      {error && <p className="small" style={{ color: "var(--critical-text)" }}>{error}</p>}
      {result && (
        <div className="mt">
          <div className="row small" style={{ marginBottom: 6 }}>
            <Badge tone={result.status < 300 ? "good" : result.status === 404 ? "neutral" : "critical"}>HTTP {result.status}</Badge>
            <span className="muted">{fmtMs(result.ms)}</span>
            {Object.entries(result.headers).map(([k, v]) => <span key={k} className="mono xs muted">{k}: {v}</span>)}
          </div>
          <pre className="textarea" style={{ margin: 0, maxHeight: 200, overflow: "auto", whiteSpace: "pre-wrap" }}>{pretty}</pre>
        </div>
      )}
    </Card>
  );
}
