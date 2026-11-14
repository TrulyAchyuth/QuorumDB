import { useState } from "react";
import { Check, Eye, EyeOff } from "lucide-react";
import { api } from "../api/client";
import type { Config } from "../api/types";
import { usePoll } from "../hooks/usePoll";
import { useSettings } from "../hooks/useSettings";
import { useTheme, type Theme } from "../hooks/useTheme";
import { updateSettings } from "../lib/settings";
import { Async, Badge, Card, Segmented } from "../components/ui";

export function Settings() {
  const settings = useSettings();
  const [theme, setTheme] = useTheme();
  const config = usePoll((s) => api<Config>("/config", { signal: s }), [settings.pinnedNode], 0);
  const [token, setToken] = useState(settings.adminToken);
  const [show, setShow] = useState(false);
  const [saved, setSaved] = useState(false);

  return (
    <>
      <div className="page-head"><div><h1>Settings</h1><p>Dashboard preferences are stored in this browser only. Cluster configuration is read from the nodes and cannot be changed here.</p></div></div>
      <div className="grid cols-2">
        <Card title="Dashboard">
          <div className="stack gap-lg">
            <div className="field"><label>Theme</label>
              <Segmented<Theme> label="Theme" value={theme} onChange={setTheme} items={[{ id: "light", label: "Light" }, { id: "dark", label: "Dark" }, { id: "system", label: "System" }]} /></div>
            <div className="field"><label htmlFor="st-node">API node</label>
              <select id="st-node" className="select" value={settings.pinnedNode ?? ""} onChange={(e) => updateSettings({ pinnedNode: e.target.value || null })}>
                <option value="">Automatic (first reachable node)</option>
                {(config.data?.members ?? []).map((m) => <option key={m.id} value={m.id}>{m.id} · {m.httpBaseUrl}</option>)}
              </select>
              <span className="help">FaultLab runs are stored on the node that executed them; pin a node to keep seeing the same run history.</span></div>
            <div className="field"><label htmlFor="st-poll">Refresh interval</label>
              <select id="st-poll" className="select" value={settings.pollMs} onChange={(e) => updateSettings({ pollMs: Number(e.target.value) })}>
                {[500, 1000, 2000, 5000].map((ms) => <option key={ms} value={ms}>{ms / 1000} s</option>)}
              </select></div>
          </div>
        </Card>
        <Card title="Admin token" sub="needed for live fault injection">
          <form className="stack" onSubmit={(e) => { e.preventDefault(); updateSettings({ adminToken: token.trim() }); setSaved(true); setTimeout(() => setSaved(false), 1500); }}>
            <div className="field"><label htmlFor="st-token">Token</label>
              <div className="row">
                <input id="st-token" className="input mono" style={{ flex: 1 }} type={show ? "text" : "password"} autoComplete="off" value={token}
                  onChange={(e) => setToken(e.target.value)} placeholder="paste the value of quorumdb.admin-token" />
                <button type="button" className="btn icon" aria-label={show ? "Hide token" : "Show token"} onClick={() => setShow(!show)}>{show ? <EyeOff /> : <Eye />}</button>
              </div>
              <span className="help">Kept in session storage for this tab only and sent only as an Authorization header to this dashboard's API. Local clusters: <code>cat .run/admin-token</code>.</span></div>
            <div className="row">
              <button className="btn primary" type="submit">{saved ? <Check /> : null}{saved ? "Saved" : "Save"}</button>
              {settings.adminToken && <button type="button" className="btn ghost" onClick={() => { setToken(""); updateSettings({ adminToken: "" }); }}>Forget</button>}
              {config.data && (config.data.faultInjectionEnabled ? <Badge tone="good">enabled on cluster</Badge> : <Badge tone="warning">disabled on cluster</Badge>)}
            </div>
          </form>
        </Card>
      </div>
      <Card title="Cluster configuration" sub={config.data ? `reported by ${config.data.nodeId}` : undefined} className="mt">
        <Async state={config}>
          {(c) => (
            <div className="grid cols-2 gap-lg">
              <dl className="kv">
                <dt>Cluster id</dt><dd className="mono">{c.clusterId}</dd>
                <dt>Election timeout</dt><dd>{c.electionTimeoutMinMs}–{c.electionTimeoutMaxMs} ms</dd>
                <dt>Heartbeat interval</dt><dd>{c.heartbeatIntervalMs} ms</dd>
                <dt>PreVote / CheckQuorum</dt><dd>{c.preVote ? "on" : "off"} / {c.checkQuorum ? "on" : "off"}</dd>
                <dt>Snapshot every</dt><dd>{c.snapshotThreshold.toLocaleString()} applied entries</dd>
                <dt>fsync</dt><dd>{c.fsync ? "every WAL write" : "off"}</dd>
                <dt>Request timeout</dt><dd>{c.requestTimeoutMs} ms</dd>
                <dt>Max key / value</dt><dd>{c.maxKeyBytes} B / {(c.maxValueBytes / 1024).toFixed(0)} KiB</dd>
                <dt>Event buffer</dt><dd>{c.eventBufferSize.toLocaleString()} events per node</dd>
                <dt>Metrics</dt><dd className="mono xs">/api/v1/metrics/prometheus</dd>
              </dl>
              <table className="table">
                <thead><tr><th>Member</th><th>HTTP</th><th>Raft (gRPC)</th></tr></thead>
                <tbody>{c.members.map((m) => <tr key={m.id}><td className="mono"><b>{m.id}</b></td><td className="mono xs">{m.httpBaseUrl}</td><td className="mono xs">{m.raftAddress}</td></tr>)}</tbody>
              </table>
            </div>
          )}
        </Async>
      </Card>
    </>
  );
}
