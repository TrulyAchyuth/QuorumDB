import { useState } from "react";
import { api } from "../api/client";
import type { ClusterSummary, NodeView } from "../api/types";
import { usePoll } from "../hooks/usePoll";
import { Async, Badge, Card, HealthBadge, RoleBadge } from "../components/ui";
import { Topology } from "../components/Topology";
import { fmtBytes, fmtDuration, fmtInt, fmtMs } from "../lib/format";

export function ClusterExplorer() {
  const cluster = usePoll((s) => api<ClusterSummary>("/cluster", { signal: s }), []);
  const [selected, setSelected] = useState<string | null>(null);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Cluster explorer</h1>
          <p>Topology as seen from the leader: each link shows the follower's replication lag in log entries. Select a node for its full state and transport statistics.</p>
        </div>
      </div>
      <Async state={cluster} rows={6}>
        {(c) => {
          const current = c.nodes.find((n) => n.id === (selected ?? c.leaderId ?? c.nodes[0]?.id)) ?? c.nodes[0];
          return (
            <div className="grid cols-2">
              <Card title="Topology" sub={`${c.size} nodes · quorum ${c.quorum}`}>
                <Topology nodes={c.nodes} selected={current?.id ?? null} onSelect={setSelected} />
                <div className="chart-legend" style={{ justifyContent: "center", marginTop: 4 }}>
                  <span><i style={{ background: "var(--accent)" }} />replicating</span>
                  <span><i style={{ background: "var(--warning)" }} />lag &gt; 50</span>
                  <span><i style={{ background: "var(--critical)" }} />no contact</span>
                </div>
              </Card>
              {current && <NodeDetail node={current} leader={c.nodes.find((n) => n.id === c.leaderId) ?? null} />}
            </div>
          );
        }}
      </Async>
    </>
  );
}

function NodeDetail({ node, leader }: { node: NodeView; leader: NodeView | null }) {
  const progress = leader && leader.id !== node.id ? leader.followers[node.id] : null;
  const links = Object.values(node.links);
  return (
    <>
      <Card title={<span className="mono">Node {node.id}</span>} actions={<><HealthBadge health={node.health} />{node.reachable && <RoleBadge role={node.role} />}</>}>
        {!node.reachable ? (
          <p className="secondary small">This node did not answer: {node.failure ?? "no response"}. Its last known state is unavailable; the rest of the cluster continues if a quorum remains.</p>
        ) : (
          <dl className="kv">
            <dt>Current term</dt><dd>{node.term}</dd>
            <dt>Voted for</dt><dd className="mono">{node.votedFor ?? "–"}</dd>
            <dt>Known leader</dt><dd className="mono">{node.leaderId ?? "none"}</dd>
            <dt>Last log index</dt><dd>{fmtInt(node.lastLogIndex)} <span className="muted">(term {node.lastLogTerm})</span></dd>
            <dt>Commit index</dt><dd>{fmtInt(node.commitIndex)}</dd>
            <dt>Last applied</dt><dd>{fmtInt(node.lastApplied)}</dd>
            <dt>Snapshot index</dt><dd>{fmtInt(node.snapshotIndex)}</dd>
            {progress && <><dt>Match index (leader's view)</dt><dd>{fmtInt(progress.matchIndex)}</dd></>}
            {progress && <><dt>Replication lag</dt><dd>{fmtInt(progress.lag)} entries</dd></>}
            {progress && <><dt>Last ack to leader</dt><dd>{fmtMs(progress.lastAckAgoMs)} ago</dd></>}
            {node.role === "LEADER" && <><dt>Pending proposals</dt><dd>{fmtInt(node.pendingProposals)}</dd></>}
            <dt>Keys</dt><dd>{fmtInt(node.storage?.keys)}</dd>
            <dt>Disk usage</dt><dd>{fmtBytes(node.storage?.diskBytes)}</dd>
            <dt>fsync</dt><dd>{node.storage?.fsync ? "every WAL write" : "off (benchmark mode)"}</dd>
            <dt>Uptime</dt><dd>{fmtDuration(node.uptimeMs)}</dd>
            <dt>Addresses</dt><dd className="mono xs">{node.httpUrl}<br />raft {node.raftAddress}</dd>
          </dl>
        )}
        {node.failure && node.reachable && <p className="small" style={{ color: "var(--critical-text)" }}>Fail-stopped: {node.failure}</p>}
        {node.faults && node.faults.active.length > 0 && (
          <div className="stack mt">
            <span className="xs muted">Active injected faults</span>
            {node.faults.active.map((f) => <Badge key={f} tone="critical">{f}</Badge>)}
          </div>
        )}
      </Card>
      <Card title="Raft links" sub={`gRPC transport from ${node.id}`} className="span-2" flush>
        {links.length === 0 ? <p className="secondary small" style={{ padding: 16 }}>Single-node cluster: no peers.</p> : (
          <div className="table-wrap">
            <table className="table">
              <thead><tr><th>Peer</th><th>Address</th><th>Status</th><th className="num">Messages sent</th><th className="num">Received</th><th className="num">Dropped</th><th className="num">In flight</th><th className="num">Last reply</th></tr></thead>
              <tbody>
                {links.map((l) => {
                  const ok = !l.faultBlocked && l.lastSuccessAgoMs >= 0 && l.lastSuccessAgoMs < 1000;
                  return (
                    <tr key={l.peer}>
                      <td className="mono"><b>{l.peer}</b></td>
                      <td className="mono xs">{l.address}</td>
                      <td>{l.faultBlocked ? <Badge tone="critical">Blocked</Badge> : ok ? <Badge tone="good">Connected</Badge> : <Badge tone="warning">{l.lastError ?? "Silent"}</Badge>}</td>
                      <td className="num">{fmtInt(l.sent)}</td>
                      <td className="num">{fmtInt(l.received)}</td>
                      <td className="num">{fmtInt(l.dropped)}</td>
                      <td className="num">{l.inFlight}</td>
                      <td className="num">{l.lastSuccessAgoMs < 0 ? "never" : fmtMs(l.lastSuccessAgoMs)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </>
  );
}
