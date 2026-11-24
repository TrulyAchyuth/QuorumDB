import { Link } from "react-router-dom";
import { ArrowRight } from "lucide-react";
import { api } from "../api/client";
import type { ClusterMetrics, ClusterSummary, RaftEvent } from "../api/types";
import { usePoll } from "../hooks/usePoll";
import { Async, Card, Empty, HealthBadge, RoleBadge, Stat } from "../components/ui";
import { TimeSeriesChart } from "../components/charts";
import { EventList } from "../components/EventList";
import { QuorumRing } from "../components/QuorumRing";
import { KEY_EVENTS } from "../lib/events";
import { fmtClock, fmtInt, fmtMs, fmtNum } from "../lib/format";

export function Overview() {
  const cluster = usePoll((s) => api<ClusterSummary>("/cluster", { signal: s }), []);
  const metrics = usePoll((s) => api<ClusterMetrics>("/cluster/metrics?window=10", { signal: s }), [], 2000);
  const events = usePoll((s) => api<RaftEvent[]>(`/cluster/events?limit=40&types=${[...KEY_EVENTS].join(",")}`, { signal: s }), [], 2500);

  const c = cluster.data;
  const m = metrics.data;
  const leaderMetrics = c?.leaderId && m ? m.nodes[c.leaderId] : null;
  const last10 = m?.perSecond.slice(-10) ?? [];
  const writeRate = last10.reduce((a, p) => a + p.writes, 0) / Math.max(1, last10.length);
  const readRate = last10.reduce((a, p) => a + p.reads, 0) / Math.max(1, last10.length);
  const series = (m?.perSecond ?? []).map((p) => ({ t: p.second, writes: p.writes, reads: p.reads }));
  const keyEvents = (events.data ?? []).filter((e) => KEY_EVENTS.has(e.type)).slice(-14).reverse();

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Cluster overview</h1>
          <p>Live state of every node, reported by the nodes themselves. Throughput and latency are measured on real requests over the last 10 seconds.</p>
        </div>
      </div>

      {c && (
        <section className="hero mt-lg">
          <Card title="Quorum" sub="live, from each node">
            <div style={{ padding: "var(--s3) var(--s4) var(--s4)" }}>
              <QuorumRing nodes={c.nodes} leaderId={c.leaderId} live={c.health !== "UNAVAILABLE"} />
            </div>
          </Card>
          <div className="card hero-copy">
            <h2>{c.leaderId ? `${c.leaderId} leads term ${c.term}` : "No leader. Writes are paused until a majority can vote."}</h2>
            <p>
              {c.healthyNodes} of {c.size} nodes are serving. A write is acknowledged once {c.quorum} nodes have stored it.
            </p>
            <div className="facts">
              <div className="fact"><b className="num">{c.quorum}</b><span>quorum size</span></div>
              <div className="fact"><b className="num">{fmtInt(c.commitIndex)}</b><span>commit index</span></div>
              <div className="fact"><b className="num">{fmtInt(c.maxReplicationLag)}</b><span>max lag</span></div>
            </div>
          </div>
        </section>
      )}

      <Async state={cluster} rows={2}>
        {(c) => (
          <div className="grid cols-6">
            <Stat label="Cluster health" value={<HealthBadge health={c.health} />} hint={c.leaderId ? `quorum ${c.quorum} of ${c.size}` : "no leader: writes unavailable"} />
            <Stat label="Leader" value={<span className="mono">{c.leaderId ?? "none"}</span>} hint={`term ${c.term}`} />
            <Stat label="Healthy nodes" value={`${c.healthyNodes}/${c.size}`} hint={c.unavailableNodes ? `${c.unavailableNodes} unavailable` : "all serving"} />
            <Stat label="Commit index" value={fmtInt(c.commitIndex)} hint={`applied ${fmtInt(c.appliedIndex)} on leader`} />
            <Stat label="Writes / s" value={fmtNum(writeRate, 0)} hint={leaderMetrics ? `p50 ${fmtMs(leaderMetrics.requests.write.p50Ms)} · p99 ${fmtMs(leaderMetrics.requests.write.p99Ms)}` : "on leader"} />
            <Stat label="Reads / s" value={fmtNum(readRate, 0)} hint={leaderMetrics ? `p50 ${fmtMs(leaderMetrics.requests.read.p50Ms)} · p99 ${fmtMs(leaderMetrics.requests.read.p99Ms)}` : "all nodes"} />
          </div>
        )}
      </Async>

      <div className="grid cols-3 mt">
        <Card title="Throughput" sub="successful requests per second, all nodes" className="span-2">
          <Async state={metrics}>
            {() => series.some((p) => p.writes + p.reads > 0) ? (
              <TimeSeriesChart ariaLabel="Requests per second over the last minute" data={series} x="t" height={230}
                xFormat={(t) => new Date(t).toLocaleTimeString("en-GB", { hour12: false })}
                series={[{ key: "writes", label: "Writes", color: "var(--series-1)" }, { key: "reads", label: "Reads", color: "var(--series-2)" }]}
                valueFormat={(v) => `${fmtInt(v)} /s`} />
            ) : (
              <Empty title="No traffic in the last minute">
                Send requests from <Link to="/operations">Operations</Link>, run <code>scripts/bench.py</code>, or start a live experiment in <Link to="/faultlab">FaultLab</Link>.
              </Empty>
            )}
          </Async>
        </Card>
        <Card title="Recent cluster events" actions={<Link to="/timeline" className="small row">Timeline <ArrowRight size={14} /></Link>} flush>
          <Async state={events}>
            {() => keyEvents.length ? (
              <div style={{ maxHeight: 268, overflowY: "auto" }}>
                <EventList events={keyEvents} time={(e) => fmtClock(e.timeMs).slice(0, 8)} />
              </div>
            ) : <Empty title="No elections or faults recorded">Nothing but normal replication in each node's retained event history.</Empty>}
          </Async>
        </Card>
      </div>

      <Card title="Nodes" sub="per-node Raft state" className="mt" flush>
        <Async state={cluster}>
          {(c) => (
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th>Node</th><th>Health</th><th>Role</th><th className="num">Term</th><th className="num">Last log index</th>
                    <th className="num">Commit</th><th className="num">Applied</th><th className="num">Replication lag</th>
                    <th className="num">Write p99</th><th className="num">Uptime</th>
                  </tr>
                </thead>
                <tbody>
                  {c.nodes.map((n) => {
                    const lag = c.leaderId ? c.nodes.find((x) => x.id === c.leaderId)?.followers[n.id]?.lag : undefined;
                    const nm = m?.nodes[n.id];
                    return (
                      <tr key={n.id}>
                        <td className="mono"><b>{n.id}</b></td>
                        <td><HealthBadge health={n.health} /></td>
                        <td>{n.reachable ? <RoleBadge role={n.role} /> : <span className="muted">–</span>}</td>
                        <td className="num">{n.reachable ? n.term : "–"}</td>
                        <td className="num">{n.reachable ? fmtInt(n.lastLogIndex) : "–"}</td>
                        <td className="num">{n.reachable ? fmtInt(n.commitIndex) : "–"}</td>
                        <td className="num">{n.reachable ? fmtInt(n.lastApplied) : "–"}</td>
                        <td className="num">{n.id === c.leaderId ? <span className="muted">leader</span> : lag == null ? "–" : fmtInt(lag)}</td>
                        <td className="num">{nm && nm.requests.write.samples ? fmtMs(nm.requests.write.p99Ms) : "–"}</td>
                        <td className="num">{n.reachable ? `${Math.floor(n.uptimeMs / 60000)}m` : "–"}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </Async>
      </Card>
    </>
  );
}
