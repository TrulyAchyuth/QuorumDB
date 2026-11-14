import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { Pause, Play, SkipBack } from "lucide-react";
import { api } from "../api/client";
import type { EventPage, RaftEvent, RunDetail, RunSummary } from "../api/types";
import { usePoll } from "../hooks/usePoll";
import { Async, Badge, Card, Empty, RoleBadge, Segmented } from "../components/ui";
import { EventList } from "../components/EventList";
import { GROUP_LABEL, KEY_EVENTS, eventTone, groupOf, label, type EventGroup } from "../lib/events";
import { fmtClock } from "../lib/format";

const LANE_TYPES = [...KEY_EVENTS, "NODE_STARTED"].join(",");

export function Timeline() {
  const [params, setParams] = useSearchParams();
  const runId = params.get("run");
  const runs = usePoll((s) => api<RunSummary[]>("/faultlab/runs", { signal: s }), [], 0);
  return (
    <>
      <div className="page-head">
        <div>
          <h1>Event timeline</h1>
          <p>Events from every node on one time axis. Drag or play the playhead to replay how leadership, terms and faults evolved; the panel shows each node's state at that moment, reconstructed only from recorded events.</p>
        </div>
        <div className="field" style={{ minWidth: 300 }}>
          <label htmlFor="tl-src">Source</label>
          <select id="tl-src" className="select" value={runId ?? ""} onChange={(e) => setParams(e.target.value ? { run: e.target.value } : {})}>
            <option value="">Live cluster (recent events)</option>
            {(runs.data ?? []).map((r) => <option key={r.id} value={r.id}>{r.mode === "LIVE" ? "Live run" : "Simulation"}: {r.name} ({r.id})</option>)}
          </select>
        </div>
      </div>
      {runId ? <RunTimeline key={runId} runId={runId} focusSeq={params.get("seq") ? Number(params.get("seq")) : null} /> : <LiveTimeline />}
    </>
  );
}

// ------------------------------------------------------------------ data sources

function RunTimeline({ runId, focusSeq }: { runId: string; focusSeq: number | null }) {
  const run = usePoll((s) => api<RunDetail>(`/faultlab/runs/${runId}`, { signal: s }), [runId], 0);
  const lane = usePoll((s) => api<EventPage>(`/faultlab/runs/${runId}/events?limit=5000&types=${LANE_TYPES}`, { signal: s }), [runId], 0);
  const focus = usePoll((s) => focusSeq == null ? Promise.resolve(null)
    : api<EventPage>(`/faultlab/runs/${runId}/events?offset=${Math.max(0, focusSeq - 1)}&limit=1`, { signal: s }), [runId, focusSeq], 0);
  if (run.error) return <Card title="Run"><Empty title="Run not found">{run.error.message} Runs live in memory on the node that ran them.</Empty></Card>;
  return (
    <Async state={run} rows={6}>
      {(d) => (
        <Async state={lane} rows={6}>
          {(page) => (
            <TimelineView
              nodes={d.nodeIds}
              laneEvents={page.events}
              start={0}
              end={Math.max(d.metrics.durationMs, page.events.at(-1)?.timeMs ?? 0)}
              relative
              initialPlayhead={focus.data?.events[0]?.timeMs ?? null}
              focusSeq={focusSeq}
              loadEvents={async (from, groups) => {
                const types = groups.length ? typesFor(groups) : "";
                const p = await api<EventPage>(`/faultlab/runs/${runId}/events?fromMs=${Math.max(0, Math.floor(from))}&limit=150${types ? `&types=${types}` : ""}`);
                return p.events;
              }}
              header={<span className="small secondary"><Link to={`/faultlab/runs/${runId}`}>{d.summary.name}</Link> · {d.summary.mode === "LIVE" ? "live run" : `simulation, seed ${d.summary.seed}`} · {page.total} key events of {d.summary.events}</span>}
              bands={d.faults.map((f) => ({ from: f.injectedAtMs, to: f.healedAtMs < 0 ? d.metrics.durationMs : Math.max(f.healedAtMs, f.injectedAtMs + 1), label: f.description }))}
            />
          )}
        </Async>
      )}
    </Async>
  );
}

function LiveTimeline() {
  // Key events (elections, faults, recoveries) are fetched separately so replication traffic cannot crowd them out.
  const events = usePoll(async (s) => {
    const [key, recent] = await Promise.all([
      api<RaftEvent[]>(`/cluster/events?limit=500&types=${LANE_TYPES}`, { signal: s }),
      api<RaftEvent[]>("/cluster/events?limit=1000", { signal: s }),
    ]);
    const seen = new Set<string>();
    return [...key, ...recent].filter((e) => {
      const id = `${e.node}:${e.seq}`;
      if (seen.has(id)) return false;
      seen.add(id);
      return true;
    }).sort((a, b) => a.timeMs - b.timeMs);
  }, [], 3000);
  return (
    <Async state={events} rows={6}>
      {(list) => {
        if (list.length === 0) return <Card title="Live cluster"><Empty title="No events yet" /></Card>;
        const nodes = [...new Set(list.map((e) => e.node))].sort();
        const lane = list.filter((e) => KEY_EVENTS.has(e.type) || e.type === "NODE_STARTED");
        const start = list[0].timeMs;
        const end = Math.max(list.at(-1)!.timeMs, Date.now() - 1);
        return (
          <TimelineView nodes={nodes} laneEvents={lane} start={start} end={end} relative={false} initialPlayhead={end} focusSeq={null}
            header={<span className="small secondary">Last {list.length} events across {nodes.length} nodes · refreshes every 3 s</span>}
            loadEvents={async (from, groups) => list.filter((e) => e.timeMs >= from && (groups.length === 0 || groups.includes(groupOf(e.type)))).slice(0, 150)}
            bands={[]} />
        );
      }}
    </Async>
  );
}

function typesFor(groups: EventGroup[]) {
  const all = ["ELECTION_TIMEOUT", "PRE_VOTE_STARTED", "ELECTION_STARTED", "VOTE_REQUESTED", "VOTE_GRANTED", "VOTE_DENIED", "LEADER_ELECTED",
    "STEPPED_DOWN", "TERM_ADVANCED", "HEARTBEAT_SENT", "ENTRY_APPENDED", "LOG_TRUNCATED", "ENTRY_COMMITTED", "ENTRY_APPLIED", "SNAPSHOT_CREATED",
    "SNAPSHOT_SENT", "SNAPSHOT_INSTALLED", "NODE_STARTED", "RECOVERY_COMPLETED", "NODE_STOPPED", "NODE_FAILED", "MESSAGE_SENT", "MESSAGE_DELIVERED",
    "MESSAGE_DROPPED", "NODE_CRASHED", "NODE_RESTARTED", "NODE_DISCONNECTED", "NODE_RECOVERED", "FAULT_INJECTED", "FAULT_HEALED", "CLIENT_REQUEST", "CLIENT_RESPONSE"];
  return all.filter((t) => groups.includes(groupOf(t))).join(",");
}

// ------------------------------------------------------------------ state reconstruction

interface NodeState { role: string; term: number }

/** Replays lane events up to time t. Only recorded events are used; nothing is inferred beyond them. */
function stateAt(nodes: string[], events: RaftEvent[], t: number): Record<string, NodeState> {
  const s: Record<string, NodeState> = Object.fromEntries(nodes.map((n) => [n, { role: "FOLLOWER", term: 0 }]));
  for (const e of events) {
    if (e.timeMs > t) break;
    const target = ["NODE_CRASHED", "NODE_RESTARTED"].includes(e.type) && e.peer && s[e.peer] ? e.peer : e.node;
    const n = s[target];
    if (!n) continue;
    if (e.term > n.term && !["FAULT_INJECTED", "FAULT_HEALED", "NODE_CRASHED", "NODE_RESTARTED"].includes(e.type)) n.term = e.term;
    switch (e.type) {
      // A deposed leader keeps believing it leads until it records STEPPED_DOWN; two leaders of different
      // terms can coexist (e.g. an isolated old leader). That is shown as recorded, not corrected.
      case "LEADER_ELECTED": n.role = "LEADER"; break;
      case "STEPPED_DOWN": n.role = "FOLLOWER"; break;
      case "ELECTION_STARTED": n.role = "CANDIDATE"; break;
      case "PRE_VOTE_STARTED": if (n.role !== "LEADER") n.role = "PRE_CANDIDATE"; break;
      case "NODE_CRASHED": case "NODE_FAILED": case "NODE_STOPPED": n.role = "DOWN"; break;
      case "NODE_RESTARTED": case "RECOVERY_COMPLETED": case "NODE_STARTED": n.role = "FOLLOWER"; break;
    }
  }
  return s;
}

/** Leadership and downtime segments per node, for drawing the lanes. */
function segments(nodes: string[], events: RaftEvent[], end: number) {
  const out: Record<string, { from: number; to: number; kind: "leader" | "down" }[]> = Object.fromEntries(nodes.map((n) => [n, []]));
  const open: Record<string, { from: number; kind: "leader" | "down" } | null> = Object.fromEntries(nodes.map((n) => [n, null]));
  const close = (n: string, t: number) => {
    const o = open[n];
    if (o) out[n].push({ from: o.from, to: t, kind: o.kind });
    open[n] = null;
  };
  for (const e of events) {
    const target = ["NODE_CRASHED", "NODE_RESTARTED"].includes(e.type) && e.peer && out[e.peer] ? e.peer : e.node;
    if (!out[target]) continue;
    if (e.type === "LEADER_ELECTED") {
      close(target, e.timeMs);
      open[target] = { from: e.timeMs, kind: "leader" };
    } else if (e.type === "STEPPED_DOWN") {
      if (open[target]?.kind === "leader") close(target, e.timeMs);
    } else if (["NODE_CRASHED", "NODE_FAILED", "NODE_STOPPED"].includes(e.type)) {
      close(target, e.timeMs);
      open[target] = { from: e.timeMs, kind: "down" };
    } else if (["NODE_RESTARTED", "RECOVERY_COMPLETED"].includes(e.type) && open[target]?.kind === "down") {
      close(target, e.timeMs);
    }
  }
  nodes.forEach((n) => close(n, end));
  return out;
}

// ------------------------------------------------------------------ view

const toneColor = { critical: "var(--critical)", accent: "var(--accent)", good: "var(--good)", muted: "var(--text-3)" };

function TimelineView(props: {
  nodes: string[]; laneEvents: RaftEvent[]; start: number; end: number; relative: boolean; initialPlayhead: number | null;
  focusSeq: number | null; header: React.ReactNode; bands: { from: number; to: number; label?: string }[];
  loadEvents: (fromMs: number, groups: EventGroup[]) => Promise<RaftEvent[]>;
}) {
  const { nodes, laneEvents, start, end } = props;
  const span = Math.max(1, end - start);
  const [playhead, setPlayhead] = useState(props.initialPlayhead ?? start);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState<"1" | "4" | "16">("4");
  const [groups, setGroups] = useState<EventGroup[]>(["election", "fault", "lifecycle", "snapshot"]);
  const [list, setList] = useState<RaftEvent[]>([]);
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (props.initialPlayhead != null) setPlayhead(Math.max(start, props.initialPlayhead - 1));
  }, [props.initialPlayhead, start]);

  useEffect(() => {
    if (!playing) return;
    let last = performance.now();
    let raf = 0;
    const step = (now: number) => {
      const dt = (now - last) * Number(speed) * (props.relative ? 1 : 1);
      last = now;
      setPlayhead((p) => {
        const next = p + dt;
        if (next >= end) {
          setPlaying(false);
          return end;
        }
        return next;
      });
      raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, [playing, speed, end, props.relative]);

  // Event list follows the playhead (debounced).
  useEffect(() => {
    const t = setTimeout(() => {
      props.loadEvents(Math.max(start, playhead - 200), groups).then(setList).catch(() => setList([]));
    }, playing ? 400 : 120);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [Math.round(playhead / 50), groups.join(",")]);

  const state = useMemo(() => stateAt(nodes, laneEvents, playhead), [nodes, laneEvents, playhead]);
  const segs = useMemo(() => segments(nodes, laneEvents, end), [nodes, laneEvents, end]);
  const W = 1000;
  const LABEL = 56;
  const LANE = 34;
  const H = nodes.length * LANE + 26;
  const x = (t: number) => LABEL + ((t - start) / span) * (W - LABEL - 8);
  const fmt = (t: number) => (props.relative ? `${((t - start) / 1000).toFixed(2)}s` : fmtClock(t));
  const highlight = new Set(props.focusSeq ? [props.focusSeq] : []);

  const seek = (clientX: number) => {
    const r = svgRef.current!.getBoundingClientRect();
    const px = ((clientX - r.left) / r.width) * W;
    setPlayhead(Math.min(end, Math.max(start, start + ((px - LABEL) / (W - LABEL - 8)) * span)));
  };

  return (
    <div className="stack gap-lg">
      <Card title="Swimlanes" sub={props.header}
        actions={
          <div className="row">
            <button className="btn sm" onClick={() => { setPlaying(false); setPlayhead(start); }} aria-label="Back to start"><SkipBack /></button>
            <button className="btn sm primary" onClick={() => { if (playhead >= end) setPlayhead(start); setPlaying((p) => !p); }}>
              {playing ? <Pause /> : <Play />}{playing ? "Pause" : "Replay"}</button>
            <Segmented label="Playback speed" value={speed} onChange={setSpeed} items={[{ id: "1", label: "1×" }, { id: "4", label: "4×" }, { id: "16", label: "16×" }]} />
            <span className="mono small" style={{ minWidth: 92, textAlign: "right" }}>{fmt(playhead)}</span>
          </div>
        }>
        <svg ref={svgRef} viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: "block", cursor: "crosshair", userSelect: "none" }}
          onMouseDown={(e) => { seek(e.clientX); const move = (ev: MouseEvent) => seek(ev.clientX); const up = () => { window.removeEventListener("mousemove", move); window.removeEventListener("mouseup", up); }; window.addEventListener("mousemove", move); window.addEventListener("mouseup", up); }}
          role="img" aria-label="Per-node event swimlanes">
          {props.bands.map((b, i) => (
            <rect key={i} x={x(b.from)} y={0} width={Math.max(2, x(b.to) - x(b.from))} height={H - 22} fill="var(--fault-band)"><title>{b.label}</title></rect>
          ))}
          {nodes.map((n, i) => {
            const y = i * LANE;
            return (
              <g key={n}>
                <line x1={LABEL} x2={W - 8} y1={y + LANE / 2} y2={y + LANE / 2} stroke="var(--grid)" />
                <text x={0} y={y + LANE / 2 + 4} fontSize={12} fontWeight={600} fill="var(--text)" fontFamily="var(--mono)">{n}</text>
                {segs[n].map((s, j) => (
                  <rect key={j} x={x(s.from)} y={y + LANE / 2 - 5} width={Math.max(2, x(s.to) - x(s.from))} height={10} rx={4}
                    fill={s.kind === "leader" ? "var(--accent)" : "var(--critical)"} opacity={s.kind === "leader" ? 0.85 : 0.35}>
                    <title>{s.kind === "leader" ? "leader" : "down"} {fmt(s.from)} – {fmt(s.to)}</title>
                  </rect>
                ))}
                {laneEvents.filter((e) => e.node === n && !["LEADER_ELECTED", "NODE_STARTED"].includes(e.type) && eventTone(e.type) !== "muted").map((e) => (
                  <circle key={e.seq} cx={x(e.timeMs)} cy={y + LANE / 2} r={3.2} fill={toneColor[eventTone(e.type)]} stroke="var(--surface-raised)" strokeWidth={1.5}>
                    <title>{fmt(e.timeMs)} {label(e.type)}{e.detail ? `: ${e.detail}` : ""}</title>
                  </circle>
                ))}
              </g>
            );
          })}
          <line x1={LABEL} x2={W - 8} y1={H - 20} y2={H - 20} stroke="var(--axis)" />
          {[0, 0.25, 0.5, 0.75, 1].map((f) => (
            <text key={f} x={x(start + f * span)} y={H - 6} fontSize={10.5} fill="var(--text-3)" textAnchor={f === 0 ? "start" : f === 1 ? "end" : "middle"}>{fmt(start + f * span)}</text>
          ))}
          <line x1={x(playhead)} x2={x(playhead)} y1={0} y2={H - 20} stroke="var(--text)" strokeWidth={1.5} />
        </svg>
        <div className="chart-legend" style={{ marginTop: 8 }}>
          <span><i style={{ background: "var(--accent)" }} />leader</span>
          <span><i style={{ background: "var(--critical)", opacity: 0.4 }} />down</span>
          <span><i style={{ background: "var(--fault-band)", height: 10 }} />fault window</span>
          <span><i className="dot" style={{ background: "var(--critical)", width: 7, height: 7 }} />failure event</span>
          <span><i className="dot" style={{ background: "var(--good)", width: 7, height: 7 }} />recovery event</span>
        </div>
      </Card>

      <div className="grid" style={{ gridTemplateColumns: "minmax(260px, 340px) minmax(0, 1fr)" }}>
        <Card title="State at playhead" sub={fmt(playhead)} flush>
          <table className="table">
            <thead><tr><th>Node</th><th>Role</th><th className="num">Term</th></tr></thead>
            <tbody>
              {nodes.map((n) => (
                <tr key={n}><td className="mono"><b>{n}</b></td><td><RoleBadge role={state[n].role} /></td><td className="num">{state[n].term || "–"}</td></tr>
              ))}
            </tbody>
          </table>
          {props.bands.filter((b) => b.from <= playhead && playhead <= b.to).map((b, i) => (
            <div key={i} style={{ padding: "8px 16px" }}><Badge tone="critical">active</Badge> <span className="small">{b.label}</span></div>
          ))}
        </Card>
        <Card title="Events from playhead" flush
          actions={<div className="row wrap" style={{ gap: 4 }}>
            {(Object.keys(GROUP_LABEL) as EventGroup[]).map((g) => {
              const on = groups.includes(g);
              return <button key={g} className={`btn sm ${on ? "accent" : ""}`} aria-pressed={on}
                onClick={() => setGroups(on ? groups.filter((x) => x !== g) : [...groups, g])}>{GROUP_LABEL[g]}</button>;
            })}
          </div>}>
          {list.length === 0 ? <Empty title="No matching events after this point">Move the playhead or enable more event groups.</Empty> : (
            <div style={{ maxHeight: 460, overflowY: "auto" }}>
              <EventList events={list} time={(e) => fmt(e.timeMs)} highlight={highlight} onSelect={(e) => setPlayhead(e.timeMs)} />
            </div>
          )}
        </Card>
      </div>
    </div>
  );
}
