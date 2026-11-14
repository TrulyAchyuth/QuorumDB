import type { NodeView } from "../api/types";

/**
 * Cluster topology: members on a circle, the leader marked, and each link drawn from the leader's
 * perspective (replication healthy / lagging / cut). Links between followers are not shown: Raft
 * followers do not talk to each other.
 */
export function Topology(props: { nodes: NodeView[]; selected: string | null; onSelect: (id: string) => void }) {
  const n = props.nodes.length;
  const size = 340;
  const r = n <= 1 ? 0 : 112;
  const cx = size / 2;
  const cy = size / 2 + 4;
  const pos = new Map(props.nodes.map((node, i) => {
    const a = -Math.PI / 2 + (2 * Math.PI * i) / Math.max(n, 1);
    return [node.id, { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) }];
  }));
  const leader = props.nodes.find((x) => x.reachable && x.role === "LEADER");

  return (
    <svg viewBox={`0 0 ${size} ${size}`} width="100%" style={{ maxHeight: 360, display: "block" }} role="img"
      aria-label={`Cluster of ${n} nodes${leader ? `, leader ${leader.id}` : ", no leader"}`}>
      {leader && props.nodes.filter((x) => x.id !== leader.id).map((f) => {
        const a = pos.get(leader.id)!;
        const b = pos.get(f.id)!;
        const link = leader.links[f.id];
        const follower = leader.followers[f.id];
        const cut = link?.faultBlocked || !f.reachable || f.health === "DOWN" || (follower && follower.lastAckAgoMs > 1000);
        const lagging = !cut && follower && follower.lag > 50;
        const color = cut ? "var(--critical)" : lagging ? "var(--warning)" : "var(--accent)";
        return (
          <g key={f.id}>
            <line x1={a.x} y1={a.y} x2={b.x} y2={b.y} stroke={color} strokeWidth={cut ? 1.5 : 2}
              strokeOpacity={cut ? 0.55 : 0.75} strokeDasharray={cut ? "4 5" : undefined} />
            {follower && (
              <text x={(a.x + b.x) / 2} y={(a.y + b.y) / 2 + 4} textAnchor="middle" fontSize={10.5} fill="var(--text-2)"
                stroke="var(--surface-raised)" strokeWidth={5} paintOrder="stroke" strokeLinejoin="round">
                {cut ? "no contact" : `lag ${follower.lag}`}
              </text>
            )}
          </g>
        );
      })}
      {props.nodes.map((node) => {
        const p = pos.get(node.id)!;
        const isLeader = leader?.id === node.id;
        const down = !node.reachable || node.health === "DOWN";
        const selected = props.selected === node.id;
        return (
          <g key={node.id} transform={`translate(${p.x},${p.y})`} style={{ cursor: "pointer" }}
            onClick={() => props.onSelect(node.id)} role="button" tabIndex={0} aria-label={`Node ${node.id}`}
            onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && props.onSelect(node.id)}>
            {selected && <circle r={36} fill="none" stroke="var(--accent)" strokeWidth={2} strokeOpacity={0.35} />}
            <circle r={29} fill="var(--surface-raised)" stroke={down ? "var(--critical)" : isLeader ? "var(--accent)" : "var(--border-strong)"}
              strokeWidth={isLeader ? 2.5 : 1.5} strokeDasharray={down ? "4 4" : undefined} />
            <text y={-2} textAnchor="middle" fontSize={14} fontWeight={650} fill={down ? "var(--text-3)" : "var(--text)"}>{node.id}</text>
            <text y={14} textAnchor="middle" fontSize={9.5} fontWeight={600} letterSpacing="0.05em"
              fill={isLeader ? "var(--accent)" : down ? "var(--critical-text)" : "var(--text-3)"}>
              {down ? (node.reachable ? "DOWN" : "UNREACHABLE") : node.role === "PRE_CANDIDATE" ? "PRE-CAND." : node.role}
            </text>
            <text y={47} textAnchor="middle" fontSize={10.5} fill="var(--text-3)" stroke="var(--surface-raised)" strokeWidth={5}
              paintOrder="stroke" strokeLinejoin="round">
              {node.reachable ? `term ${node.term} · ${node.commitIndex}` : ""}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
