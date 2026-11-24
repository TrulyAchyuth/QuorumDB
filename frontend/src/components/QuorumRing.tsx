import type { CSSProperties } from "react";
import type { NodeView } from "../api/types";

const WIDTH = 360;
const HEIGHT = 300;
const CENTER_X = WIDTH / 2;
const CENTER_Y = HEIGHT / 2 + 4;
const RING_RADIUS = 98;
const NODE_RADIUS = 24;
const STAGGER_MS = 110;

interface Placed {
  node: NodeView;
  x: number;
  y: number;
}

function healthColor(node: NodeView): string {
  if (!node.reachable) return "var(--text-3)";
  if (node.health === "UP") return "var(--good)";
  if (node.health === "DEGRADED") return "var(--warning)";
  return "var(--critical)";
}

/** Puts the leader at the top of the ring and the other nodes clockwise, sorted by id. */
function place(nodes: NodeView[], leaderId: string | null): Placed[] {
  const ordered = [...nodes].sort((a, b) => {
    if (a.id === leaderId) return -1;
    if (b.id === leaderId) return 1;
    return a.id.localeCompare(b.id);
  });
  return ordered.map((node, i) => {
    const angle = -Math.PI / 2 + (i * 2 * Math.PI) / ordered.length;
    return {
      node,
      x: CENTER_X + RING_RADIUS * Math.cos(angle),
      y: CENTER_Y + RING_RADIUS * Math.sin(angle),
    };
  });
}

/**
 * Live picture of the cluster: one circle per node, the leader in the centre-top with a halo, and
 * packets travelling leader to followers (writes, cyan) and followers to leader (reads, violet).
 * Motion is decorative only, is removed under prefers-reduced-motion, and stops when no leader exists.
 */
export function QuorumRing({ nodes, leaderId, live }: { nodes: NodeView[]; leaderId: string | null; live: boolean }) {
  const placed = place(nodes, leaderId);
  const leader = placed.find((p) => p.node.id === leaderId) ?? null;
  const followers = placed.filter((p) => p !== leader);
  const summary = nodes.map((n) => `${n.id} ${n.role.toLowerCase()}`).join(", ");

  return (
    <svg className="quorum" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label={`Cluster of ${nodes.length} nodes: ${summary}`}>
      {placed.map((a, i) =>
        placed.slice(i + 1).map((b) => {
          const touchesLeader = leader !== null && (a === leader || b === leader);
          const style: CSSProperties = { animationDelay: `${i * 60}ms` };
          return (
            <path
              key={`${a.node.id}-${b.node.id}`}
              className={`edge ${touchesLeader ? "live" : ""}`}
              d={`M${a.x} ${a.y} L${b.x} ${b.y}`}
              pathLength={1}
              style={style}
            />
          );
        }),
      )}

      {live && leader && followers.map((f, i) => (
        <g key={`packets-${f.node.id}`}>
          <circle r={3.2} className="packet">
            <animateMotion dur="1.8s" repeatCount="indefinite" begin={`${i * 0.45}s`}
              path={`M${leader.x} ${leader.y} L${f.x} ${f.y}`} />
          </circle>
          <circle r={2.6} className="packet read">
            <animateMotion dur="1.8s" repeatCount="indefinite" begin={`${i * 0.45 + 0.9}s`}
              path={`M${f.x} ${f.y} L${leader.x} ${leader.y}`} />
          </circle>
        </g>
      ))}

      {leader && <circle cx={leader.x} cy={leader.y} r={NODE_RADIUS + 10} className="halo" />}

      {placed.map((p, i) => (
        <g key={p.node.id} className="node" style={{ animationDelay: `${i * STAGGER_MS}ms` }}>
          <circle cx={p.x} cy={p.y} r={NODE_RADIUS} className="node-ring" stroke={healthColor(p.node)} />
          <text x={p.x} y={p.y + 4} textAnchor="middle" className="node-role">
            {p.node.reachable ? p.node.role.toLowerCase() : "down"}
          </text>
          <text x={p.x} y={p.y + NODE_RADIUS + 17} textAnchor="middle" className="node-label">
            {p.node.id}
          </text>
        </g>
      ))}
    </svg>
  );
}
