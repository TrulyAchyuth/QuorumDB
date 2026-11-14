import type { RaftEvent } from "../api/types";
import { describe, eventTone, label } from "../lib/events";

const toneColor = { critical: "var(--critical)", accent: "var(--accent)", good: "var(--good)", muted: "var(--axis)" };

export function EventList(props: { events: RaftEvent[]; time: (e: RaftEvent) => string; highlight?: Set<number>; onSelect?: (e: RaftEvent) => void }) {
  return (
    <div className="events" role="list">
      {props.events.map((e) => (
        <div key={`${e.node}-${e.seq}`} role="listitem" className={`event ${props.highlight?.has(e.seq) ? "highlight" : ""}`}
          onClick={() => props.onSelect?.(e)} style={props.onSelect ? { cursor: "pointer" } : undefined}>
          <span className="t">{props.time(e)}</span>
          <span className="n">{e.node}</span>
          <span className="what">
            <span className="row" style={{ gap: 6, display: "inline-flex" }}>
              <i className="dot" style={{ background: toneColor[eventTone(e.type)], width: 6, height: 6 }} />
              <span className="type">{label(e.type)}</span>
              {e.term > 0 && <span className="muted xs">term {e.term}</span>}
            </span>
            {describe(e) && <div className="detail">{describe(e)}</div>}
          </span>
        </div>
      ))}
    </div>
  );
}
