import { type ReactNode, useEffect, useRef } from "react";
import { AlertTriangle, CheckCircle2, CircleDashed, Crown, Inbox, Loader2, MinusCircle, WifiOff, XCircle } from "lucide-react";

export function Card(props: { title?: ReactNode; sub?: ReactNode; actions?: ReactNode; children: ReactNode; flush?: boolean; className?: string }) {
  return (
    <section className={`card ${props.className ?? ""}`}>
      {props.title !== undefined && (
        <header className="card-head">
          <h3>{props.title}</h3>
          {props.sub && <span className="sub">{props.sub}</span>}
          {props.actions && <div className="actions">{props.actions}</div>}
        </header>
      )}
      <div className={`card-body ${props.flush ? "flush" : ""}`}>{props.children}</div>
    </section>
  );
}

export function Stat(props: { label: ReactNode; value: ReactNode; unit?: string; hint?: ReactNode; icon?: ReactNode }) {
  return (
    <div className="card stat">
      <div className="label">{props.icon}{props.label}</div>
      <div className="value">
        {props.value}
        {props.unit && <span className="unit">{props.unit}</span>}
      </div>
      {props.hint && <div className="hint">{props.hint}</div>}
    </div>
  );
}

type Tone = "good" | "warning" | "critical" | "neutral" | "accent";

export function Badge(props: { tone?: Tone; icon?: ReactNode; children: ReactNode; title?: string; outline?: boolean }) {
  return (
    <span className={`badge ${props.tone ?? ""} ${props.outline ? "outline" : ""}`} title={props.title}>
      {props.icon}
      {props.children}
    </span>
  );
}

/** Status is always icon + label + color, never color alone. */
const HEALTH: Record<string, { tone: Tone; icon: ReactNode; label: string }> = {
  UP: { tone: "good", icon: <CheckCircle2 />, label: "Healthy" },
  HEALTHY: { tone: "good", icon: <CheckCircle2 />, label: "Healthy" },
  DEGRADED: { tone: "warning", icon: <AlertTriangle />, label: "Degraded" },
  DOWN: { tone: "critical", icon: <XCircle />, label: "Down" },
  UNAVAILABLE: { tone: "critical", icon: <XCircle />, label: "Unavailable" },
  UNREACHABLE: { tone: "critical", icon: <WifiOff />, label: "Unreachable" },
};

export function HealthBadge({ health }: { health: string }) {
  const h = HEALTH[health] ?? { tone: "neutral" as Tone, icon: <CircleDashed />, label: health };
  return <Badge tone={h.tone} icon={h.icon}>{h.label}</Badge>;
}

export function RoleBadge({ role }: { role: string }) {
  if (role === "LEADER") return <Badge tone="accent" icon={<Crown />}>Leader</Badge>;
  if (role === "FOLLOWER") return <Badge>Follower</Badge>;
  if (role === "CANDIDATE" || role === "PRE_CANDIDATE") return <Badge tone="warning" icon={<CircleDashed />}>{role === "CANDIDATE" ? "Candidate" : "Pre-candidate"}</Badge>;
  return <Badge tone="critical" icon={<MinusCircle />}>{role === "UNKNOWN" ? "Unknown" : "Down"}</Badge>;
}

const VERDICT: Record<string, { tone: Tone; icon: ReactNode; label: string }> = {
  SAFE: { tone: "good", icon: <CheckCircle2 />, label: "Safe" },
  PASS: { tone: "good", icon: <CheckCircle2 />, label: "Pass" },
  LINEARIZABLE: { tone: "good", icon: <CheckCircle2 />, label: "Linearizable" },
  HEALTHY: { tone: "good", icon: <CheckCircle2 />, label: "Healthy" },
  OK: { tone: "good", icon: <CheckCircle2 />, label: "OK" },
  EXPECTED_UNAVAILABILITY: { tone: "neutral", icon: <MinusCircle />, label: "Expected unavailability" },
  NOT_EXERCISED: { tone: "neutral", icon: <MinusCircle />, label: "Not exercised" },
  NOT_CHECKED: { tone: "neutral", icon: <MinusCircle />, label: "Not checked" },
  INCONCLUSIVE: { tone: "warning", icon: <AlertTriangle />, label: "Inconclusive" },
  UNDETERMINED: { tone: "warning", icon: <AlertTriangle />, label: "Undetermined" },
  LIVENESS_ISSUE: { tone: "warning", icon: <AlertTriangle />, label: "Liveness issue" },
  ISSUE: { tone: "warning", icon: <AlertTriangle />, label: "Issue" },
  UNKNOWN: { tone: "warning", icon: <AlertTriangle />, label: "Unknown" },
  VIOLATION: { tone: "critical", icon: <XCircle />, label: "Violation" },
  VIOLATED: { tone: "critical", icon: <XCircle />, label: "Violated" },
  SAFETY_VIOLATION: { tone: "critical", icon: <XCircle />, label: "Safety violation" },
  NOT_LINEARIZABLE: { tone: "critical", icon: <XCircle />, label: "Not linearizable" },
};

export function VerdictBadge({ value }: { value: string }) {
  const v = VERDICT[value] ?? { tone: "neutral" as Tone, icon: <CircleDashed />, label: value };
  return <Badge tone={v.tone} icon={v.icon}>{v.label}</Badge>;
}

export function Empty(props: { title: string; children?: ReactNode; icon?: ReactNode; action?: ReactNode }) {
  return (
    <div className="state">
      {props.icon ?? <Inbox />}
      <strong>{props.title}</strong>
      {props.children && <div className="small">{props.children}</div>}
      {props.action && <div className="mt">{props.action}</div>}
    </div>
  );
}

export function ErrorState(props: { error: Error; onRetry?: () => void }) {
  return (
    <div className="state error" role="alert">
      <XCircle />
      <strong>Could not load</strong>
      <div className="small">{props.error.message}</div>
      {props.onRetry && <button className="btn sm mt" onClick={props.onRetry}>Retry</button>}
    </div>
  );
}

export function Loading({ rows = 3, height = 18 }: { rows?: number; height?: number }) {
  return (
    <div className="stack" aria-busy="true" aria-label="Loading" style={{ padding: 16 }}>
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} className="skeleton" style={{ height, width: `${92 - i * 14}%` }} />
      ))}
    </div>
  );
}

export function Spinner() {
  return <Loader2 className="spin" size={15} />;
}

/** Wraps a polled section: first load, error with retry, or children (kept visible while refreshing). */
export function Async<T>(props: { state: { data: T | null; error: Error | null; loading: boolean; refresh: () => void }; rows?: number; children: (data: T) => ReactNode }) {
  const { data, error, loading, refresh } = props.state;
  if (loading && data == null) return <Loading rows={props.rows} />;
  if (error && data == null) return <ErrorState error={error} onRetry={refresh} />;
  if (data == null) return <Loading rows={props.rows} />;
  return <>{props.children(data)}</>;
}

export function ConfirmDialog(props: {
  open: boolean; title: string; children: ReactNode; confirmLabel: string; danger?: boolean; busy?: boolean;
  onConfirm: () => void; onCancel: () => void;
}) {
  const cancelRef = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    if (!props.open) return;
    cancelRef.current?.focus();
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && props.onCancel();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [props.open, props.onCancel]);
  if (!props.open) return null;
  return (
    <div className="overlay" onMouseDown={(e) => e.target === e.currentTarget && props.onCancel()}>
      <div className="dialog" role="alertdialog" aria-modal="true" aria-labelledby="dlg-title">
        <div className="body">
          <h2 id="dlg-title">{props.title}</h2>
          <div className="small">{props.children}</div>
        </div>
        <div className="foot">
          <button ref={cancelRef} className="btn" onClick={props.onCancel}>Cancel</button>
          <button className={`btn ${props.danger ? "solid-danger" : "primary"}`} onClick={props.onConfirm} disabled={props.busy}>
            {props.busy && <Spinner />}{props.confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}

export function Tabs<T extends string>(props: { value: T; onChange: (v: T) => void; items: { id: T; label: string; icon?: ReactNode }[] }) {
  return (
    <div className="tabs" role="tablist">
      {props.items.map((it) => (
        <button key={it.id} role="tab" aria-selected={props.value === it.id} className={props.value === it.id ? "on" : ""}
          onClick={() => props.onChange(it.id)}>
          {it.icon}{it.label}
        </button>
      ))}
    </div>
  );
}

export function Segmented<T extends string>(props: { value: T; onChange: (v: T) => void; items: { id: T; label: string }[]; label?: string }) {
  return (
    <div className="segmented" role="radiogroup" aria-label={props.label}>
      {props.items.map((it) => (
        <button key={it.id} role="radio" aria-checked={props.value === it.id} className={props.value === it.id ? "on" : ""}
          onClick={() => props.onChange(it.id)}>
          {it.label}
        </button>
      ))}
    </div>
  );
}

export function Callout(props: { tone?: "warning" | "critical"; icon?: ReactNode; children: ReactNode }) {
  return <div className={`callout ${props.tone ?? ""}`}>{props.icon ?? <AlertTriangle />}<div>{props.children}</div></div>;
}
