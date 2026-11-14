import { Link, useSearchParams } from "react-router-dom";
import { ArrowLeft } from "lucide-react";
import { api } from "../../api/client";
import type { Comparison } from "../../api/types";
import { usePoll } from "../../hooks/usePoll";
import { Async, Callout, Card } from "../../components/ui";

export function Compare() {
  const [params] = useSearchParams();
  const a = params.get("a") ?? "";
  const b = params.get("b") ?? "";
  const cmp = usePoll((s) => api<Comparison>(`/faultlab/compare?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`, { signal: s }), [a, b], 0);
  return (
    <>
      <div className="row" style={{ marginBottom: 12 }}><Link to="/faultlab?tab=runs" className="row small"><ArrowLeft size={14} />All runs</Link></div>
      <div className="page-head"><div><h1>Run comparison</h1><p>Side by side. The highlighted value is the better one where "better" is well defined.</p></div></div>
      <Async state={cmp} rows={10}>
        {(c) => (
          <>
            {c.observations.length > 0 && <div className="stack" style={{ marginBottom: 16 }}>{c.observations.map((o) => <Callout key={o}>{o}</Callout>)}</div>}
            <Card flush title={<><Link to={`/faultlab/runs/${a}`}>A: {c.aName}</Link> <span className="muted">vs</span> <Link to={`/faultlab/runs/${b}`}>B: {c.bName}</Link></>}>
              <div className="table-wrap">
                <table className="table">
                  <thead><tr><th>Metric</th><th className="num">A</th><th className="num">B</th><th className="num">B − A</th></tr></thead>
                  <tbody>
                    {c.rows.map((r) => (
                      <tr key={r.metric}>
                        <td>{r.metric}{r.unit && <span className="muted xs"> ({r.unit})</span>}</td>
                        <td className="num" style={r.better === "a" ? { fontWeight: 650, color: "var(--good-text)" } : undefined}>{r.a}</td>
                        <td className="num" style={r.better === "b" ? { fontWeight: 650, color: "var(--good-text)" } : undefined}>{r.b}</td>
                        <td className="num muted">{r.delta}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          </>
        )}
      </Async>
    </>
  );
}
