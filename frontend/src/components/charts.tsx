import type { ReactNode } from "react";
import {
  Bar, BarChart, CartesianGrid, Line, LineChart, ReferenceArea, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";

export interface Series {
  key: string;
  label: string;
  color: string; // a CSS var, e.g. "var(--series-1)"
  unit?: string;
}

const axisProps = {
  stroke: "var(--axis)",
  tick: { fill: "var(--text-3)", fontSize: 11 },
  tickLine: false,
  axisLine: { stroke: "var(--axis)" },
};

export function Legend({ series }: { series: Series[] }) {
  if (series.length < 2) return null;
  return (
    <div className="chart-legend" aria-hidden="true">
      {series.map((s) => (
        <span key={s.key}><i style={{ background: s.color }} />{s.label}</span>
      ))}
    </div>
  );
}

function TooltipBox(props: { active?: boolean; label?: unknown; payload?: { dataKey?: unknown; value?: unknown }[]; series: Series[]; xLabel: (x: number) => string; format: (v: number, s: Series) => string }) {
  if (!props.active || !props.payload?.length) return null;
  return (
    <div className="chart-tooltip">
      <div className="h">{props.xLabel(Number(props.label))}</div>
      {props.series.map((s) => {
        const p = props.payload!.find((x) => x.dataKey === s.key);
        if (!p || p.value == null) return null;
        return (
          <div className="r" key={s.key}>
            <span><i className="dot" style={{ background: s.color, width: 7, height: 7 }} />{s.label}</span>
            <b>{props.format(Number(p.value), s)}</b>
          </div>
        );
      })}
    </div>
  );
}

export interface Band { from: number; to: number; label?: string }

/** Time series with a crosshair tooltip. One y-axis only; different units belong in separate charts. */
export function TimeSeriesChart(props: {
  data: Record<string, number | null>[];
  x: string;
  series: Series[];
  height?: number;
  xFormat: (x: number) => string;
  yFormat?: (v: number) => string;
  valueFormat?: (v: number, s: Series) => string;
  bands?: Band[];
  markers?: { x: number; label: string }[];
  step?: boolean;
  ariaLabel: string;
}) {
  const fmt = props.valueFormat ?? ((v: number) => v.toLocaleString("en-US", { maximumFractionDigits: 1 }));
  return (
    <figure style={{ margin: 0 }} aria-label={props.ariaLabel}>
      <Legend series={props.series} />
      <div style={{ height: props.height ?? 220, marginTop: props.series.length > 1 ? 8 : 0 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={props.data} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
            <CartesianGrid stroke="var(--grid)" vertical={false} />
            <XAxis dataKey={props.x} type="number" domain={["dataMin", "dataMax"]} tickFormatter={props.xFormat} {...axisProps} minTickGap={40} />
            <YAxis {...axisProps} axisLine={false} width={48} tickFormatter={props.yFormat} allowDecimals={false} />
            {props.bands?.map((b, i) => (
              <ReferenceArea key={i} x1={b.from} x2={b.to} fill="var(--fault-band)" stroke="none" ifOverflow="hidden" />
            ))}
            {props.markers?.map((m, i) => (
              <ReferenceLine key={i} x={m.x} stroke="var(--text-3)" strokeWidth={1} />
            ))}
            <Tooltip cursor={{ stroke: "var(--text-3)", strokeWidth: 1 }} isAnimationActive={false}
              content={(p) => <TooltipBox {...(p as object)} series={props.series} xLabel={props.xFormat} format={fmt} />} />
            {props.series.map((s) => (
              <Line key={s.key} dataKey={s.key} name={s.label} stroke={s.color} strokeWidth={2} dot={false}
                activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--surface-raised)" }} isAnimationActive={false}
                type={props.step ? "stepAfter" : "monotone"} connectNulls={false} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </figure>
  );
}

/** Grouped bars: one group per category, one bar per series. */
export function GroupedBarChart(props: {
  data: Record<string, number | string>[];
  category: string;
  series: Series[];
  height?: number;
  yFormat?: (v: number) => string;
  valueFormat?: (v: number, s: Series) => string;
  categoryLabel: (c: string) => string;
  ariaLabel: string;
  footer?: ReactNode;
}) {
  const fmt = props.valueFormat ?? ((v: number) => v.toLocaleString("en-US", { maximumFractionDigits: 1 }));
  return (
    <figure style={{ margin: 0 }} aria-label={props.ariaLabel}>
      <Legend series={props.series} />
      <div style={{ height: props.height ?? 240, marginTop: 8 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={props.data} margin={{ top: 8, right: 12, bottom: 0, left: 0 }} barGap={2} barCategoryGap="28%">
            <CartesianGrid stroke="var(--grid)" vertical={false} />
            <XAxis dataKey={props.category} {...axisProps} tickFormatter={(c) => props.categoryLabel(String(c))} />
            <YAxis {...axisProps} axisLine={false} width={56} tickFormatter={props.yFormat} />
            <Tooltip cursor={{ fill: "var(--hover)" }} isAnimationActive={false}
              content={(p) => <TooltipBox {...(p as object)} label={undefined} series={props.series}
                xLabel={() => props.categoryLabel(String((p as { label?: unknown }).label))} format={fmt} />} />
            {props.series.map((s) => (
              <Bar key={s.key} dataKey={s.key} name={s.label} fill={s.color} radius={[4, 4, 0, 0]} maxBarSize={28} isAnimationActive={false} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
      {props.footer}
    </figure>
  );
}
