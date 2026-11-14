export const fmtInt = (n: number | null | undefined) => (n == null ? "–" : Math.round(n).toLocaleString("en-US"));

export function fmtNum(n: number | null | undefined, digits = 1) {
  if (n == null || Number.isNaN(n)) return "–";
  return n.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: 0 });
}

export function fmtMs(ms: number | null | undefined) {
  if (ms == null || ms < 0) return "–";
  if (ms < 1) return `${ms.toFixed(2)} ms`;
  if (ms < 1000) return `${Math.round(ms)} ms`;
  return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)} s`;
}

export function fmtBytes(b: number | null | undefined) {
  if (b == null) return "–";
  const units = ["B", "KB", "MB", "GB"];
  let v = b;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

export function fmtDuration(ms: number) {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

export function fmtClock(epochMs: number) {
  const d = new Date(epochMs);
  return d.toLocaleTimeString("en-GB", { hour12: false }) + "." + String(d.getMilliseconds()).padStart(3, "0");
}

export function fmtPct(v: number | null | undefined, digits = 1) {
  return v == null ? "–" : `${(v * 100).toFixed(digits)}%`;
}

export function ago(epochMs: number) {
  const d = Date.now() - epochMs;
  if (d < 2000) return "just now";
  if (d < 60_000) return `${Math.round(d / 1000)}s ago`;
  if (d < 3_600_000) return `${Math.round(d / 60_000)}m ago`;
  return `${Math.round(d / 3_600_000)}h ago`;
}
