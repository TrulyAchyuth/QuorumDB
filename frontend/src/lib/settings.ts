// Viewer-local settings. The admin token lives in sessionStorage only (cleared when the tab closes)
// and is never sent anywhere except as an Authorization header to this dashboard's own API.
export interface Settings {
  pinnedNode: string | null;
  adminToken: string;
  pollMs: number;
}

const DEFAULTS: Settings = { pinnedNode: null, adminToken: "", pollMs: 1000 };
let cache: Settings | null = null;
const listeners = new Set<() => void>();

function read(): Settings {
  const s = { ...DEFAULTS };
  try {
    const local = JSON.parse(localStorage.getItem("quorumdb.settings") ?? "{}");
    if (typeof local.pinnedNode === "string" || local.pinnedNode === null) s.pinnedNode = local.pinnedNode ?? null;
    if (typeof local.pollMs === "number" && local.pollMs >= 500) s.pollMs = local.pollMs;
  } catch {
    /* storage unavailable: defaults */
  }
  try {
    s.adminToken = sessionStorage.getItem("quorumdb.adminToken") ?? "";
  } catch {
    /* ignore */
  }
  return s;
}

export function getSettings(): Settings {
  if (!cache) cache = read();
  return cache;
}

export function updateSettings(patch: Partial<Settings>) {
  cache = { ...getSettings(), ...patch };
  try {
    localStorage.setItem("quorumdb.settings", JSON.stringify({ pinnedNode: cache.pinnedNode, pollMs: cache.pollMs }));
    if (patch.adminToken !== undefined) {
      if (patch.adminToken) sessionStorage.setItem("quorumdb.adminToken", patch.adminToken);
      else sessionStorage.removeItem("quorumdb.adminToken");
    }
  } catch {
    /* ignore */
  }
  listeners.forEach((l) => l());
}

export function subscribeSettings(l: () => void) {
  listeners.add(l);
  return () => listeners.delete(l);
}
