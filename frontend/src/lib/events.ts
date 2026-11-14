// Display metadata for Raft and FaultLab event types.
export type EventGroup = "election" | "replication" | "snapshot" | "lifecycle" | "fault" | "client" | "network";

const GROUPS: Record<string, EventGroup> = {
  ELECTION_TIMEOUT: "election", PRE_VOTE_STARTED: "election", ELECTION_STARTED: "election", VOTE_REQUESTED: "election",
  VOTE_GRANTED: "election", VOTE_DENIED: "election", LEADER_ELECTED: "election", STEPPED_DOWN: "election", TERM_ADVANCED: "election",
  HEARTBEAT_SENT: "replication", ENTRY_APPENDED: "replication", LOG_TRUNCATED: "replication", ENTRY_COMMITTED: "replication", ENTRY_APPLIED: "replication",
  SNAPSHOT_CREATED: "snapshot", SNAPSHOT_SENT: "snapshot", SNAPSHOT_INSTALLED: "snapshot",
  NODE_STARTED: "lifecycle", RECOVERY_COMPLETED: "lifecycle", NODE_STOPPED: "lifecycle", NODE_FAILED: "lifecycle",
  NODE_CRASHED: "fault", NODE_RESTARTED: "fault", NODE_DISCONNECTED: "fault", NODE_RECOVERED: "fault", FAULT_INJECTED: "fault", FAULT_HEALED: "fault",
  MESSAGE_SENT: "network", MESSAGE_DELIVERED: "network", MESSAGE_DROPPED: "network",
  CLIENT_REQUEST: "client", CLIENT_RESPONSE: "client",
};

export const groupOf = (type: string): EventGroup => GROUPS[type] ?? "lifecycle";

export const GROUP_LABEL: Record<EventGroup, string> = {
  election: "Elections", replication: "Replication", snapshot: "Snapshots", lifecycle: "Lifecycle", fault: "Faults",
  client: "Clients", network: "Messages",
};

/** Events that matter for understanding a run; the rest are high-volume detail. */
export const KEY_EVENTS = new Set([
  "LEADER_ELECTED", "STEPPED_DOWN", "ELECTION_STARTED", "ELECTION_TIMEOUT", "LOG_TRUNCATED", "NODE_FAILED",
  "RECOVERY_COMPLETED", "SNAPSHOT_CREATED", "SNAPSHOT_INSTALLED", "NODE_CRASHED", "NODE_RESTARTED", "NODE_DISCONNECTED",
  "NODE_RECOVERED", "FAULT_INJECTED", "FAULT_HEALED", "VOTE_DENIED", "TERM_ADVANCED",
]);

export const label = (type: string) => type.toLowerCase().replace(/_/g, " ").replace(/^\w/, (c) => c.toUpperCase());

/** Tone for an event marker: critical for failures, accent for leadership, muted otherwise. */
export function eventTone(type: string): "critical" | "accent" | "good" | "muted" {
  if (["NODE_FAILED", "NODE_CRASHED", "FAULT_INJECTED", "NODE_DISCONNECTED", "LOG_TRUNCATED", "MESSAGE_DROPPED"].includes(type)) return "critical";
  if (["LEADER_ELECTED", "STEPPED_DOWN"].includes(type)) return "accent";
  if (["FAULT_HEALED", "NODE_RECOVERED", "RECOVERY_COMPLETED", "NODE_RESTARTED", "SNAPSHOT_INSTALLED"].includes(type)) return "good";
  return "muted";
}

export function describe(e: { type: string; term: number; index: number; peer: string | null; detail: string | null }) {
  const parts: string[] = [];
  if (e.index > 0 && groupOf(e.type) === "replication") parts.push(`index ${e.index}`);
  if (e.peer && !["FAULT_INJECTED", "FAULT_HEALED"].includes(e.type)) parts.push(`peer ${e.peer}`);
  if (e.detail) parts.push(e.detail);
  return parts.join(" · ");
}
