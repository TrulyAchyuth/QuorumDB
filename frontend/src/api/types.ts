// Mirrors the backend's JSON. Field names match the Java records exactly.

export interface Follower { nextIndex: number; matchIndex: number; lag: number; lastAckAgoMs: number }
export interface Link {
  peer: string; address: string; sent: number; failed: number; dropped: number; received: number; inFlight: number;
  lastSuccessAgoMs: number; lastReceivedAgoMs: number; lastError: string | null; faultBlocked: boolean;
}
export interface NodeView {
  id: string; httpUrl: string; raftAddress: string; reachable: boolean;
  health: "UP" | "DEGRADED" | "DOWN" | "UNREACHABLE"; role: string; term: number; leaderId: string | null;
  votedFor: string | null; commitIndex: number; lastApplied: number; lastLogIndex: number; lastLogTerm: number;
  snapshotIndex: number; pendingProposals: number; failure: string | null; uptimeMs: number;
  followers: Record<string, Follower>; links: Record<string, Link>;
  faults: { blockedPeers: string[]; delayMs: number; dropProbability: number; storageFailing: boolean; crashed: boolean; active: string[] } | null;
  storage: { diskBytes: number; snapshotIndex: number; lastIndex: number; keys: number; fsync: boolean } | null;
  observedAtMs: number;
}
export interface ClusterSummary {
  clusterId: string; health: "HEALTHY" | "DEGRADED" | "UNAVAILABLE"; size: number; quorum: number; leaderId: string | null;
  term: number; commitIndex: number; appliedIndex: number; healthyNodes: number; unavailableNodes: number;
  maxReplicationLag: number; observedBy: string; observedAtMs: number; nodes: NodeView[];
}
export interface RaftEvent {
  seq: number; timeMs: number; node: string; type: string; term: number; index: number; entryTerm: number;
  checksum: number; peer: string | null; detail: string | null;
}
export interface Window { totalOk: number; totalErrors: number; opsPerSecond: number; p50Ms: number; p95Ms: number; p99Ms: number; maxMs: number; samples: number }
export interface Point { second: number; writes: number; reads: number; errors: number }
export interface Metrics {
  node: string; windowSeconds: number; requests: { write: Window; read: Window }; perSecond: Point[];
  term: number; commitIndex: number; appliedIndex: number; lastLogIndex: number; pendingProposals: number; queuedTasks: number;
  diskBytes: number; heapUsedBytes: number; heapMaxBytes: number; processCpuLoad: number; availableProcessors: number;
}
export interface ClusterMetrics { perSecond: Point[]; nodes: Record<string, Metrics>; unreachable: string[] }
export interface Operation {
  id: number; timeMs: number; type: string; key: string; consistency: string | null; status: string; latencyMicros: number;
  logIndex: number; term: number; readIndex: number; committed: boolean; servedBy: string; clientId: string | null;
}
export interface Config {
  nodeId: string; clusterId: string; members: { id: string; raftAddress: string; httpBaseUrl: string }[]; grpcPort: number;
  electionTimeoutMinMs: number; electionTimeoutMaxMs: number; heartbeatIntervalMs: number; snapshotThreshold: number;
  preVote: boolean; checkQuorum: boolean; fsync: boolean; requestTimeoutMs: number; maxKeyBytes: number; maxValueBytes: number;
  faultInjectionEnabled: boolean; eventBufferSize: number; dataDir: string;
}
export interface ApiErrorBody { error: string; message: string; node?: string; leaderHint?: string | null }

// ---- FaultLab ----
export type FaultType = "CRASH" | "RESTART" | "ISOLATE" | "PARTITION" | "DELAY" | "DROP" | "STORAGE_FAILURE" | "DISK_LOSS" | "HEAL";
export interface Trigger { atMs?: number | null; when?: string | null; value?: number | null; afterMs?: number | null }
export interface FaultStep {
  type: FaultType; targets?: string[] | null; groups?: string[][] | null; start: Trigger; end?: Trigger | null;
  durationMs?: number | null; delayMs?: number | null; dropProbability?: number | null;
}
export interface Scenario {
  name: string; description: string; nodes: number; seed: number; durationMs: number;
  cluster: { electionTimeoutMinMs: number; electionTimeoutMaxMs: number; heartbeatIntervalMs: number; preVote: boolean;
    checkQuorum: boolean; snapshotThreshold: number; netLatencyMinMs: number; netLatencyMaxMs: number };
  workload: { clients: number; opIntervalMs: number; keys: number; readRatio: number; deleteRatio: number; opTimeoutMs: number;
    readMode: "LINEARIZABLE" | "LEADER_LOCAL" | "STALE" };
  faults: FaultStep[];
}
export interface RunSummary {
  id: string; mode: "SIMULATION" | "LIVE"; name: string; createdAtMs: number; headline: string; safety: string;
  linearizability: string; liveness: string; nodes: number; seed: number; durationMs: number; opsTotal: number;
  availability: number; elections: number; faults: number; events: number; traceHash: string; replayOf: string | null;
}
export interface Violation { message: string; timeMs: number; nodes: string[]; index: number; term: number; evidence: number[] }
export interface SafetyCheck { id: string; name: string; property: string; status: string; examined: number; examinedWhat: string; violations: Violation[] }
export interface SafetyReport { verdict: string; checks: SafetyCheck[]; assumptionBreaches: string[]; observationGaps: string[] }
export interface Finding { kind: string; timeMs: number; title: string; detail: string; evidence: number[] }
export interface FaultRecord {
  step: number; type: string; targets: string[]; description: string; injectedAtMs: number; healedAtMs: number;
  leaderAtInjection: string | null; termAtInjection: number; commitAtInjection: number; hitLeader: boolean;
}
export interface Sample { timeMs: number; commitIndex: number; term: number; leader: string | null; nodesUp: number; replicationLag: number; opsOk: number; opsNotOk: number }
export interface RunMetrics {
  durationMs: number; nodes: number; opsTotal: number; opsOk: number; opsFailed: number; opsIndeterminate: number; availability: number;
  failedDuringFaults: number; failedOutsideFaults: number; latencyP50Ms: number; latencyP95Ms: number; latencyP99Ms: number;
  throughputOpsPerSec: number;
  elections: { term: number; leader: string; leaderlessSinceMs: number; firstTimeoutMs: number; electedAtMs: number; durationMs: number }[];
  meanElectionMs: number; maxElectionMs: number;
  recoveries: { step: number; fault: string; injectedAtMs: number; newLeaderAfterMs: number; firstCommitAfterMs: number; catchUpMs: number }[];
  meanRecoveryMs: number; maxRecoveryMs: number; finalCommitIndex: number; finalTerm: number; electionsStarted: number;
  leaderChanges: number; logTruncations: number; snapshotsCreated: number; snapshotsInstalled: number;
  meanReplicationLag: number; maxReplicationLag: number; messagesSent: number; messagesDelivered: number; messagesDropped: number;
  timeline: Sample[];
}
export interface NodeEnd { node: string; up: boolean; role: string; term: number; lastIndex: number; commitIndex: number; lastApplied: number; snapshotIndex: number; keys: number; stateDigest: number }
export interface LinReport { result: string; operationsChecked: number; keysChecked: number; keys: { key: string; result: string; operations: number; explanation: string | null; problemOps: number[] }[] }
export interface RunDetail {
  summary: RunSummary; scenario: Scenario; faults: FaultRecord[]; safety: SafetyReport; linearizability: LinReport | null;
  liveness: { status: string; quorumAvailableAtEnd: boolean; leaderAtEnd: string | null; converged: boolean; findings: string[] };
  metrics: RunMetrics; explanation: Finding[]; finalState: NodeEnd[]; nodeIds: string[];
}
export interface EventPage { total: number; offset: number; events: RaftEvent[] }
export interface Comparison {
  aName: string; bName: string; sameScenario: boolean; identicalTrace: boolean;
  rows: { metric: string; unit: string; a: string; b: string; delta: string; better: string }[]; observations: string[];
}
export interface Replay { originalId: string; replayId: string; mode: string; identical: boolean; originalHash: string; replayHash: string; originalEvents: number; replayEvents: number; note: string }
export interface LiveStatus { state: string; scenario: string | null; startedAtMs: number; elapsedMs: number; durationMs: number; opsCompleted: number; faultsInjected: number; runId: string | null; error: string | null }
export interface ActiveFault { directive: { faultId: string; action: string; peers: string[]; delayMs: number; dropProbability: number; expiresAtMs: number; description: string }; appliedAtMs: number }
