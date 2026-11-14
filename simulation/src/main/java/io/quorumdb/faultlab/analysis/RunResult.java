package io.quorumdb.faultlab.analysis;

import io.quorumdb.faultlab.check.LinearizabilityChecker;
import io.quorumdb.faultlab.check.SafetyReport;
import io.quorumdb.faultlab.scenario.Scenario;
import io.quorumdb.raft.RaftEvent;

import java.util.List;

/**
 * Everything recorded and concluded about one FaultLab run.
 *
 * @param mode      SIMULATION (deterministic, replayable) or LIVE (real cluster, wall clock, not replayable)
 * @param traceHash SHA-256 over the full ordered event trace; two simulated runs of the same scenario and
 *                  seed must produce the same hash
 * @param headline  one-word classification, see {@link Headline}
 */
public record RunResult(Mode mode, Scenario scenario, String traceHash, List<RaftEvent> events,
                        List<FaultRecord> faults, SafetyReport safety, LinearizabilityChecker.Report linearizability,
                        Liveness liveness, RunMetrics metrics, List<Finding> explanation, Headline headline,
                        List<NodeEnd> finalState) {

    public enum Mode { SIMULATION, LIVE }

    public enum Headline {
        /** Safe, live, and every request outside fault windows succeeded. */
        HEALTHY,
        /** Safe; requests failed only while faults made a quorum or leader unavailable. This is correct behaviour. */
        EXPECTED_UNAVAILABILITY,
        /** Safe, but the cluster failed to make progress when it should have been able to. */
        LIVENESS_ISSUE,
        /** A safety property or linearizability was violated. */
        SAFETY_VIOLATION,
        /** Observations were incomplete; no conclusion can be drawn. */
        INCONCLUSIVE
    }

    /**
     * A fault as it actually played out.
     *
     * @param healedAtMs       -1 if it was still active when the run ended
     * @param leaderAtInjection leader when the fault was injected (null if none)
     * @param hitLeader        whether the fault took the leader away from the majority
     */
    public record FaultRecord(int step, Scenario.FaultType type, List<String> targets, String description,
                              long injectedAtMs, long healedAtMs, String leaderAtInjection, long termAtInjection,
                              long commitAtInjection, boolean hitLeader) {}

    /**
     * Liveness assessment, kept apart from safety on purpose.
     *
     * @param quorumAvailableAtEnd whether a majority of nodes was up and mutually connected when the run ended
     * @param converged            whether all connected nodes had applied the same prefix at the end
     */
    public record Liveness(Status status, boolean quorumAvailableAtEnd, String leaderAtEnd, boolean converged,
                           List<String> findings) {
        public enum Status { OK, EXPECTED_UNAVAILABILITY, ISSUE, UNKNOWN }
    }

    /** A statement about the run, backed by the sequence numbers of the events it was derived from. */
    public record Finding(String kind, long timeMs, String title, String detail, List<Long> evidence) {}

    public record NodeEnd(String node, boolean up, String role, long term, long lastIndex, long commitIndex,
                          long lastApplied, long snapshotIndex, int keys, long stateDigest) {}
}
