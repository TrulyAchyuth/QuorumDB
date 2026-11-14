package io.quorumdb.faultlab.analysis;

import java.util.List;

/**
 * Quantitative summary of a run. In simulations all times are virtual milliseconds.
 *
 * @param availability       fraction of client operations that completed successfully
 * @param failedDuringFaults non-successful operations that overlapped a fault window (or its recovery grace)
 * @param failedOutsideFaults non-successful operations with no fault to blame
 * @param elections          every successful election, with how long the cluster was without a leader
 * @param recoveries         per fault: how long until service was restored
 * @param meanReplicationLag mean over samples of (leader last index - slowest live follower match index)
 */
public record RunMetrics(long durationMs, int nodes,
                         long opsTotal, long opsOk, long opsFailed, long opsIndeterminate, double availability,
                         long failedDuringFaults, long failedOutsideFaults,
                         double latencyP50Ms, double latencyP95Ms, double latencyP99Ms, double throughputOpsPerSec,
                         List<Election> elections, double meanElectionMs, long maxElectionMs,
                         List<Recovery> recoveries, double meanRecoveryMs, long maxRecoveryMs,
                         long finalCommitIndex, long finalTerm, long electionsStarted, long leaderChanges,
                         long logTruncations, long snapshotsCreated, long snapshotsInstalled,
                         double meanReplicationLag, long maxReplicationLag,
                         long messagesSent, long messagesDelivered, long messagesDropped,
                         List<Sample> timeline) {

    /**
     * @param leaderlessSinceMs when the previous leader was lost (0 for the first election)
     * @param firstTimeoutMs    when the first node noticed (its election timer fired)
     * @param durationMs        electedAtMs - leaderlessSinceMs: the full leaderless window
     */
    public record Election(long term, String leader, long leaderlessSinceMs, long firstTimeoutMs, long electedAtMs,
                           long durationMs) {}

    /**
     * @param newLeaderAfterMs   time from injection until a new leader was elected (-1 if not applicable / never)
     * @param firstCommitAfterMs time from injection until the next entry was committed under the new leader (-1 = n/a)
     * @param catchUpMs          for restarted nodes: time from restart until it had applied everything that was
     *                           committed at the moment it restarted (-1 = n/a)
     */
    public record Recovery(int step, String fault, long injectedAtMs, long newLeaderAfterMs, long firstCommitAfterMs,
                           long catchUpMs) {}

    /** Periodic sample for charts. */
    public record Sample(long timeMs, long commitIndex, long term, String leader, int nodesUp, long replicationLag,
                         long opsOk, long opsNotOk) {}
}
