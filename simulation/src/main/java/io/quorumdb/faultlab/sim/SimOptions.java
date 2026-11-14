package io.quorumdb.faultlab.sim;

/**
 * Cluster and network parameters of a simulation.
 *
 * @param recordMessages whether per-message SENT/DELIVERED/DROPPED events are recorded (large traces)
 * @param observeIntervalMs how often the safety checker samples every node's log and state
 */
public record SimOptions(int nodes, long seed, long electionTimeoutMinMs, long electionTimeoutMaxMs,
                         long heartbeatIntervalMs, boolean preVote, boolean checkQuorum, long snapshotThreshold,
                         long netLatencyMinMs, long netLatencyMaxMs, boolean recordMessages,
                         long observeIntervalMs) {

    public SimOptions {
        if (nodes < 1 || nodes > 9) throw new IllegalArgumentException("nodes must be between 1 and 9");
        if (netLatencyMinMs < 0 || netLatencyMaxMs < netLatencyMinMs) {
            throw new IllegalArgumentException("invalid network latency range");
        }
        if (observeIntervalMs < 1) throw new IllegalArgumentException("observeIntervalMs must be positive");
    }

    public static SimOptions defaults(int nodes, long seed) {
        return new SimOptions(nodes, seed, 150, 300, 50, true, true, 0, 1, 5, false, 100);
    }

    public SimOptions withRecordMessages(boolean value) {
        return new SimOptions(nodes, seed, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, preVote,
                checkQuorum, snapshotThreshold, netLatencyMinMs, netLatencyMaxMs, value, observeIntervalMs);
    }

    public SimOptions withSnapshotThreshold(long value) {
        return new SimOptions(nodes, seed, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, preVote,
                checkQuorum, value, netLatencyMinMs, netLatencyMaxMs, recordMessages, observeIntervalMs);
    }

    public SimOptions withPreVote(boolean value) {
        return new SimOptions(nodes, seed, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, value,
                checkQuorum, snapshotThreshold, netLatencyMinMs, netLatencyMaxMs, recordMessages, observeIntervalMs);
    }

    public SimOptions withCheckQuorum(boolean value) {
        return new SimOptions(nodes, seed, electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, preVote,
                value, snapshotThreshold, netLatencyMinMs, netLatencyMaxMs, recordMessages, observeIntervalMs);
    }
}
