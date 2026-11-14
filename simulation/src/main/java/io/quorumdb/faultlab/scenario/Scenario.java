package io.quorumdb.faultlab.scenario;

import java.util.ArrayList;
import java.util.List;

/**
 * A reproducible failure experiment: cluster shape, client workload and a schedule of faults.
 * Together with {@code seed} it fully determines a simulated run. Every field except the fault
 * list has a default, so a scenario can be as small as {@code {"faults":[...]}}.
 */
public record Scenario(String name, String description, Integer nodes, Long seed, Long durationMs,
                       Cluster cluster, Workload workload, List<FaultStep> faults) {

    /** Raft and network tuning. Null fields take defaults. */
    public record Cluster(Long electionTimeoutMinMs, Long electionTimeoutMaxMs, Long heartbeatIntervalMs,
                          Boolean preVote, Boolean checkQuorum, Long snapshotThreshold,
                          Long netLatencyMinMs, Long netLatencyMaxMs) {
        public static final Cluster DEFAULT = new Cluster(null, null, null, null, null, null, null, null);

        public Cluster normalized() {
            return new Cluster(or(electionTimeoutMinMs, 150L), or(electionTimeoutMaxMs, 300L),
                    or(heartbeatIntervalMs, 50L), or(preVote, true), or(checkQuorum, true),
                    or(snapshotThreshold, 0L), or(netLatencyMinMs, 1L), or(netLatencyMaxMs, 5L));
        }
    }

    public enum ReadMode {
        /** ReadIndex: leader confirms leadership with a majority before answering. */
        LINEARIZABLE,
        /** Answer from a node that believes it is leader, without confirmation (can be stale in a partition). */
        LEADER_LOCAL,
        /** Answer from any node's local state (can be arbitrarily stale). */
        STALE
    }

    /** Simulated clients. {@code clients = 0} disables the workload. */
    public record Workload(Integer clients, Long opIntervalMs, Integer keys, Double readRatio, Double deleteRatio,
                           Long opTimeoutMs, ReadMode readMode) {
        public static final Workload DEFAULT = new Workload(null, null, null, null, null, null, null);

        public Workload normalized() {
            return new Workload(or(clients, 3), or(opIntervalMs, 40L), or(keys, 5), or(readRatio, 0.5),
                    or(deleteRatio, 0.05), or(opTimeoutMs, 1500L), or(readMode, ReadMode.LINEARIZABLE));
        }
    }

    public enum FaultType {
        /** Kill the process; restarted when the fault ends. */
        CRASH,
        /** Kill and immediately restart the process. */
        RESTART,
        /** Cut the node off from every other node. */
        ISOLATE,
        /** Split the cluster into groups that cannot talk to each other. */
        PARTITION,
        /** Add latency to every message to/from the node. */
        DELAY,
        /** Drop each message to/from the node with a probability. */
        DROP,
        /** Make the node's disk reject writes; the node fail-stops and is restarted when the fault ends. */
        STORAGE_FAILURE,
        /** Destroy the node's disk and restart it empty. Violates Raft's durability assumption on purpose. */
        DISK_LOSS,
        /** Remove every network fault. */
        HEAL
    }

    public enum Condition { LEADER_ELECTED, COMMIT_INDEX_AT_LEAST, OPS_COMPLETED_AT_LEAST }

    /**
     * When something starts or ends: either at an absolute time, or when a condition first holds
     * (optionally {@code afterMs} later).
     */
    public record Trigger(Long atMs, Condition when, Long value, Long afterMs) {
        public static Trigger at(long ms) {
            return new Trigger(ms, null, null, null);
        }

        public static Trigger when(Condition condition, long value, long afterMs) {
            return new Trigger(null, condition, value, afterMs);
        }
    }

    /**
     * One fault.
     *
     * @param targets    node ids ("n1") or selectors: "leader", "follower", "random"
     * @param groups     for PARTITION: the groups; may be omitted to split {@code targets} from the rest
     * @param start      when to inject
     * @param end        when to heal (alternative to {@code durationMs}); neither = lasts until the run ends
     */
    public record FaultStep(FaultType type, List<String> targets, List<List<String>> groups, Trigger start,
                            Trigger end, Long durationMs, Long delayMs, Double dropProbability) {

        public static FaultStep of(FaultType type, long atMs, Long durationMs, String... targets) {
            return new FaultStep(type, List.of(targets), null, Trigger.at(atMs), null, durationMs, null, null);
        }

        public FaultStep withDelay(long ms) {
            return new FaultStep(type, targets, groups, start, end, durationMs, ms, dropProbability);
        }

        public FaultStep withDrop(double probability) {
            return new FaultStep(type, targets, groups, start, end, durationMs, delayMs, probability);
        }

        public FaultStep withGroups(List<List<String>> newGroups) {
            return new FaultStep(type, targets, newGroups, start, end, durationMs, delayMs, dropProbability);
        }
    }

    /** Fills in defaults and validates. @throws IllegalArgumentException describing the first problem */
    public Scenario normalized() {
        int n = or(nodes, 3);
        long duration = or(durationMs, 10_000L);
        Cluster c = (cluster == null ? Cluster.DEFAULT : cluster).normalized();
        Workload w = (workload == null ? Workload.DEFAULT : workload).normalized();
        List<FaultStep> steps = faults == null ? List.of() : List.copyOf(faults);

        require(n >= 1 && n <= 9, "nodes must be between 1 and 9");
        require(duration >= 100 && duration <= 600_000, "durationMs must be between 100 and 600000");
        require(c.electionTimeoutMinMs() >= 10 && c.electionTimeoutMaxMs() > c.electionTimeoutMinMs(),
                "election timeout must satisfy 10 <= min < max");
        require(c.heartbeatIntervalMs() >= 1 && c.heartbeatIntervalMs() * 2 <= c.electionTimeoutMinMs(),
                "heartbeatIntervalMs must be at most half of electionTimeoutMinMs");
        require(c.netLatencyMinMs() >= 0 && c.netLatencyMaxMs() >= c.netLatencyMinMs() && c.netLatencyMaxMs() <= 10_000,
                "network latency must satisfy 0 <= min <= max <= 10000");
        require(c.snapshotThreshold() >= 0, "snapshotThreshold must not be negative");
        require(w.clients() >= 0 && w.clients() <= 32, "clients must be between 0 and 32");
        require(w.opIntervalMs() >= 1, "opIntervalMs must be at least 1");
        require(w.keys() >= 1 && w.keys() <= 1000, "keys must be between 1 and 1000");
        require(w.readRatio() >= 0 && w.readRatio() <= 1 && w.deleteRatio() >= 0 && w.readRatio() + w.deleteRatio() <= 1,
                "readRatio and deleteRatio must be probabilities summing to at most 1");
        require(w.opTimeoutMs() >= 10, "opTimeoutMs must be at least 10");
        require(steps.size() <= 64, "at most 64 fault steps");

        List<String> ids = new ArrayList<>();
        for (int i = 1; i <= n; i++) ids.add("n" + i);
        for (int i = 0; i < steps.size(); i++) {
            FaultStep f = steps.get(i);
            String where = "faults[" + i + "]: ";
            require(f != null && f.type() != null, where + "type is required");
            require(f.start() != null && (f.start().atMs() != null || f.start().when() != null),
                    where + "start needs atMs or when");
            require(f.start().atMs() == null || (f.start().atMs() >= 0 && f.start().atMs() <= duration),
                    where + "start.atMs must be within the run");
            require(f.durationMs() == null || f.durationMs() >= 0, where + "durationMs must not be negative");
            require(f.durationMs() == null || f.end() == null, where + "give either durationMs or end, not both");
            if (f.targets() != null) {
                for (String t : f.targets()) {
                    require(ids.contains(t) || List.of("leader", "follower", "random").contains(t),
                            where + "unknown target '" + t + "'");
                }
            }
            switch (f.type()) {
                case HEAL -> { }
                case PARTITION -> {
                    require((f.groups() != null && f.groups().size() >= 2) || (f.targets() != null && !f.targets().isEmpty()),
                            where + "PARTITION needs groups (two or more) or targets");
                    if (f.groups() != null) {
                        List<String> seen = new ArrayList<>();
                        for (List<String> g : f.groups()) {
                            for (String t : g) {
                                require(ids.contains(t) || t.equals("leader"), where + "unknown node '" + t + "' in groups");
                                require(!seen.contains(t), where + "node '" + t + "' is in two groups");
                                seen.add(t);
                            }
                        }
                    }
                }
                case DELAY -> {
                    require(f.targets() != null && !f.targets().isEmpty(), where + "targets are required");
                    require(f.delayMs() != null && f.delayMs() > 0 && f.delayMs() <= 60_000, where + "DELAY needs delayMs in (0, 60000]");
                }
                case DROP -> {
                    require(f.targets() != null && !f.targets().isEmpty(), where + "targets are required");
                    require(f.dropProbability() != null && f.dropProbability() > 0 && f.dropProbability() <= 1,
                            where + "DROP needs dropProbability in (0, 1]");
                }
                default -> require(f.targets() != null && !f.targets().isEmpty(), where + "targets are required");
            }
        }
        return new Scenario(name == null || name.isBlank() ? "Untitled scenario" : name, description == null ? "" : description,
                n, or(seed, 1L), duration, c, w, steps);
    }

    public Scenario withSeed(long newSeed) {
        return new Scenario(name, description, nodes, newSeed, durationMs, cluster, workload, faults);
    }

    public Scenario withCluster(Cluster newCluster) {
        return new Scenario(name, description, nodes, seed, durationMs, newCluster, workload, faults);
    }

    public Scenario withName(String newName) {
        return new Scenario(newName, description, nodes, seed, durationMs, cluster, workload, faults);
    }

    private static <T> T or(T value, T fallback) {
        return value == null ? fallback : value;
    }

    private static void require(boolean ok, String message) {
        if (!ok) throw new IllegalArgumentException(message);
    }
}
