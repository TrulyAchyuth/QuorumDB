package io.quorumdb.faultlab.sim;

import io.quorumdb.raft.RaftMessage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Simulated message network. Every message gets an independent random latency (so reordering
 * happens naturally) and may be dropped, delayed or duplicated according to the active faults.
 * All randomness comes from the single seeded generator, in a fixed order.
 */
public final class SimNetwork {

    /** Why a message did not arrive (null = delivered). */
    public enum Fate { DELIVER, DROP_PARTITION, DROP_RANDOM, DROP_NODE_DOWN }

    private final Random random;
    private final long latencyMinMs;
    private final long latencyMaxMs;

    /** Partition group per node; nodes in different groups cannot talk. Empty = fully connected. */
    private final Map<String, Integer> groups = new HashMap<>();
    private final Set<String> isolated = new HashSet<>();
    private final Map<String, Long> extraDelayMs = new HashMap<>();
    private final Map<String, Double> dropProbability = new HashMap<>();
    private double duplicateProbability;

    public SimNetwork(Random random, long latencyMinMs, long latencyMaxMs) {
        this.random = random;
        this.latencyMinMs = latencyMinMs;
        this.latencyMaxMs = Math.max(latencyMinMs, latencyMaxMs);
    }

    public boolean connected(String a, String b) {
        if (isolated.contains(a) || isolated.contains(b)) return false;
        if (groups.isEmpty()) return true;
        // Nodes not named in any group form one implicit group together.
        return groups.getOrDefault(a, -1).equals(groups.getOrDefault(b, -1));
    }

    /** Decides at send time whether the message is lost. Consumes randomness only when a drop rate is set. */
    public Fate fateOnSend(String from, String to) {
        if (!connected(from, to)) return Fate.DROP_PARTITION;
        double p = Math.max(dropProbability.getOrDefault(from, 0.0), dropProbability.getOrDefault(to, 0.0));
        if (p > 0 && random.nextDouble() < p) return Fate.DROP_RANDOM;
        return Fate.DELIVER;
    }

    public long latencyMs(String from, String to) {
        long base = latencyMinMs + (latencyMaxMs > latencyMinMs ? random.nextInt((int) (latencyMaxMs - latencyMinMs + 1)) : 0);
        return base + Math.max(extraDelayMs.getOrDefault(from, 0L), extraDelayMs.getOrDefault(to, 0L));
    }

    public boolean shouldDuplicate() {
        return duplicateProbability > 0 && random.nextDouble() < duplicateProbability;
    }

    // ---- fault controls ----

    public void partition(List<List<String>> partitionGroups) {
        groups.clear();
        for (int g = 0; g < partitionGroups.size(); g++) {
            for (String node : partitionGroups.get(g)) groups.put(node, g);
        }
    }

    public void healPartition() {
        groups.clear();
    }

    public void isolate(String node, boolean on) {
        if (on) isolated.add(node); else isolated.remove(node);
    }

    public void setExtraDelay(String node, long delayMs) {
        if (delayMs <= 0) extraDelayMs.remove(node); else extraDelayMs.put(node, delayMs);
    }

    public void setDropProbability(String node, double probability) {
        if (probability <= 0) dropProbability.remove(node); else dropProbability.put(node, Math.min(probability, 1.0));
    }

    public void setDuplicateProbability(double probability) {
        this.duplicateProbability = probability;
    }

    public void healAll() {
        groups.clear();
        isolated.clear();
        extraDelayMs.clear();
        dropProbability.clear();
    }

    public boolean hasActiveFaults() {
        return !groups.isEmpty() || !isolated.isEmpty() || !extraDelayMs.isEmpty() || !dropProbability.isEmpty();
    }

    /** Compact one-line description of a message for timeline events. */
    public static String describe(RaftMessage m) {
        return switch (m) {
            case RaftMessage.RequestVote v -> (v.preVote() ? "PreVote" : "RequestVote") + " term=" + v.term()
                    + " lastLog=" + v.lastLogIndex() + "@" + v.lastLogTerm();
            case RaftMessage.RequestVoteReply v -> (v.preVote() ? "PreVoteReply" : "RequestVoteReply")
                    + " term=" + v.term() + " granted=" + v.granted();
            case RaftMessage.AppendEntries a -> (a.entries().isEmpty() ? "Heartbeat" : "AppendEntries") + " term=" + a.term()
                    + " prev=" + a.prevLogIndex() + "@" + a.prevLogTerm() + " entries=" + a.entries().size()
                    + " commit=" + a.leaderCommit();
            case RaftMessage.AppendEntriesReply r -> "AppendEntriesReply term=" + r.term() + " success=" + r.success()
                    + (r.success() ? " match=" + r.matchIndex() : " conflict=" + r.conflictIndex() + "@" + r.conflictTerm());
            case RaftMessage.InstallSnapshot s -> "InstallSnapshot term=" + s.term() + " through="
                    + s.lastIncludedIndex() + "@" + s.lastIncludedTerm() + " offset=" + s.offset()
                    + " bytes=" + s.data().length + (s.more() ? " more" : "");
            case RaftMessage.InstallSnapshotReply r -> "InstallSnapshotReply term=" + r.term() + " through="
                    + r.lastIncludedIndex() + " next=" + r.nextOffset();
        };
    }
}
