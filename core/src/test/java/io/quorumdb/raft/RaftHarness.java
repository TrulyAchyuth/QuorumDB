package io.quorumdb.raft;

import io.quorumdb.raft.Collaborators.Cancellable;
import io.quorumdb.raft.Collaborators.Scheduler;
import io.quorumdb.raft.Collaborators.StateMachine;
import io.quorumdb.storage.FaultyStorage;
import io.quorumdb.storage.MemoryStorage;

import java.util.ArrayList;
import java.util.List;

/**
 * Drives a single {@link RaftNode} by hand: the test advances a manual clock, injects messages and
 * inspects what the node tried to send. Election timeouts are made exact (jitter = 0).
 */
final class RaftHarness {
    static final long ELECTION_TIMEOUT = 100;
    static final long HEARTBEAT = 20;

    record Sent(String to, RaftMessage message) {}

    private static final class Timer {
        long at;
        Runnable task;
        boolean cancelled;
    }

    final MemoryStorage disk;
    final FaultyStorage storage;
    final List<Sent> outbox = new ArrayList<>();
    final List<RaftEvent> events = new ArrayList<>();
    final List<String> applied = new ArrayList<>();
    final List<byte[]> restoredSnapshots = new ArrayList<>();
    private final List<Timer> timers = new ArrayList<>();
    private long now;
    final RaftNode node;

    private final Scheduler scheduler = new Scheduler() {
        @Override
        public long nowMs() {
            return now;
        }

        @Override
        public Cancellable schedule(long delayMs, Runnable task) {
            Timer t = new Timer();
            t.at = now + delayMs;
            t.task = task;
            timers.add(t);
            return () -> t.cancelled = true;
        }
    };

    /** State machine that records applied commands as strings; its snapshot is the joined list. */
    private final StateMachine machine = new StateMachine() {
        @Override
        public byte[] apply(LogEntry entry) {
            applied.add(new String(entry.data()));
            return ("result:" + new String(entry.data())).getBytes();
        }

        @Override
        public byte[] snapshot() {
            return String.join(",", applied).getBytes();
        }

        @Override
        public void restore(byte[] snapshot) {
            restoredSnapshots.add(snapshot);
            applied.clear();
            String s = new String(snapshot);
            if (!s.isEmpty()) applied.addAll(List.of(s.split(",")));
        }
    };

    RaftHarness(int clusterSize, boolean preVote, boolean checkQuorum, long snapshotThreshold) {
        this(clusterSize, preVote, checkQuorum, snapshotThreshold, new MemoryStorage());
    }

    RaftHarness(int clusterSize, boolean preVote, boolean checkQuorum, long snapshotThreshold, MemoryStorage disk) {
        this(clusterSize, preVote, checkQuorum, snapshotThreshold, disk, false);
    }

    /** With {@code joining}, n1 starts as a node being added to the cluster n2..nN (not yet a voter). */
    RaftHarness(int clusterSize, boolean preVote, boolean checkQuorum, long snapshotThreshold, MemoryStorage disk,
                boolean joining) {
        this.disk = disk;
        this.storage = new FaultyStorage(disk);
        List<String> peers = new ArrayList<>();
        for (int i = 2; i <= clusterSize; i++) peers.add("n" + i);
        RaftConfig config = new RaftConfig("n1", peers, ELECTION_TIMEOUT, ELECTION_TIMEOUT * 2, HEARTBEAT, 100, 1 << 20,
                snapshotThreshold, 100, preVote, checkQuorum, true, joining);
        node = new RaftNode(config, storage, machine, (to, m) -> outbox.add(new Sent(to, m)), scheduler, bound -> 0,
                events::add);
        node.start();
    }

    /** Classic Raft: three nodes, no PreVote, no CheckQuorum. */
    static RaftHarness classic() {
        return new RaftHarness(3, false, false, 0);
    }

    void advance(long ms) {
        long target = now + ms;
        while (true) {
            Timer next = null;
            for (Timer t : timers) {
                if (!t.cancelled && t.at <= target && (next == null || t.at < next.at)) next = t;
            }
            if (next == null) break;
            timers.remove(next);
            now = next.at;
            next.task.run();
        }
        now = target;
    }

    void receive(RaftMessage message) {
        node.handle(message);
    }

    /** Makes this node the leader of term 1 (or term+1) with a vote from n2. Clears the outbox. */
    void electLeader() {
        advance(ELECTION_TIMEOUT);
        long term = node.currentTerm() + (node.role() == RaftNode.Role.PRE_CANDIDATE ? 1 : 0);
        if (node.role() == RaftNode.Role.PRE_CANDIDATE) {
            receive(new RaftMessage.RequestVoteReply("n2", node.currentTerm(), term, true, true));
        }
        receive(new RaftMessage.RequestVoteReply("n2", term, term, true, false));
        if (node.role() != RaftNode.Role.LEADER) throw new AssertionError("expected to be leader, was " + node.role());
        outbox.clear();
    }

    /** Acknowledges replication up to {@code matchIndex} from a follower, echoing the latest read sequence. */
    void ack(String follower, long matchIndex) {
        long readSeq = 0;
        for (Sent s : outbox) {
            if (s.message() instanceof RaftMessage.AppendEntries a) readSeq = Math.max(readSeq, a.readSeq());
        }
        receive(new RaftMessage.AppendEntriesReply(follower, node.currentTerm(), true, matchIndex, 0, 0, readSeq));
    }

    <T extends RaftMessage> List<T> sent(Class<T> type) {
        List<T> out = new ArrayList<>();
        for (Sent s : outbox) if (type.isInstance(s.message())) out.add(type.cast(s.message()));
        return out;
    }

    <T extends RaftMessage> T lastSent(Class<T> type, String to) {
        for (int i = outbox.size() - 1; i >= 0; i--) {
            Sent s = outbox.get(i);
            if (s.to().equals(to) && type.isInstance(s.message())) return type.cast(s.message());
        }
        throw new AssertionError("no " + type.getSimpleName() + " sent to " + to + "; outbox=" + outbox);
    }

    boolean saw(RaftEvent.Type type) {
        return events.stream().anyMatch(e -> e.type() == type);
    }

    static LogEntry entry(long index, long term, String command) {
        return new LogEntry(index, term, LogEntry.Type.COMMAND, command.getBytes());
    }

    static RaftMessage.AppendEntries append(String leader, long term, long prevIndex, long prevTerm, long commit,
                                             LogEntry... entries) {
        return new RaftMessage.AppendEntries(leader, term, prevIndex, prevTerm, List.of(entries), commit, 0);
    }
}
