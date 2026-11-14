package io.quorumdb.faultlab.sim;

import io.quorumdb.faultlab.check.NodeObservation;
import io.quorumdb.kv.KvStateMachine;
import io.quorumdb.raft.Collaborators.Cancellable;
import io.quorumdb.raft.Collaborators.Scheduler;
import io.quorumdb.raft.LogEntry;
import io.quorumdb.raft.RaftConfig;
import io.quorumdb.raft.RaftEvent;
import io.quorumdb.raft.RaftMessage;
import io.quorumdb.raft.RaftNode;
import io.quorumdb.storage.FaultyStorage;
import io.quorumdb.storage.MemoryStorage;
import io.quorumdb.storage.RaftStorage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * A whole QuorumDB cluster running inside one thread on virtual time. The nodes are the real
 * {@link RaftNode} and {@link KvStateMachine} classes; only the clock, network and disk are simulated.
 *
 * <p>Crash semantics: a crash abandons the node object without running any shutdown logic (no
 * callbacks fire, timers and in-flight deliveries are silently discarded) and keeps the node's
 * {@link MemoryStorage} "disk". A restart builds a fresh node and state machine on that disk.
 */
public final class SimCluster {

    public static final String FAULTLAB = "faultlab";

    /** One simulated machine. */
    public final class SimNode {
        public final String id;
        public final MemoryStorage disk = new MemoryStorage();
        public final FaultyStorage storage = new FaultyStorage(disk);
        private RaftNode raft;
        private KvStateMachine kv;
        private int incarnation;
        private boolean up;

        SimNode(String id) {
            this.id = id;
        }

        public boolean isUp() { return up; }
        /** The node is up and has not fail-stopped. */
        public boolean isServing() { return up && raft.isRunning(); }
        public RaftNode raft() { return raft; }
        public KvStateMachine kv() { return kv; }
        public int incarnation() { return incarnation; }
    }

    private final SimOptions options;
    private final EventLoop loop = new EventLoop();
    private final Random random;
    private final SimNetwork network;
    private final Map<String, SimNode> nodes = new LinkedHashMap<>();
    private final List<RaftEvent> events = new ArrayList<>();
    private final List<Consumer<RaftEvent>> listeners = new ArrayList<>();
    private long eventSeq;
    private long messagesSent;
    private long messagesDelivered;
    private long messagesDropped;

    private final UnaryOperator<RaftStorage> storageDecorator;

    public SimCluster(SimOptions options) {
        this(options, UnaryOperator.identity());
    }

    /**
     * @param storageDecorator wraps each node's storage as seen by Raft. Tests use it to seed storage bugs
     *                         (e.g. a disk that forgets votes) and prove the safety checker notices.
     */
    public SimCluster(SimOptions options, UnaryOperator<RaftStorage> storageDecorator) {
        this.storageDecorator = storageDecorator;
        this.options = options;
        this.random = new Random(options.seed());
        this.network = new SimNetwork(random, options.netLatencyMinMs(), options.netLatencyMaxMs());
        for (int i = 1; i <= options.nodes(); i++) {
            nodes.put("n" + i, new SimNode("n" + i));
        }
    }

    // =====================================================================================
    // Node lifecycle
    // =====================================================================================

    public void startAll() {
        for (SimNode node : nodes.values()) boot(node);
    }

    private void boot(SimNode node) {
        node.incarnation++;
        node.up = true;
        node.kv = new KvStateMachine();
        int incarnation = node.incarnation;
        List<String> peers = nodes.keySet().stream().filter(p -> !p.equals(node.id)).toList();
        RaftConfig config = new RaftConfig(node.id, peers, options.electionTimeoutMinMs(),
                options.electionTimeoutMaxMs(), options.heartbeatIntervalMs(), 64, 1 << 20,
                options.snapshotThreshold(), 10_000, options.preVote(), options.checkQuorum(), true);
        Scheduler scheduler = new Scheduler() {
            @Override
            public long nowMs() {
                return loop.nowMs();
            }

            @Override
            public Cancellable schedule(long delayMs, Runnable task) {
                // Timers die with the incarnation that set them.
                EventLoop.Task t = loop.schedule(delayMs, () -> {
                    if (node.up && node.incarnation == incarnation) task.run();
                });
                return t::cancel;
            }
        };
        node.raft = new RaftNode(config, storageDecorator.apply(node.storage), node.kv, (to, message) -> send(node.id, to, message),
                scheduler, random::nextLong, e -> {
                    if (node.incarnation == incarnation) record(e);
                });
        node.raft.start();
    }

    /** Abrupt process death. Stable storage is kept. */
    public void crash(String id) {
        SimNode node = node(id);
        if (!node.up) return;
        node.up = false;
        node.incarnation++;
        emit(RaftEvent.Type.NODE_CRASHED, id, "process killed; volatile state lost, disk kept");
    }

    public void restart(String id) {
        SimNode node = node(id);
        if (node.up) crash(id);
        emit(RaftEvent.Type.NODE_RESTARTED, id, "process started on existing disk");
        boot(node);
    }

    /** Destroys the node's disk. This breaks Raft's durability assumption on purpose. */
    public void wipeDisk(String id) {
        SimNode node = node(id);
        if (node.up) crash(id);
        node.disk.wipe();
    }

    // =====================================================================================
    // Network
    // =====================================================================================

    private void send(String from, String to, RaftMessage message) {
        messagesSent++;
        SimNetwork.Fate fate = network.fateOnSend(from, to);
        if (options.recordMessages()) {
            emitMessage(RaftEvent.Type.MESSAGE_SENT, from, to, message, null);
        }
        if (fate != SimNetwork.Fate.DELIVER) {
            dropped(from, to, message, fate);
            return;
        }
        int copies = network.shouldDuplicate() ? 2 : 1;
        for (int i = 0; i < copies; i++) {
            long latency = network.latencyMs(from, to);
            loop.schedule(latency, () -> deliver(from, to, message));
        }
    }

    private void deliver(String from, String to, RaftMessage message) {
        SimNode target = nodes.get(to);
        if (target == null || !target.up) {
            dropped(from, to, message, SimNetwork.Fate.DROP_NODE_DOWN);
            return;
        }
        if (!network.connected(from, to)) { // the link was cut while the message was in flight
            dropped(from, to, message, SimNetwork.Fate.DROP_PARTITION);
            return;
        }
        messagesDelivered++;
        if (options.recordMessages()) {
            emitMessage(RaftEvent.Type.MESSAGE_DELIVERED, from, to, message, null);
        }
        target.raft.handle(message);
    }

    private void dropped(String from, String to, RaftMessage message, SimNetwork.Fate fate) {
        messagesDropped++;
        if (options.recordMessages()) {
            emitMessage(RaftEvent.Type.MESSAGE_DROPPED, from, to, message, fate.name());
        }
    }

    private void emitMessage(RaftEvent.Type type, String from, String to, RaftMessage message, String suffix) {
        String detail = SimNetwork.describe(message) + (suffix == null ? "" : " [" + suffix + "]");
        record(new RaftEvent(0, loop.nowMs(), from, type, message.term(), 0, 0, 0, to, detail));
    }

    // =====================================================================================
    // Events
    // =====================================================================================

    private void record(RaftEvent e) {
        RaftEvent stamped = new RaftEvent(++eventSeq, loop.nowMs(), e.node(), e.type(), e.term(), e.index(),
                e.entryTerm(), e.checksum(), e.peer(), e.detail());
        events.add(stamped);
        for (Consumer<RaftEvent> l : listeners) l.accept(stamped);
    }

    /** Records an event attributed to FaultLab itself (fault injection, client activity). */
    public RaftEvent emit(RaftEvent.Type type, String target, String detail) {
        return emitFrom(FAULTLAB, type, target, 0, detail);
    }

    public RaftEvent emitFrom(String source, RaftEvent.Type type, String target, long index, String detail) {
        record(new RaftEvent(0, loop.nowMs(), source, type, 0, index, 0, 0, target, detail));
        return events.get(events.size() - 1);
    }

    public void addListener(Consumer<RaftEvent> listener) {
        listeners.add(listener);
    }

    // =====================================================================================
    // Inspection
    // =====================================================================================

    /** A consistent view of one node's durable log and volatile state, for the safety checker. */
    public NodeObservation observe(String id) {
        SimNode node = node(id);
        List<NodeObservation.LogRef> log = new ArrayList<>();
        for (long i = node.disk.snapshotIndex() + 1; i <= node.disk.lastIndex(); i++) {
            LogEntry e = node.disk.entry(i);
            log.add(new NodeObservation.LogRef(e.index(), e.term(), e.checksum()));
        }
        boolean serving = node.isServing();
        return new NodeObservation(id, loop.nowMs(), node.disk.currentTerm(),
                serving ? node.raft.role().name() : "DOWN",
                serving ? node.raft.commitIndex() : 0, serving ? node.raft.lastApplied() : 0,
                node.disk.snapshotIndex(), node.disk.snapshotTerm(), log,
                serving ? node.kv.digest() : 0, serving);
    }

    /** The serving leader with the highest term, or null. */
    public SimNode leader() {
        SimNode best = null;
        for (SimNode n : nodes.values()) {
            if (n.isServing() && n.raft.isLeader() && (best == null || n.raft.currentTerm() > best.raft.currentTerm())) {
                best = n;
            }
        }
        return best;
    }

    public long maxCommitIndex() {
        long max = 0;
        for (SimNode n : nodes.values()) if (n.isServing()) max = Math.max(max, n.raft.commitIndex());
        return max;
    }

    public SimNode node(String id) {
        SimNode node = nodes.get(id);
        if (node == null) throw new IllegalArgumentException("unknown node " + id);
        return node;
    }

    public List<String> nodeIds() { return List.copyOf(nodes.keySet()); }
    public Iterable<SimNode> nodes() { return nodes.values(); }
    public EventLoop loop() { return loop; }
    public SimNetwork network() { return network; }
    public Random random() { return random; }
    public SimOptions options() { return options; }
    public List<RaftEvent> events() { return events; }
    public long messagesSent() { return messagesSent; }
    public long messagesDelivered() { return messagesDelivered; }
    public long messagesDropped() { return messagesDropped; }

    public void runFor(long durationMs) {
        loop.runUntil(loop.nowMs() + durationMs);
    }
}
