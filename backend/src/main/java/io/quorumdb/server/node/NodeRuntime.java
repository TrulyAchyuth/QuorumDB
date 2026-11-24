package io.quorumdb.server.node;

import io.quorumdb.kv.KvCommand;
import io.quorumdb.kv.KvStateMachine;
import io.quorumdb.raft.Collaborators.Cancellable;
import io.quorumdb.raft.Collaborators.Scheduler;
import io.quorumdb.raft.LogEntry;
import io.quorumdb.raft.Outcomes;
import io.quorumdb.raft.RaftConfig;
import io.quorumdb.raft.RaftEvent;
import io.quorumdb.raft.RaftMessage;
import io.quorumdb.raft.RaftNode;
import io.quorumdb.raft.RaftStatus;
import io.quorumdb.server.config.ClusterConfig;
import io.quorumdb.server.config.NodeProperties;
import io.quorumdb.server.transport.GrpcTransport;
import io.quorumdb.server.transport.NetworkFaults;
import io.quorumdb.storage.FaultyStorage;
import io.quorumdb.storage.FileStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Hosts one {@link RaftNode} in a real process.
 *
 * <h2>Threading</h2>
 * All Raft and state machine work happens on one dedicated thread (the "node thread", a single-threaded
 * scheduled executor), exactly as {@link RaftNode} requires. Other threads (HTTP handlers, gRPC) never
 * touch Raft state directly: they enqueue tasks and wait on futures. The node thread never blocks on
 * the network. WAL appends are written and fsynced by the storage's own writer thread, which reports
 * progress back through {@link RaftNode#onPersisted()} on the node thread; the rarer storage writes (votes,
 * truncation, snapshots) still block it.
 *
 * <h2>Group commit</h2>
 * Concurrent write requests are queued and drained in one batch per node-thread turn, so N concurrent
 * clients share one WAL append and one fsync instead of paying for N.
 *
 * <h2>Crash / restart</h2>
 * {@link #crash()} discards the Raft node and state machine (volatile state) without a graceful
 * shutdown; {@link #restart()} recovers them from the files on disk, the same path a fresh process takes.
 */
public final class NodeRuntime implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NodeRuntime.class);
    private static final int MAX_QUEUED_TASKS = 100_000;
    private static final int MAX_BATCH = 1024;

    public enum ReadMode { LINEARIZABLE, LEADER, STALE }

    /** Result of a read. {@code status} OK means {@code value} (possibly null = absent) is valid. */
    public record ReadResult(Outcomes.Status status, KvStateMachine.Versioned value, long readIndex, String leaderHint,
                             String servedBy) {}

    public record ScanResult(Outcomes.Status status, List<Map.Entry<String, KvStateMachine.Versioned>> entries,
                             long readIndex, String leaderHint) {}

    private record PendingWrite(byte[] command, CompletableFuture<Outcomes.Proposal> future) {}

    private final NodeProperties props;
    private final ClusterConfig cluster;
    private final EventLog events;
    private final NetworkFaults faults = new NetworkFaults();
    private final ScheduledThreadPoolExecutor nodeThread;
    private final GrpcTransport transport;
    private final ConcurrentLinkedQueue<PendingWrite> writeQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean drainScheduled = new AtomicBoolean();
    private final long startedAtMs = System.currentTimeMillis();

    // Owned by the node thread.
    private FileStorage fileStorage;
    private FaultyStorage storage;
    private KvStateMachine kv;
    private RaftNode raft;
    private int incarnation;
    private boolean crashed;

    /** Last status computed on the node thread; readable from any thread. */
    private volatile RaftStatus lastStatus;
    private volatile long lastLeaderChangeMs = System.currentTimeMillis();
    private volatile String lastLeader;

    public NodeRuntime(NodeProperties props, ClusterConfig cluster) {
        this.props = props;
        this.cluster = cluster;
        this.events = new EventLog(props.eventBufferSize());
        this.nodeThread = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "raft-" + cluster.self().id());
            t.setDaemon(true);
            return t;
        });
        nodeThread.setRemoveOnCancelPolicy(true);
        this.transport = new GrpcTransport(cluster, props.grpcPort(), faults, this::deliver);
    }

    public void start() throws IOException {
        transport.start();
        call(() -> {
            fileStorage = new FileStorage(Path.of(props.dataDir()),
                    props.fsync() ? FileStorage.SyncMode.ALWAYS : FileStorage.SyncMode.NEVER, 16L << 20);
            storage = new FaultyStorage(fileStorage);
            storage.setDurabilityListener(() -> nodeThread.execute(() -> {
                if (!crashed && raft != null) safely(raft::onPersisted);
            }));
            boot();
            return null;
        });
        log.info("node {} started (cluster {}, {} members, data dir {})", cluster.self().id(), cluster.clusterId(),
                cluster.size(), props.dataDir());
    }

    private void boot() {
        incarnation++;
        crashed = false;
        kv = new KvStateMachine();
        int inc = incarnation;
        RaftConfig config = new RaftConfig(cluster.self().id(), cluster.peerIds(), props.electionTimeoutMinMs(),
                props.electionTimeoutMaxMs(), props.heartbeatIntervalMs(), 512, 4 << 20, props.snapshotThreshold(),
                50_000, props.preVote(), props.checkQuorum(), false, props.snapshotTrailingEntries());
        Scheduler scheduler = new Scheduler() {
            @Override
            public long nowMs() {
                return System.nanoTime() / 1_000_000;
            }

            @Override
            public Cancellable schedule(long delayMs, Runnable task) {
                ScheduledFuture<?> f = nodeThread.schedule(() -> {
                    if (incarnation == inc && !crashed) safely(task);
                }, delayMs, TimeUnit.MILLISECONDS);
                return () -> f.cancel(false);
            }
        };
        raft = new RaftNode(config, storage, kv, transport, scheduler, bound -> ThreadLocalRandom.current().nextLong(bound),
                e -> {
                    if (incarnation == inc) record(e);
                });
        raft.start();
        refreshStatus();
    }

    /**
     * Stores an event emitted by the Raft core. Raft runs on the monotonic clock (right for timeouts), but
     * events are stamped with the wall clock here, when they are recorded on the node thread: they are shown
     * to people and merged with client and fault events that carry wall-clock times. Converting monotonic
     * time with an offset taken at start-up drifted by minutes inside a VM whose monotonic clock stood still
     * while the VM was paused (measured: 7.5 minutes behind after 11 minutes of uptime in Docker on Colima).
     */
    private void record(RaftEvent e) {
        RaftEvent stamped = events.append(new RaftEvent(0, System.currentTimeMillis(), e.node(), e.type(), e.term(),
                e.index(), e.entryTerm(), e.checksum(), e.peer(), e.detail()));
        if (stamped.type() == RaftEvent.Type.LEADER_ELECTED || stamped.type() == RaftEvent.Type.STEPPED_DOWN) {
            lastLeaderChangeMs = stamped.timeMs();
        }
    }

    /** Records an event caused by FaultLab or an operator (not by Raft itself). */
    public void recordExternal(RaftEvent.Type type, String detail) {
        long term = lastStatus == null ? 0 : lastStatus.term();
        events.append(new RaftEvent(0, System.currentTimeMillis(), cluster.self().id(), type, term, 0, 0, 0,
                cluster.self().id(), detail));
    }

    private void safely(Runnable task) {
        try {
            task.run();
        } catch (RuntimeException e) {
            log.error("unexpected error on node thread", e);
        } finally {
            refreshStatus();
        }
    }

    private void refreshStatus() {
        if (raft == null) return;
        RaftStatus s = raft.status();
        lastStatus = s;
        String leader = s.running() ? s.leaderId() : null;
        lastLeader = leader;
    }

    // =====================================================================================
    // Inbound Raft messages
    // =====================================================================================

    private void deliver(RaftMessage message) {
        if (nodeThread.getQueue().size() > MAX_QUEUED_TASKS) return; // overloaded: shed like a lost packet
        nodeThread.execute(() -> {
            if (!crashed && raft != null) safely(() -> raft.handle(message));
        });
    }

    // =====================================================================================
    // Client operations
    // =====================================================================================

    /** Proposes a command; completes when it is committed and applied (or definitely/possibly failed). */
    public CompletableFuture<Outcomes.Proposal> write(KvCommand command) {
        CompletableFuture<Outcomes.Proposal> future = new CompletableFuture<>();
        if (nodeThread.getQueue().size() > MAX_QUEUED_TASKS) {
            future.complete(new Outcomes.Proposal(Outcomes.Status.BUSY, 0, 0, null, lastLeader));
            return future;
        }
        writeQueue.add(new PendingWrite(command.encode(), future));
        if (drainScheduled.compareAndSet(false, true)) nodeThread.execute(this::drainWrites);
        return future;
    }

    private void drainWrites() {
        drainScheduled.set(false);
        List<RaftNode.Proposal> batch = new ArrayList<>();
        PendingWrite w;
        while (batch.size() < MAX_BATCH && (w = writeQueue.poll()) != null) {
            PendingWrite pending = w;
            batch.add(new RaftNode.Proposal(pending.command(), pending.future()::complete));
        }
        if (!writeQueue.isEmpty() && drainScheduled.compareAndSet(false, true)) nodeThread.execute(this::drainWrites);
        if (batch.isEmpty()) return;
        if (crashed || raft == null) {
            batch.forEach(p -> p.callback().accept(new Outcomes.Proposal(Outcomes.Status.NOT_LEADER, 0, 0, null, null)));
            return;
        }
        safely(() -> raft.propose(batch));
    }

    public CompletableFuture<ReadResult> read(String key, ReadMode mode) {
        CompletableFuture<ReadResult> future = new CompletableFuture<>();
        String self = cluster.self().id();
        nodeThread.execute(() -> safely(() -> {
            if (crashed || raft == null || !raft.isRunning()) {
                future.complete(new ReadResult(Outcomes.Status.NOT_LEADER, null, 0, null, self));
                return;
            }
            switch (mode) {
                case LINEARIZABLE -> raft.linearizableRead(outcome -> future.complete(outcome.status() == Outcomes.Status.OK
                        ? new ReadResult(Outcomes.Status.OK, kv.get(key), outcome.readIndex(), self, self)
                        : new ReadResult(outcome.status(), null, 0, outcome.leaderHint(), self)));
                case LEADER -> future.complete(raft.isLeader()
                        ? new ReadResult(Outcomes.Status.OK, kv.get(key), raft.lastApplied(), self, self)
                        : new ReadResult(Outcomes.Status.NOT_LEADER, null, 0, raft.leaderId(), self));
                case STALE -> future.complete(new ReadResult(Outcomes.Status.OK, kv.get(key), raft.lastApplied(),
                        raft.leaderId(), self));
            }
        }));
        return future;
    }

    public CompletableFuture<ScanResult> scan(String prefix, int limit, ReadMode mode) {
        CompletableFuture<ScanResult> future = new CompletableFuture<>();
        nodeThread.execute(() -> safely(() -> {
            if (crashed || raft == null || !raft.isRunning()) {
                future.complete(new ScanResult(Outcomes.Status.NOT_LEADER, List.of(), 0, null));
                return;
            }
            switch (mode) {
                case LINEARIZABLE -> raft.linearizableRead(o -> future.complete(o.status() == Outcomes.Status.OK
                        ? new ScanResult(Outcomes.Status.OK, kv.scan(prefix, limit), o.readIndex(), cluster.self().id())
                        : new ScanResult(o.status(), List.of(), 0, o.leaderHint())));
                case LEADER -> future.complete(raft.isLeader()
                        ? new ScanResult(Outcomes.Status.OK, kv.scan(prefix, limit), raft.lastApplied(), cluster.self().id())
                        : new ScanResult(Outcomes.Status.NOT_LEADER, List.of(), 0, raft.leaderId()));
                case STALE -> future.complete(new ScanResult(Outcomes.Status.OK, kv.scan(prefix, limit), raft.lastApplied(),
                        raft.leaderId()));
            }
        }));
        return future;
    }

    // =====================================================================================
    // Live fault injection
    // =====================================================================================

    /** Simulated process crash: volatile state is lost, the disk is kept. The HTTP server stays up. */
    public void crash() {
        call(() -> {
            if (crashed) return null;
            recordExternal(RaftEvent.Type.NODE_CRASHED, "raft process state discarded (live fault injection)");
            crashed = true;
            incarnation++;
            raft.stop();
            refreshStatus();
            return null;
        });
    }

    /** Recovers the node from its files, as a freshly started process would. */
    public void restart() {
        call(() -> {
            if (!crashed && raft.isRunning()) {
                crashed = true;
                incarnation++;
                raft.stop();
            }
            recordExternal(RaftEvent.Type.NODE_RESTARTED, "recovering from data dir");
            storage.setFailWrites(false);
            boot();
            return null;
        });
    }

    public void setStorageFailure(boolean failing) {
        call(() -> {
            storage.setFailWrites(failing);
            recordExternal(failing ? RaftEvent.Type.FAULT_INJECTED : RaftEvent.Type.FAULT_HEALED,
                    failing ? "disk writes now fail" : "disk writes work again");
            return null;
        });
    }

    /** Forces a snapshot and log compaction now. */
    public void snapshot() {
        call(() -> {
            raft.takeSnapshot();
            return null;
        });
    }

    // =====================================================================================
    // Introspection
    // =====================================================================================

    /** Durable log entries in {@code [from, to]} as (index, term, checksum), plus volatile state. */
    public record LogView(long term, String role, long commitIndex, long lastApplied, long snapshotIndex,
                          long snapshotTerm, long from, List<long[]> entries, long stateDigest, boolean running) {}

    public LogView logView(long from, int limit) {
        return call(() -> {
            long first = Math.max(from, fileStorage.snapshotIndex() + 1);
            long last = Math.min(fileStorage.lastIndex(), first + limit - 1);
            List<long[]> out = new ArrayList<>();
            for (long i = first; i <= last; i++) {
                LogEntry e = fileStorage.entry(i);
                out.add(new long[] {e.index(), e.term(), e.checksum()});
            }
            boolean running = !crashed && raft.isRunning();
            return new LogView(fileStorage.currentTerm(), running ? raft.role().name() : "DOWN",
                    running ? raft.commitIndex() : 0, running ? raft.lastApplied() : 0, fileStorage.snapshotIndex(),
                    fileStorage.snapshotTerm(), first, out, running ? kv.digest() : 0, running);
        });
    }

    public record Storage(long diskBytes, long snapshotIndex, long lastIndex, int keys, boolean failingWrites,
                          String dataDir, boolean fsync) {}

    public Storage storageInfo() {
        return call(() -> new Storage(fileStorage.diskUsageBytes(), fileStorage.snapshotIndex(), fileStorage.lastIndex(),
                crashed ? 0 : kv.size(), storage.isFailingWrites(), props.dataDir(), props.fsync()));
    }

    /** Fresh status from the node thread (falls back to the last known one if the thread is busy). */
    public RaftStatus status() {
        try {
            return nodeThread.submit(() -> {
                refreshStatus();
                return lastStatus;
            }).get(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return lastStatus;
        } catch (ExecutionException | TimeoutException e) {
            return lastStatus;
        }
    }

    public boolean isCrashed() {
        return crashed;
    }

    private <T> T call(Supplier<T> body) {
        try {
            return nodeThread.submit(body::get).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof RuntimeException r ? r : new IllegalStateException(e.getCause());
        } catch (TimeoutException e) {
            throw new IllegalStateException("node thread did not respond in time", e);
        }
    }

    public EventLog events() { return events; }
    public NetworkFaults faults() { return faults; }
    public GrpcTransport transport() { return transport; }
    public ClusterConfig cluster() { return cluster; }
    public NodeProperties props() { return props; }
    public long startedAtMs() { return startedAtMs; }
    public long lastLeaderChangeMs() { return lastLeaderChangeMs; }
    public int queuedTasks() { return nodeThread.getQueue().size(); }

    @Override
    public void close() {
        try {
            call(() -> {
                if (raft != null) raft.stop();
                if (fileStorage != null) fileStorage.close();
                return null;
            });
        } catch (RuntimeException e) {
            log.warn("error while stopping node", e);
        }
        transport.close();
        nodeThread.shutdownNow();
    }
}
