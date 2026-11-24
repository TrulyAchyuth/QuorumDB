package io.quorumdb.raft;

import io.quorumdb.raft.Collaborators.Cancellable;
import io.quorumdb.raft.Collaborators.EventListener;
import io.quorumdb.raft.Collaborators.RandomSource;
import io.quorumdb.raft.Collaborators.Scheduler;
import io.quorumdb.raft.Collaborators.StateMachine;
import io.quorumdb.raft.Collaborators.Transport;
import io.quorumdb.raft.Outcomes.Status;
import io.quorumdb.raft.RaftMessage.AppendEntries;
import io.quorumdb.raft.RaftMessage.AppendEntriesReply;
import io.quorumdb.raft.RaftMessage.InstallSnapshot;
import io.quorumdb.raft.RaftMessage.RequestVote;
import io.quorumdb.raft.RaftMessage.RequestVoteReply;
import io.quorumdb.storage.RaftStorage;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * A single Raft participant (Ongaro &amp; Ousterhout, "In Search of an Understandable Consensus
 * Algorithm", extended version). Implements leader election (§5.2), log replication (§5.3), the
 * election restriction and current-term commit rule (§5.4), snapshots/InstallSnapshot (§7),
 * linearizable reads via ReadIndex (§8 / thesis §6.4) and the PreVote + CheckQuorum extensions
 * (thesis §9.6, §6.2).
 *
 * <p>This class holds the node's state, the client-facing operations (proposals, ReadIndex reads),
 * message dispatch and commit/apply. The protocol logic lives in package-private helpers that operate on
 * that state: {@link Elections}, {@link Replication} and {@link Snapshots}.
 *
 * <h2>Concurrency model</h2>
 * The node is a deterministic, single-threaded state machine. <strong>Every</strong> method must be
 * invoked from one thread (the "node thread"): message handlers, timer callbacks scheduled through
 * the {@link Scheduler}, proposals and reads. There are no locks; there is no hidden clock or
 * randomness. This is what allows the same class to run under the deterministic simulator.
 *
 * <h2>Durability ordering</h2>
 * Hard state and log changes are persisted through {@link RaftStorage} (which returns only once
 * durable) <em>before</em> any message that depends on them is sent. A storage failure is fatal:
 * the node fail-stops ({@link #failure()}) instead of running on state it cannot trust.
 *
 * <h2>Commit / apply / acknowledge</h2>
 * An entry is <em>committed</em> once the leader that created it has replicated it on a majority
 * (or a later entry of the leader's current term is so replicated). Each node <em>applies</em>
 * committed entries to its state machine strictly in index order. A proposal is
 * <em>acknowledged</em> to its caller only after the leader has applied it.
 */
public final class RaftNode {

    public enum Role { FOLLOWER, PRE_CANDIDATE, CANDIDATE, LEADER }

    /** A client command together with the callback that receives its fate. */
    public record Proposal(byte[] command, Consumer<Outcomes.Proposal> callback) {}

    static final class Progress {
        long nextIndex;
        long matchIndex;
        boolean inflight;
        long inflightSinceMs;
        /**
         * Index of the snapshot in flight to this follower (0 = none). Tracked apart from {@link #inflight}:
         * replies to heartbeats and read rounds clear that flag, but say nothing about the snapshot.
         */
        long snapshotSentIndex;
        long snapshotSentMs;
        long lastAckMs = -1;
        long ackedReadSeq;
    }

    private record PendingProposal(long term, Consumer<Outcomes.Proposal> callback) {}

    private static final class PendingRead {
        final long seq;
        final long readIndex;
        final Consumer<Outcomes.Read> callback;

        PendingRead(long seq, long readIndex, Consumer<Outcomes.Read> callback) {
            this.seq = seq;
            this.readIndex = readIndex;
            this.callback = callback;
        }
    }

    final RaftConfig cfg;
    final String id;
    final RaftStorage storage;
    final StateMachine stateMachine;
    final Transport transport;
    final Scheduler scheduler;
    final RandomSource random;
    private final EventListener listener;

    final Elections elections;
    final Replication replication;
    final Snapshots snapshots;

    // ---- volatile state (rebuilt after a restart) ----
    Role role = Role.FOLLOWER;
    String leaderId;
    long commitIndex;
    long lastApplied;
    boolean running;
    private String failure;
    private long eventSeq;

    long lastLeaderContactMs = Long.MIN_VALUE / 2;
    Cancellable electionTimer;
    Cancellable heartbeatTimer;

    // candidate state
    final Set<String> votes = new LinkedHashSet<>();

    // leader state
    final Map<String, Progress> progress = new LinkedHashMap<>();
    private final TreeMap<Long, PendingProposal> pending = new TreeMap<>();
    private final ArrayDeque<PendingRead> reads = new ArrayDeque<>();
    long termStartIndex;
    long readSeq;

    public RaftNode(RaftConfig cfg, RaftStorage storage, StateMachine stateMachine, Transport transport,
                    Scheduler scheduler, RandomSource random, EventListener listener) {
        this.cfg = cfg;
        this.id = cfg.nodeId();
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.transport = transport;
        this.scheduler = scheduler;
        this.random = random;
        this.listener = listener == null ? EventListener.NONE : listener;
        this.elections = new Elections(this);
        this.replication = new Replication(this);
        this.snapshots = new Snapshots(this);
    }

    // =====================================================================================
    // Lifecycle
    // =====================================================================================

    /** Recovers volatile state from storage and starts participating as a follower. */
    public void start() {
        if (running) return;
        guarded(() -> {
            running = true;
            emit(RaftEvent.Type.NODE_STARTED, 0, 0, 0, null, "cluster=" + cfg.clusterSize());
            byte[] snapshot = storage.loadSnapshot();
            if (snapshot != null) {
                stateMachine.restore(snapshot);
            }
            // Everything in the snapshot is committed and applied; the rest of the log is
            // re-committed by the protocol (commitIndex is volatile, Raft Figure 2).
            commitIndex = storage.snapshotIndex();
            lastApplied = storage.snapshotIndex();
            emit(RaftEvent.Type.RECOVERY_COMPLETED, storage.snapshotIndex(), storage.snapshotTerm(), 0, null,
                    "term=" + storage.currentTerm() + " votedFor=" + storage.votedFor()
                            + " snapshotIndex=" + storage.snapshotIndex() + " lastIndex=" + storage.lastIndex());
            role = Role.FOLLOWER;
            elections.resetElectionTimer();
        });
    }

    /** Stops participating. Pending proposals are completed with {@link Status#UNKNOWN}. */
    public void stop() {
        if (!running) return;
        running = false;
        cancelTimers();
        failPendingWork("node stopped");
        emit(RaftEvent.Type.NODE_STOPPED, 0, 0, 0, null, null);
    }

    private void fail(Throwable cause) {
        failure = cause.getClass().getSimpleName() + ": " + cause.getMessage();
        running = false;
        cancelTimers();
        try {
            emit(RaftEvent.Type.NODE_FAILED, 0, 0, 0, null, failure);
        } finally {
            failPendingWork("node failed");
        }
    }

    private void cancelTimers() {
        if (electionTimer != null) electionTimer.cancel();
        if (heartbeatTimer != null) heartbeatTimer.cancel();
        electionTimer = null;
        heartbeatTimer = null;
    }

    /** Runs {@code body}; storage errors and broken internal invariants halt the node (fail-stop). */
    void guarded(Runnable body) {
        try {
            body.run();
        } catch (RuntimeException e) {
            fail(e);
        }
    }

    // =====================================================================================
    // Client-facing operations
    // =====================================================================================

    public void propose(byte[] command, Consumer<Outcomes.Proposal> callback) {
        propose(List.of(new Proposal(command, callback)));
    }

    /**
     * Appends the commands to the leader's log (one durable write for the whole batch) and starts
     * replicating them. Each callback fires exactly once.
     */
    public void propose(List<Proposal> proposals) {
        if (proposals.isEmpty()) return;
        if (!running || role != Role.LEADER) {
            Status status = Status.NOT_LEADER;
            for (Proposal p : proposals) p.callback().accept(new Outcomes.Proposal(status, 0, 0, null, leaderHint()));
            return;
        }
        int room = cfg.maxPendingProposals() - pending.size();
        List<Proposal> accepted = proposals.size() <= room ? proposals : proposals.subList(0, Math.max(room, 0));
        for (Proposal rejected : proposals.subList(accepted.size(), proposals.size())) {
            rejected.callback().accept(new Outcomes.Proposal(Status.BUSY, 0, 0, null, id));
        }
        if (accepted.isEmpty()) return;
        guarded(() -> {
            long term = storage.currentTerm();
            long index = storage.lastIndex();
            List<LogEntry> entries = new ArrayList<>(accepted.size());
            for (Proposal p : accepted) {
                entries.add(new LogEntry(++index, term, LogEntry.Type.COMMAND, p.command()));
            }
            // Register callbacks first so a storage failure below still completes them (as UNKNOWN).
            for (int i = 0; i < accepted.size(); i++) {
                pending.put(entries.get(i).index(), new PendingProposal(term, accepted.get(i).callback()));
            }
            storage.append(entries);
            emitAppended(entries, "proposed");
            replication.broadcastAppend(false);
            replication.advanceCommitIndex();
        });
    }

    /**
     * Linearizable read barrier (ReadIndex). The callback fires with {@link Status#OK} once (a) this
     * node has confirmed, via a round of heartbeats acknowledged by a majority <em>after</em> this call,
     * that it is still the leader, and (b) the state machine has applied everything committed at the
     * time of the call. The caller then reads the state machine on the node thread, inside the callback.
     */
    public void linearizableRead(Consumer<Outcomes.Read> callback) {
        if (!running || role != Role.LEADER) {
            callback.accept(new Outcomes.Read(Status.NOT_LEADER, 0, leaderHint()));
            return;
        }
        if (reads.size() >= cfg.maxPendingProposals()) {
            callback.accept(new Outcomes.Read(Status.BUSY, 0, id));
            return;
        }
        // A new leader may not know the latest commit index until it commits an entry of its own
        // term, so the read index is never below the leader's first entry (its no-op).
        long readIndex = Math.max(commitIndex, termStartIndex);
        // All reads tagged with round N arrived before round N's heartbeats were sent, so a majority
        // of acknowledgements for round N proves leadership at a point after each of them arrived.
        // Reads arriving while a round is in flight share the next round instead of each starting one.
        boolean roundInFlight = confirmedReadSeq() < readSeq;
        reads.addLast(new PendingRead(readSeq + 1, readIndex, callback));
        guarded(() -> {
            if (!roundInFlight) startReadRound();
            serveReads();
        });
    }

    private void startReadRound() {
        readSeq++;
        replication.broadcastAppend(true);
    }

    /** Highest read sequence acknowledged by a majority (the leader counts itself). */
    private long confirmedReadSeq() {
        long[] acked = new long[progress.size() + 1];
        int i = 0;
        for (Progress p : progress.values()) acked[i++] = p.ackedReadSeq;
        acked[i] = readSeq;
        Arrays.sort(acked);
        return acked[acked.length - cfg.quorum()];
    }

    void serveReads() {
        if (role != Role.LEADER || reads.isEmpty()) return;
        long confirmed = confirmedReadSeq();
        boolean waitingForNextRound = false;
        Iterator<PendingRead> it = reads.iterator();
        List<PendingRead> ready = new ArrayList<>();
        while (it.hasNext()) {
            PendingRead read = it.next();
            if (read.seq > confirmed) {
                waitingForNextRound |= read.seq > readSeq;
                continue;
            }
            if (lastApplied >= read.readIndex) {
                it.remove();
                ready.add(read);
            }
        }
        if (waitingForNextRound && confirmed >= readSeq) startReadRound();
        for (PendingRead read : ready) read.callback.accept(new Outcomes.Read(Status.OK, read.readIndex, id));
    }

    void failPendingWork(String reason) {
        if (!pending.isEmpty()) {
            List<Map.Entry<Long, PendingProposal>> lost = new ArrayList<>(pending.entrySet());
            pending.clear();
            for (Map.Entry<Long, PendingProposal> e : lost) {
                e.getValue().callback().accept(
                        new Outcomes.Proposal(Status.UNKNOWN, e.getKey(), e.getValue().term(), null, leaderHint()));
            }
        }
        if (!reads.isEmpty()) {
            List<PendingRead> lost = new ArrayList<>(reads);
            reads.clear();
            for (PendingRead r : lost) r.callback.accept(new Outcomes.Read(Status.NOT_LEADER, 0, leaderHint()));
        }
    }

    private String leaderHint() {
        return role == Role.LEADER ? id : leaderId;
    }

    // =====================================================================================
    // Message dispatch
    // =====================================================================================

    /** Entry point for every message delivered by the transport. */
    public void handle(RaftMessage message) {
        if (!running) return;
        if (message == null || message.from() == null || !cfg.peers().contains(message.from()) || message.term() < 0) {
            return; // not a member of this cluster's configuration, or malformed: ignore
        }
        guarded(() -> {
            switch (message) {
                case RequestVote m -> elections.onRequestVote(m);
                case RequestVoteReply m -> elections.onRequestVoteReply(m);
                case AppendEntries m -> replication.onAppendEntries(m);
                case AppendEntriesReply m -> replication.onAppendEntriesReply(m);
                case InstallSnapshot m -> snapshots.onInstallSnapshot(m);
            }
        });
    }

    // =====================================================================================
    // Commit and apply
    // =====================================================================================

    void setCommitIndex(long newCommitIndex) {
        long from = commitIndex + 1;
        commitIndex = newCommitIndex;
        for (long i = from; i <= newCommitIndex; i++) {
            LogEntry e = storage.entry(i);
            emit(RaftEvent.Type.ENTRY_COMMITTED, i, e.term(), e.checksum(), null, null);
        }
        applyCommitted();
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            LogEntry e = storage.entry(lastApplied + 1);
            byte[] result = e.type() == LogEntry.Type.COMMAND ? stateMachine.apply(e) : null;
            lastApplied = e.index();
            emit(RaftEvent.Type.ENTRY_APPLIED, e.index(), e.term(), e.checksum(), null, null);
            PendingProposal proposal = pending.remove(e.index());
            if (proposal != null) {
                // Acknowledge only now: committed by a majority and applied locally.
                proposal.callback().accept(new Outcomes.Proposal(Status.OK, e.index(), e.term(), result, id));
            }
        }
        if (cfg.snapshotThreshold() > 0 && lastApplied - storage.snapshotIndex() >= cfg.snapshotThreshold()) {
            snapshots.snapshotNow();
        }
        serveReads();
    }

    // =====================================================================================
    // Snapshots (Raft §7)
    // =====================================================================================

    /** Snapshots the state machine at {@code lastApplied} and compacts the log prefix. */
    public void takeSnapshot() {
        if (!running || lastApplied <= storage.snapshotIndex()) return;
        guarded(snapshots::snapshotNow);
    }

    // =====================================================================================
    // Introspection
    // =====================================================================================

    long lastLogTerm() {
        return storage.termAt(storage.lastIndex());
    }

    void emitAppended(List<LogEntry> entries, String detail) {
        for (LogEntry e : entries) {
            emit(RaftEvent.Type.ENTRY_APPENDED, e.index(), e.term(), e.checksum(), null, detail);
        }
    }

    void emit(RaftEvent.Type type, long index, long entryTerm, long checksum, String peer, String detail) {
        listener.onEvent(new RaftEvent(++eventSeq, scheduler.nowMs(), id, type, storage.currentTerm(), index,
                entryTerm, checksum, peer, detail));
    }

    public RaftStatus status() {
        Map<String, RaftStatus.Peer> peers = new LinkedHashMap<>();
        long now = scheduler.nowMs();
        if (role == Role.LEADER) {
            progress.forEach((peer, p) -> peers.put(peer,
                    new RaftStatus.Peer(p.nextIndex, p.matchIndex, p.lastAckMs < 0 ? -1 : now - p.lastAckMs)));
        }
        return new RaftStatus(id, role, storage.currentTerm(), leaderHint(), storage.votedFor(), commitIndex,
                lastApplied, storage.lastIndex(), lastLogTerm(), storage.snapshotIndex(), storage.snapshotTerm(),
                running, failure, pending.size(), peers);
    }

    public String id() { return id; }
    public Role role() { return role; }
    public boolean isLeader() { return running && role == Role.LEADER; }
    public long currentTerm() { return storage.currentTerm(); }
    public long commitIndex() { return commitIndex; }
    public long lastApplied() { return lastApplied; }
    public String leaderId() { return leaderHint(); }
    public boolean isRunning() { return running; }
    /** @return why the node fail-stopped, or null if it has not */
    public String failure() { return failure; }
    public RaftConfig config() { return cfg; }
}
