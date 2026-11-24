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
 * <h2>Concurrency model</h2>
 * The node is a deterministic, single-threaded state machine. <strong>Every</strong> method must be
 * invoked from one thread (the "node thread"): message handlers, timer callbacks scheduled through
 * the {@link Scheduler}, proposals and reads. There are no locks; there is no hidden clock or
 * randomness. This is what allows the same class to run under the deterministic simulator.
 *
 * <h2>Durability ordering</h2>
 * Hard state, truncations and snapshots are persisted through {@link RaftStorage} (which returns only
 * once durable) <em>before</em> any message that depends on them is sent. Log appends may become durable
 * later, off this thread: a follower acknowledges entries only once {@link RaftStorage#durableIndex()}
 * covers them, and a leader counts itself towards a majority only up to its own durable index. Storage
 * reports progress through {@link #onPersisted()}. A storage failure is fatal: the node fail-stops
 * ({@link #failure()}) instead of running on state it cannot trust.
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

    private static final class Progress {
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

    private final RaftConfig cfg;
    private final String id;
    private final RaftStorage storage;
    private final StateMachine stateMachine;
    private final Transport transport;
    private final Scheduler scheduler;
    private final RandomSource random;
    private final EventListener listener;

    // ---- volatile state (rebuilt after a restart) ----
    private Role role = Role.FOLLOWER;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private boolean running;
    private String failure;
    private long eventSeq;

    private long lastLeaderContactMs = Long.MIN_VALUE / 2;

    // follower: success reply held back until the entries it acknowledges are durable (0 = none)
    private long awaitingAckIndex;
    private long awaitingAckReadSeq;
    private Cancellable electionTimer;
    private Cancellable heartbeatTimer;

    // candidate state
    private final Set<String> votes = new LinkedHashSet<>();

    // leader state
    private final Map<String, Progress> progress = new LinkedHashMap<>();
    private final TreeMap<Long, PendingProposal> pending = new TreeMap<>();
    private final ArrayDeque<PendingRead> reads = new ArrayDeque<>();
    private long termStartIndex;
    private long readSeq;

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
            resetElectionTimer();
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
    private void guarded(Runnable body) {
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
            broadcastAppend(false);
            advanceCommitIndex();
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
        broadcastAppend(true);
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

    private void serveReads() {
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

    private void failPendingWork(String reason) {
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

    /**
     * Storage made more of the log durable (or a background write failed). Must be called on the node
     * thread; the leader may now commit, a follower sends the acknowledgement it was holding back.
     */
    public void onPersisted() {
        if (!running) return;
        guarded(() -> {
            long durable = storage.durableIndex(); // throws if a background write failed: fail-stop
            if (role == Role.LEADER) {
                advanceCommitIndex();
            } else if (role == Role.FOLLOWER && awaitingAckIndex > 0 && leaderId != null && durable >= awaitingAckIndex) {
                transport.send(leaderId, new AppendEntriesReply(id, storage.currentTerm(), true, awaitingAckIndex,
                        0, 0, awaitingAckReadSeq));
                awaitingAckIndex = 0;
                awaitingAckReadSeq = 0;
            }
        });
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
                case RequestVote m -> onRequestVote(m);
                case RequestVoteReply m -> onRequestVoteReply(m);
                case AppendEntries m -> onAppendEntries(m);
                case AppendEntriesReply m -> onAppendEntriesReply(m);
                case InstallSnapshot m -> onInstallSnapshot(m);
            }
        });
    }

    // =====================================================================================
    // Elections (Raft §5.2, §5.4.1)
    // =====================================================================================

    private void resetElectionTimer() {
        if (electionTimer != null) electionTimer.cancel();
        long span = cfg.electionTimeoutMaxMs() - cfg.electionTimeoutMinMs();
        long timeout = cfg.electionTimeoutMinMs() + random.nextLong(span);
        electionTimer = scheduler.schedule(timeout, this::onElectionTimeout);
    }

    private void onElectionTimeout() {
        if (!running || role == Role.LEADER) return;
        guarded(() -> {
            emit(RaftEvent.Type.ELECTION_TIMEOUT, 0, 0, 0, leaderId, "no leader contact within election timeout");
            leaderId = null;
            campaign(cfg.preVote());
        });
    }

    private void campaign(boolean preVote) {
        votes.clear();
        votes.add(id);
        long campaignTerm;
        if (preVote) {
            // Pre-vote: ask whether we *could* win without bumping our term, so a node that
            // cannot reach a majority does not inflate terms and later depose a healthy leader.
            role = Role.PRE_CANDIDATE;
            campaignTerm = storage.currentTerm() + 1;
            emit(RaftEvent.Type.PRE_VOTE_STARTED, storage.lastIndex(), lastLogTerm(), 0, null, "for term " + campaignTerm);
        } else {
            role = Role.CANDIDATE;
            campaignTerm = storage.currentTerm() + 1;
            storage.saveHardState(campaignTerm, id); // vote for self, durably, before asking others
            emit(RaftEvent.Type.TERM_ADVANCED, 0, 0, 0, null, "campaigning");
            emit(RaftEvent.Type.ELECTION_STARTED, storage.lastIndex(), lastLogTerm(), 0, null, null);
        }
        resetElectionTimer();
        if (votes.size() >= cfg.quorum()) {
            wonRound(preVote);
            return;
        }
        RequestVote request = new RequestVote(id, campaignTerm, storage.lastIndex(), lastLogTerm(), preVote);
        for (String peer : cfg.peers()) {
            emit(RaftEvent.Type.VOTE_REQUESTED, storage.lastIndex(), lastLogTerm(), 0, peer, preVote ? "pre-vote" : null);
            transport.send(peer, request);
        }
    }

    private void wonRound(boolean preVote) {
        if (preVote) {
            campaign(false);
        } else {
            becomeLeader();
        }
    }

    private void onRequestVote(RequestVote m) {
        long now = scheduler.nowMs();
        boolean leaderIsFresh = role == Role.LEADER
                || (leaderId != null && now - lastLeaderContactMs < cfg.electionTimeoutMinMs());
        if ((cfg.preVote() || cfg.checkQuorum()) && leaderIsFresh) {
            // Leader stickiness (thesis §4.2.3): while we believe a live leader exists, a vote request
            // is disregarded entirely: no vote, no term change. The reply carries our term untouched.
            emit(RaftEvent.Type.VOTE_DENIED, 0, 0, 0, m.from(),
                    (m.preVote() ? "pre-vote: " : "") + "current leader still fresh");
            transport.send(m.from(), new RequestVoteReply(id, storage.currentTerm(), m.term(), false, m.preVote()));
            return;
        }
        boolean upToDate = m.lastLogTerm() > lastLogTerm()
                || (m.lastLogTerm() == lastLogTerm() && m.lastLogIndex() >= storage.lastIndex());

        if (m.preVote()) {
            // Granting a pre-vote changes no state at all.
            boolean grant = m.term() > storage.currentTerm() && upToDate;
            emit(grant ? RaftEvent.Type.VOTE_GRANTED : RaftEvent.Type.VOTE_DENIED, 0, 0, 0, m.from(),
                    "pre-vote for term " + m.term() + (grant ? "" : upToDate ? ": stale term" : ": log not up to date"));
            transport.send(m.from(), new RequestVoteReply(id, storage.currentTerm(), m.term(), grant, true));
            return;
        }

        if (m.term() > storage.currentTerm()) {
            becomeFollower(m.term(), null, "saw higher term in RequestVote from " + m.from());
        }
        String votedFor = storage.votedFor();
        boolean grant = m.term() == storage.currentTerm()
                && (votedFor == null || votedFor.equals(m.from()))
                && upToDate;
        if (grant) {
            if (votedFor == null) {
                storage.saveHardState(storage.currentTerm(), m.from()); // durable before replying
            }
            resetElectionTimer();
            emit(RaftEvent.Type.VOTE_GRANTED, 0, 0, 0, m.from(), null);
        } else {
            String why = m.term() < storage.currentTerm() ? "stale term " + m.term()
                    : !upToDate ? "candidate log not up to date"
                    : "already voted for " + votedFor;
            emit(RaftEvent.Type.VOTE_DENIED, 0, 0, 0, m.from(), why);
        }
        transport.send(m.from(), new RequestVoteReply(id, storage.currentTerm(), m.term(), grant, false));
    }

    private void onRequestVoteReply(RequestVoteReply m) {
        if (m.term() > storage.currentTerm()) {
            becomeFollower(m.term(), null, "saw higher term in vote reply from " + m.from());
            return;
        }
        boolean forCurrentRound = m.preVote()
                ? role == Role.PRE_CANDIDATE && m.requestTerm() == storage.currentTerm() + 1
                : role == Role.CANDIDATE && m.requestTerm() == storage.currentTerm();
        if (!forCurrentRound || !m.granted()) return;
        if (votes.add(m.from()) && votes.size() >= cfg.quorum()) {
            wonRound(m.preVote());
        }
    }

    private void becomeFollower(long term, String leader, String reason) {
        boolean wasLeader = role == Role.LEADER;
        if (term > storage.currentTerm()) {
            storage.saveHardState(term, null);
            emit(RaftEvent.Type.TERM_ADVANCED, 0, 0, 0, leader, reason);
        }
        if (role != Role.FOLLOWER) {
            emit(RaftEvent.Type.STEPPED_DOWN, 0, 0, 0, leader, "was " + role + ": " + reason);
        }
        role = Role.FOLLOWER;
        leaderId = leader;
        votes.clear();
        // A held-back acknowledgement was verified against the previous leader's log, not the next one's.
        awaitingAckIndex = 0;
        awaitingAckReadSeq = 0;
        if (wasLeader) {
            if (heartbeatTimer != null) heartbeatTimer.cancel();
            heartbeatTimer = null;
            progress.clear();
            failPendingWork(reason);
        }
        resetElectionTimer();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        if (electionTimer != null) electionTimer.cancel();
        electionTimer = null;
        long now = scheduler.nowMs();
        progress.clear();
        for (String peer : cfg.peers()) {
            Progress p = new Progress();
            p.nextIndex = storage.lastIndex() + 1;
            p.lastAckMs = now; // grace period for CheckQuorum
            progress.put(peer, p);
        }
        readSeq = 0;
        // A leader may only count replicas for entries of its own term (§5.4.2), so it appends a
        // no-op immediately; committing it also commits every earlier entry.
        LogEntry noop = new LogEntry(storage.lastIndex() + 1, storage.currentTerm(), LogEntry.Type.NOOP, new byte[0]);
        storage.append(List.of(noop));
        termStartIndex = noop.index();
        emit(RaftEvent.Type.LEADER_ELECTED, noop.index(), noop.term(), 0, null, "votes=" + votes);
        emitAppended(List.of(noop), "leader no-op");
        broadcastAppend(true);
        heartbeatTimer = scheduler.schedule(cfg.heartbeatIntervalMs(), this::onHeartbeatTimer);
        advanceCommitIndex();
    }

    // =====================================================================================
    // Log replication: leader side (Raft §5.3, §5.4.2)
    // =====================================================================================

    private void onHeartbeatTimer() {
        if (!running || role != Role.LEADER) return;
        guarded(() -> {
            long now = scheduler.nowMs();
            if (cfg.checkQuorum()) {
                int active = 1;
                for (Progress p : progress.values()) {
                    if (now - p.lastAckMs < cfg.electionTimeoutMaxMs()) active++;
                }
                if (active < cfg.quorum()) {
                    becomeFollower(storage.currentTerm(), null,
                            "check-quorum: heard from only " + active + "/" + cfg.clusterSize() + " nodes");
                    return;
                }
            }
            if (cfg.emitHeartbeatEvents()) {
                emit(RaftEvent.Type.HEARTBEAT_SENT, commitIndex, 0, 0, null, null);
            }
            broadcastAppend(true);
            heartbeatTimer = scheduler.schedule(cfg.heartbeatIntervalMs(), this::onHeartbeatTimer);
        });
    }

    private void broadcastAppend(boolean force) {
        for (Map.Entry<String, Progress> e : progress.entrySet()) {
            sendAppend(e.getKey(), e.getValue(), force);
        }
    }

    /**
     * Sends the follower whatever it is missing. At most one un-acknowledged data message is kept in
     * flight per follower; {@code force} (heartbeats, read rounds) sends regardless, which is also the
     * retransmission path for lost messages.
     */
    private void sendAppend(String peer, Progress p, boolean force) {
        long now = scheduler.nowMs();
        if (p.nextIndex <= storage.snapshotIndex()) {
            // The entries this follower needs were compacted away: ship the snapshot. Still send a
            // heartbeat so it keeps recognising us while the (potentially large) snapshot is in flight.
            if (force) {
                transport.send(peer, new AppendEntries(id, storage.currentTerm(), storage.snapshotIndex(),
                        storage.snapshotTerm(), List.of(), commitIndex, readSeq));
            }
            // One snapshot in flight at a time: send again only for a newer snapshot, or after a timeout
            // (the message, or its reply, was lost).
            if (p.snapshotSentIndex != storage.snapshotIndex() || now - p.snapshotSentMs >= cfg.electionTimeoutMaxMs()) {
                p.snapshotSentIndex = storage.snapshotIndex();
                p.snapshotSentMs = now;
                emit(RaftEvent.Type.SNAPSHOT_SENT, storage.snapshotIndex(), storage.snapshotTerm(), 0, peer, null);
                transport.send(peer, new InstallSnapshot(id, storage.currentTerm(), storage.snapshotIndex(),
                        storage.snapshotTerm(), storage.loadSnapshot(), readSeq));
            }
            return;
        }
        if (p.inflight && !force) return;
        long prevIndex = p.nextIndex - 1;
        long last = Math.min(storage.lastIndex(), prevIndex + cfg.maxEntriesPerAppend());
        List<LogEntry> entries = last > prevIndex ? storage.entries(p.nextIndex, last) : List.of();
        int bytes = 0;
        for (int i = 0; i < entries.size(); i++) {
            bytes += entries.get(i).sizeBytes();
            if (bytes > cfg.maxBytesPerAppend() && i > 0) {
                entries = entries.subList(0, i);
                break;
            }
        }
        if (!entries.isEmpty()) {
            p.inflight = true;
            p.inflightSinceMs = now;
        }
        transport.send(peer, new AppendEntries(id, storage.currentTerm(), prevIndex, storage.termAt(prevIndex),
                entries, commitIndex, readSeq));
    }

    private void onAppendEntriesReply(AppendEntriesReply m) {
        if (m.term() > storage.currentTerm()) {
            becomeFollower(m.term(), null, "saw higher term in AppendEntries reply from " + m.from());
            return;
        }
        if (role != Role.LEADER || m.term() != storage.currentTerm()) return; // stale reply
        Progress p = progress.get(m.from());
        if (p == null) return;
        // Any same-term reply proves the follower still accepts us as leader.
        p.lastAckMs = scheduler.nowMs();
        p.ackedReadSeq = Math.max(p.ackedReadSeq, m.readSeq());
        p.inflight = false;
        if (m.success()) {
            if (p.snapshotSentIndex > 0 && m.matchIndex() >= p.snapshotSentIndex) {
                p.snapshotSentIndex = 0; // the follower has (at least) what the snapshot covered
            }
            if (m.matchIndex() > p.matchIndex) {
                p.matchIndex = Math.min(m.matchIndex(), storage.lastIndex());
                p.nextIndex = Math.max(p.nextIndex, p.matchIndex + 1);
                advanceCommitIndex();
            }
            if (role == Role.LEADER && p.nextIndex <= storage.lastIndex()) {
                sendAppend(m.from(), p, false);
            }
        } else {
            long next = m.conflictIndex();
            if (m.conflictTerm() > 0) {
                // Fast backtracking: skip the follower's whole conflicting term in one step.
                long scan = Math.min(storage.lastIndex(), p.nextIndex - 1);
                while (scan > storage.snapshotIndex() && storage.termAt(scan) > m.conflictTerm()) scan--;
                if (scan > storage.snapshotIndex() && storage.termAt(scan) == m.conflictTerm()) {
                    next = scan + 1;
                }
            }
            // Replies can be stale or duplicated: never move below what is already known to match.
            p.nextIndex = Math.max(p.matchIndex + 1, Math.min(Math.max(next, 1), storage.lastIndex() + 1));
            sendAppend(m.from(), p, false);
        }
        if (role == Role.LEADER) serveReads();
    }

    /**
     * Advances the commit index to the highest index stored on a majority, but only if that entry is
     * from the current term (§5.4.2, Figure 8): earlier-term entries are committed indirectly.
     */
    private void advanceCommitIndex() {
        if (role != Role.LEADER) return;
        long[] match = new long[progress.size() + 1];
        int i = 0;
        for (Progress p : progress.values()) match[i++] = p.matchIndex;
        match[i] = storage.durableIndex(); // the leader's own copy counts only once it is durable
        Arrays.sort(match);
        long majorityIndex = match[match.length - cfg.quorum()];
        if (majorityIndex > commitIndex && storage.termAt(majorityIndex) == storage.currentTerm()) {
            setCommitIndex(majorityIndex);
            // Let idle followers learn the new commit index without waiting for the next heartbeat.
            broadcastAppend(false);
        }
    }

    // =====================================================================================
    // Log replication: follower side
    // =====================================================================================

    private void onAppendEntries(AppendEntries m) {
        if (m.term() < storage.currentTerm()) {
            transport.send(m.from(), new AppendEntriesReply(id, storage.currentTerm(), false, 0, 0, 0, m.readSeq()));
            return;
        }
        if (!acceptLeader(m.from(), m.term())) return;

        long prevIndex = m.prevLogIndex();
        long prevTerm = m.prevLogTerm();
        List<LogEntry> entries = m.entries();
        long lastNewIndex = prevIndex + entries.size();
        long snapshotIndex = storage.snapshotIndex();

        if (prevIndex < snapshotIndex) {
            // Part of this message is already covered by our snapshot, hence committed and identical
            // to the leader's log (Leader Completeness). Skip that part.
            int skip = (int) Math.min(entries.size(), snapshotIndex - prevIndex);
            if (prevIndex + skip < snapshotIndex) {
                reply(m, true, lastNewIndex, 0, 0);
                return;
            }
            entries = entries.subList(skip, entries.size());
            prevIndex = snapshotIndex;
            prevTerm = storage.snapshotTerm();
        }
        if (prevIndex > storage.lastIndex()) {
            reply(m, false, 0, storage.lastIndex() + 1, 0);
            return;
        }
        if (storage.termAt(prevIndex) != prevTerm) {
            long conflictTerm = storage.termAt(prevIndex);
            long conflictIndex = prevIndex;
            while (conflictIndex - 1 > snapshotIndex && storage.termAt(conflictIndex - 1) == conflictTerm) {
                conflictIndex--;
            }
            reply(m, false, 0, conflictIndex, conflictTerm);
            return;
        }

        // Skip entries we already have; on the first term mismatch delete it and everything after (§5.3).
        int firstNew = 0;
        while (firstNew < entries.size()) {
            LogEntry e = entries.get(firstNew);
            if (e.index() > storage.lastIndex()) break;
            if (storage.termAt(e.index()) != e.term()) {
                if (e.index() <= commitIndex) {
                    throw new IllegalStateException("refusing to truncate committed entry " + e.index()
                            + " (commitIndex=" + commitIndex + "): safety violation");
                }
                emit(RaftEvent.Type.LOG_TRUNCATED, e.index(), storage.termAt(e.index()), 0, m.from(),
                        "removing [" + e.index() + ", " + storage.lastIndex() + "] conflicting with leader");
                storage.truncateSuffix(e.index());
                awaitingAckIndex = Math.min(awaitingAckIndex, e.index() - 1);
                break;
            }
            firstNew++;
        }
        if (firstNew < entries.size()) {
            List<LogEntry> toAppend = entries.subList(firstNew, entries.size());
            storage.append(toAppend); // durable before we acknowledge
            emitAppended(toAppend, "from leader");
        }
        if (m.leaderCommit() > commitIndex) {
            // Only up to the last entry verified to match the leader, never our unverified suffix.
            long target = Math.min(m.leaderCommit(), lastNewIndex);
            if (target > commitIndex) setCommitIndex(target);
        }
        if (storage.durableIndex() >= lastNewIndex) {
            reply(m, true, lastNewIndex, 0, 0);
        } else {
            // Acknowledging now could promise entries a crash would lose; onPersisted() replies instead.
            awaitingAckIndex = Math.max(awaitingAckIndex, lastNewIndex);
            awaitingAckReadSeq = Math.max(awaitingAckReadSeq, m.readSeq());
        }
    }

    private void reply(AppendEntries m, boolean success, long matchIndex, long conflictIndex, long conflictTerm) {
        transport.send(m.from(), new AppendEntriesReply(id, storage.currentTerm(), success, matchIndex,
                conflictIndex, conflictTerm, m.readSeq()));
    }

    /** Common handling for a message from a leader whose term is at least ours. */
    private boolean acceptLeader(String leader, long term) {
        if (term > storage.currentTerm() || role != Role.FOLLOWER) {
            if (role == Role.LEADER && term == storage.currentTerm()) {
                throw new IllegalStateException("two leaders in term " + term + ": " + id + " and " + leader);
            }
            becomeFollower(term, leader, "accepted leader " + leader);
        }
        leaderId = leader;
        lastLeaderContactMs = scheduler.nowMs();
        resetElectionTimer();
        return true;
    }

    // =====================================================================================
    // Commit and apply
    // =====================================================================================

    private void setCommitIndex(long newCommitIndex) {
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
            snapshotNow();
        }
        serveReads();
    }

    // =====================================================================================
    // Snapshots (Raft §7)
    // =====================================================================================

    /** Snapshots the state machine at {@code lastApplied} and compacts the log prefix. */
    public void takeSnapshot() {
        if (!running || lastApplied <= storage.snapshotIndex()) return;
        guarded(this::snapshotNow);
    }

    private void snapshotNow() {
        long index = lastApplied;
        long term = storage.termAt(index);
        byte[] data = stateMachine.snapshot();
        storage.saveSnapshot(index, term, data);
        emit(RaftEvent.Type.SNAPSHOT_CREATED, index, term, 0, null, data.length + " bytes");
    }

    private void onInstallSnapshot(InstallSnapshot m) {
        if (m.term() < storage.currentTerm()) {
            transport.send(m.from(), new AppendEntriesReply(id, storage.currentTerm(), false, 0, 0, 0, m.readSeq()));
            return;
        }
        if (!acceptLeader(m.from(), m.term())) return;
        if (m.lastIncludedIndex() > commitIndex) {
            storage.installSnapshot(m.lastIncludedIndex(), m.lastIncludedTerm(), m.data());
            stateMachine.restore(m.data());
            commitIndex = m.lastIncludedIndex();
            lastApplied = m.lastIncludedIndex();
            emit(RaftEvent.Type.SNAPSHOT_INSTALLED, m.lastIncludedIndex(), m.lastIncludedTerm(), 0, m.from(),
                    m.data().length + " bytes");
        }
        // Whether installed or already covered, our committed prefix matches the leader up to here.
        transport.send(m.from(), new AppendEntriesReply(id, storage.currentTerm(), true, m.lastIncludedIndex(),
                0, 0, m.readSeq()));
    }

    // =====================================================================================
    // Introspection
    // =====================================================================================

    private long lastLogTerm() {
        return storage.termAt(storage.lastIndex());
    }

    private void emitAppended(List<LogEntry> entries, String detail) {
        for (LogEntry e : entries) {
            emit(RaftEvent.Type.ENTRY_APPENDED, e.index(), e.term(), e.checksum(), null, detail);
        }
    }

    private void emit(RaftEvent.Type type, long index, long entryTerm, long checksum, String peer, String detail) {
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
