package io.quorumdb.raft;

import io.quorumdb.raft.Collaborators.Scheduler;
import io.quorumdb.raft.Collaborators.Transport;
import io.quorumdb.raft.RaftMessage.AppendEntries;
import io.quorumdb.raft.RaftMessage.AppendEntriesReply;
import io.quorumdb.raft.RaftNode.Progress;
import io.quorumdb.raft.RaftNode.Role;
import io.quorumdb.storage.RaftStorage;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Log replication (Raft §5.3) and the commit rule (§5.4.2): the leader side (heartbeats, AppendEntries,
 * reply handling, CheckQuorum) and the follower side (accepting a leader, consistency check, conflict
 * truncation). Operates on the state held by its {@link RaftNode}, on the node thread only.
 */
final class Replication {
    private final RaftNode n;
    private final RaftConfig cfg;
    private final String id;
    private final RaftStorage storage;
    private final Transport transport;
    private final Scheduler scheduler;

    Replication(RaftNode node) {
        this.n = node;
        this.cfg = node.cfg;
        this.id = node.id;
        this.storage = node.storage;
        this.transport = node.transport;
        this.scheduler = node.scheduler;
    }

    // ---------------------------------------------------------------- leader side

    void onHeartbeatTimer() {
        if (!n.running || n.role != Role.LEADER) return;
        n.guarded(() -> {
            long now = scheduler.nowMs();
            if (cfg.checkQuorum()) {
                int active = 1;
                for (Progress p : n.progress.values()) {
                    if (now - p.lastAckMs < cfg.electionTimeoutMaxMs()) active++;
                }
                if (active < cfg.quorum()) {
                    n.elections.becomeFollower(storage.currentTerm(), null,
                            "check-quorum: heard from only " + active + "/" + cfg.clusterSize() + " nodes");
                    return;
                }
            }
            if (cfg.emitHeartbeatEvents()) {
                n.emit(RaftEvent.Type.HEARTBEAT_SENT, n.commitIndex, 0, 0, null, null);
            }
            broadcastAppend(true);
            n.heartbeatTimer = scheduler.schedule(cfg.heartbeatIntervalMs(), this::onHeartbeatTimer);
        });
    }

    void broadcastAppend(boolean force) {
        for (Map.Entry<String, Progress> e : n.progress.entrySet()) {
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
            n.snapshots.send(peer, p, force, now);
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
                entries, n.commitIndex, n.readSeq));
    }

    void onAppendEntriesReply(AppendEntriesReply m) {
        if (m.term() > storage.currentTerm()) {
            n.elections.becomeFollower(m.term(), null, "saw higher term in AppendEntries reply from " + m.from());
            return;
        }
        if (n.role != Role.LEADER || m.term() != storage.currentTerm()) return; // stale reply
        Progress p = n.progress.get(m.from());
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
            if (n.role == Role.LEADER && p.nextIndex <= storage.lastIndex()) {
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
        if (n.role == Role.LEADER) n.serveReads();
    }

    /**
     * Advances the commit index to the highest index stored on a majority, but only if that entry is
     * from the current term (§5.4.2, Figure 8): earlier-term entries are committed indirectly.
     */
    void advanceCommitIndex() {
        if (n.role != Role.LEADER) return;
        long[] match = new long[n.progress.size() + 1];
        int i = 0;
        for (Progress p : n.progress.values()) match[i++] = p.matchIndex;
        match[i] = storage.durableIndex(); // the leader's own copy counts only once it is durable
        Arrays.sort(match);
        long majorityIndex = match[match.length - cfg.quorum()];
        if (majorityIndex > n.commitIndex && storage.termAt(majorityIndex) == storage.currentTerm()) {
            n.setCommitIndex(majorityIndex);
            // Let idle followers learn the new commit index without waiting for the next heartbeat.
            broadcastAppend(false);
        }
    }

    // ---------------------------------------------------------------- follower side

    void onAppendEntries(AppendEntries m) {
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
                if (e.index() <= n.commitIndex) {
                    throw new IllegalStateException("refusing to truncate committed entry " + e.index()
                            + " (commitIndex=" + n.commitIndex + "): safety violation");
                }
                n.emit(RaftEvent.Type.LOG_TRUNCATED, e.index(), storage.termAt(e.index()), 0, m.from(),
                        "removing [" + e.index() + ", " + storage.lastIndex() + "] conflicting with leader");
                storage.truncateSuffix(e.index());
                n.awaitingAckIndex = Math.min(n.awaitingAckIndex, e.index() - 1);
                break;
            }
            firstNew++;
        }
        if (firstNew < entries.size()) {
            List<LogEntry> toAppend = entries.subList(firstNew, entries.size());
            storage.append(toAppend); // durable before we acknowledge
            n.emitAppended(toAppend, "from leader");
        }
        if (m.leaderCommit() > n.commitIndex) {
            // Only up to the last entry verified to match the leader, never our unverified suffix.
            long target = Math.min(m.leaderCommit(), lastNewIndex);
            if (target > n.commitIndex) n.setCommitIndex(target);
        }
        if (storage.durableIndex() >= lastNewIndex) {
            reply(m, true, lastNewIndex, 0, 0);
        } else {
            // Acknowledging now could promise entries a crash would lose; onPersisted() replies instead.
            n.awaitingAckIndex = Math.max(n.awaitingAckIndex, lastNewIndex);
            n.awaitingAckReadSeq = Math.max(n.awaitingAckReadSeq, m.readSeq());
        }
    }

    private void reply(AppendEntries m, boolean success, long matchIndex, long conflictIndex, long conflictTerm) {
        transport.send(m.from(), new AppendEntriesReply(id, storage.currentTerm(), success, matchIndex,
                conflictIndex, conflictTerm, m.readSeq()));
    }

    /** Common handling for a message from a leader whose term is at least ours. */
    boolean acceptLeader(String leader, long term) {
        if (term > storage.currentTerm() || n.role != Role.FOLLOWER) {
            if (n.role == Role.LEADER && term == storage.currentTerm()) {
                throw new IllegalStateException("two leaders in term " + term + ": " + id + " and " + leader);
            }
            n.elections.becomeFollower(term, leader, "accepted leader " + leader);
        }
        n.leaderId = leader;
        n.lastLeaderContactMs = scheduler.nowMs();
        n.elections.resetElectionTimer();
        return true;
    }
}
