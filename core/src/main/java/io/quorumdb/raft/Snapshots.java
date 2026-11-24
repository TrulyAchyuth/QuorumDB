package io.quorumdb.raft;

import io.quorumdb.raft.Collaborators.Transport;
import io.quorumdb.raft.RaftMessage.AppendEntries;
import io.quorumdb.raft.RaftMessage.AppendEntriesReply;
import io.quorumdb.raft.RaftMessage.InstallSnapshot;
import io.quorumdb.raft.RaftMessage.InstallSnapshotReply;
import io.quorumdb.raft.RaftNode.Progress;
import io.quorumdb.raft.RaftNode.Role;
import io.quorumdb.storage.RaftStorage;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Snapshots and log compaction (Raft §7): taking a local snapshot, sending one to a follower that needs
 * compacted entries, and installing one received from the leader. Operates on the state held by its
 * {@link RaftNode}, on the node thread only.
 */
final class Snapshots {
    private final RaftNode n;
    private final RaftConfig cfg;
    private final String id;
    private final RaftStorage storage;
    private final Transport transport;

    // leader: the snapshot being sent, loaded once rather than once per chunk
    private long cachedSnapshotIndex = -1;
    private byte[] cachedSnapshot;

    // follower: chunks of the snapshot being received, identified by leader, term and index
    private String receivingFrom;
    private long receivingTerm;
    private long receivingIndex;
    private ByteArrayOutputStream receiving;

    Snapshots(RaftNode node) {
        this.n = node;
        this.cfg = node.cfg;
        this.id = node.id;
        this.storage = node.storage;
        this.transport = node.transport;
    }

    /** Snapshots the state machine at {@code lastApplied} and compacts the log prefix. */
    void snapshotNow() {
        long index = n.lastApplied;
        long term = storage.termAt(index);
        byte[] data = n.stateMachine.snapshot();
        storage.saveSnapshot(index, term, data, cfg.snapshotTrailingEntries());
        n.emit(RaftEvent.Type.SNAPSHOT_CREATED, index, term, 0, null, data.length + " bytes");
    }

    /** Leader: the entries {@code peer} needs were compacted away, so it gets the snapshot instead. */
    void send(String peer, Progress p, boolean force, long now) {
        // Still send a heartbeat so it keeps recognising us while the (potentially large) snapshot is in flight.
        if (force) {
            transport.send(peer, new AppendEntries(id, storage.currentTerm(), storage.snapshotIndex(),
                    storage.snapshotTerm(), List.of(), n.commitIndex, n.readSeq));
        }
        // One snapshot in flight at a time: send again only for a newer snapshot, or after a timeout
        // (the message, or its reply, was lost).
        // A lost chunk (or its reply) restarts the transfer from the beginning.
        if (p.snapshotSentIndex != storage.snapshotIndex() || now - p.snapshotSentMs >= cfg.electionTimeoutMaxMs()) {
            p.snapshotSentIndex = storage.snapshotIndex();
            p.snapshotOffset = 0;
            n.emit(RaftEvent.Type.SNAPSHOT_SENT, storage.snapshotIndex(), storage.snapshotTerm(), 0, peer, null);
            sendSnapshotChunk(peer, p);
        }
    }

    /** Sends the chunk of the current snapshot that starts at {@code p.snapshotOffset}, at most maxBytesPerAppend. */
    private void sendSnapshotChunk(String peer, Progress p) {
        if (cachedSnapshotIndex != storage.snapshotIndex()) {
            cachedSnapshot = storage.loadSnapshot();
            cachedSnapshotIndex = storage.snapshotIndex();
        }
        int from = (int) p.snapshotOffset;
        int to = (int) Math.min(cachedSnapshot.length, (long) from + cfg.maxBytesPerAppend());
        p.snapshotSentMs = n.scheduler.nowMs();
        transport.send(peer, new InstallSnapshot(id, storage.currentTerm(), storage.snapshotIndex(),
                storage.snapshotTerm(), from, Arrays.copyOfRange(cachedSnapshot, from, to),
                to < cachedSnapshot.length, n.readSeq));
    }

    /** Leader: a follower confirmed a non-final chunk. */
    void onInstallSnapshotReply(InstallSnapshotReply m) {
        if (m.term() > storage.currentTerm()) {
            n.elections.becomeFollower(m.term(), null, "saw higher term in InstallSnapshot reply from " + m.from());
            return;
        }
        if (n.role != Role.LEADER || m.term() != storage.currentTerm()) return; // stale reply
        Progress p = n.progress.get(m.from());
        if (p == null) return;
        p.lastAckMs = n.scheduler.nowMs();
        p.ackedReadSeq = Math.max(p.ackedReadSeq, m.readSeq());
        // Only a reply for the transfer in progress that moves it forward sends the next chunk; stale or
        // duplicated replies do nothing.
        if (p.snapshotSentIndex == m.lastIncludedIndex() && m.lastIncludedIndex() == storage.snapshotIndex()
                && m.nextOffset() > p.snapshotOffset && m.nextOffset() < cachedSnapshotLength()) {
            p.snapshotOffset = m.nextOffset();
            sendSnapshotChunk(m.from(), p);
        }
        n.serveReads();
    }

    private long cachedSnapshotLength() {
        return cachedSnapshotIndex == storage.snapshotIndex() && cachedSnapshot != null ? cachedSnapshot.length : 0;
    }

    /** Follower: installs the leader's snapshot unless our committed state already covers it. */
    void onInstallSnapshot(InstallSnapshot m) {
        if (m.term() < storage.currentTerm()) {
            transport.send(m.from(), new AppendEntriesReply(id, storage.currentTerm(), false, 0, 0, 0, m.readSeq()));
            return;
        }
        if (!n.replication.acceptLeader(m.from(), m.term())) return;
        if (m.lastIncludedIndex() > n.commitIndex) {
            byte[] data = assembleSnapshot(m);
            if (data == null) return; // more chunks to come, or this one was out of order
            storage.installSnapshot(m.lastIncludedIndex(), m.lastIncludedTerm(), data);
            n.stateMachine.restore(data);
            n.commitIndex = m.lastIncludedIndex();
            n.lastApplied = m.lastIncludedIndex();
            n.emit(RaftEvent.Type.SNAPSHOT_INSTALLED, m.lastIncludedIndex(), m.lastIncludedTerm(), 0, m.from(),
                    data.length + " bytes");
        }
        // Whether installed or already covered, our committed prefix matches the leader up to here.
        transport.send(m.from(), new AppendEntriesReply(id, storage.currentTerm(), true, m.lastIncludedIndex(),
                0, 0, m.readSeq()));
    }

    /**
     * Adds a chunk to the snapshot being received. Returns the whole snapshot once its last chunk has arrived,
     * otherwise null (after acknowledging the bytes received so far). Chunks are only joined if they come from
     * the same leader, term and index: two leaders' snapshots at the same index need not be byte-identical.
     */
    private byte[] assembleSnapshot(InstallSnapshot m) {
        if (m.offset() == 0 && !m.more()) {
            receiving = null;
            return m.data(); // the whole snapshot in one message
        }
        boolean sameTransfer = receiving != null && m.from().equals(receivingFrom) && m.term() == receivingTerm
                && m.lastIncludedIndex() == receivingIndex;
        if (m.offset() == 0) {
            receivingFrom = m.from();
            receivingTerm = m.term();
            receivingIndex = m.lastIncludedIndex();
            receiving = new ByteArrayOutputStream();
        } else if (!sameTransfer || m.offset() != receiving.size()) {
            // Out of order, duplicated or from another transfer: report where we are; the leader restarts if needed.
            long have = sameTransfer ? receiving.size() : 0;
            transport.send(m.from(), new InstallSnapshotReply(id, storage.currentTerm(), m.lastIncludedIndex(), have,
                    m.readSeq()));
            return null;
        }
        receiving.writeBytes(m.data());
        if (m.more()) {
            transport.send(m.from(), new InstallSnapshotReply(id, storage.currentTerm(), m.lastIncludedIndex(),
                    receiving.size(), m.readSeq()));
            return null;
        }
        byte[] data = receiving.toByteArray();
        receiving = null;
        return data;
    }
}
