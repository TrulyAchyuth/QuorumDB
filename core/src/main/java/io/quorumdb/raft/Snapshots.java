package io.quorumdb.raft;

import io.quorumdb.raft.Collaborators.Transport;
import io.quorumdb.raft.RaftMessage.AppendEntries;
import io.quorumdb.raft.RaftMessage.AppendEntriesReply;
import io.quorumdb.raft.RaftMessage.InstallSnapshot;
import io.quorumdb.raft.RaftNode.Progress;
import io.quorumdb.storage.RaftStorage;

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
        if (p.snapshotSentIndex != storage.snapshotIndex() || now - p.snapshotSentMs >= cfg.electionTimeoutMaxMs()) {
            p.snapshotSentIndex = storage.snapshotIndex();
            p.snapshotSentMs = now;
            n.emit(RaftEvent.Type.SNAPSHOT_SENT, storage.snapshotIndex(), storage.snapshotTerm(), 0, peer, null);
            transport.send(peer, new InstallSnapshot(id, storage.currentTerm(), storage.snapshotIndex(),
                    storage.snapshotTerm(), storage.loadSnapshot(), n.readSeq));
        }
    }

    /** Follower: installs the leader's snapshot unless our committed state already covers it. */
    void onInstallSnapshot(InstallSnapshot m) {
        if (m.term() < storage.currentTerm()) {
            transport.send(m.from(), new AppendEntriesReply(id, storage.currentTerm(), false, 0, 0, 0, m.readSeq()));
            return;
        }
        if (!n.replication.acceptLeader(m.from(), m.term())) return;
        if (m.lastIncludedIndex() > n.commitIndex) {
            storage.installSnapshot(m.lastIncludedIndex(), m.lastIncludedTerm(), m.data());
            n.stateMachine.restore(m.data());
            n.commitIndex = m.lastIncludedIndex();
            n.lastApplied = m.lastIncludedIndex();
            n.emit(RaftEvent.Type.SNAPSHOT_INSTALLED, m.lastIncludedIndex(), m.lastIncludedTerm(), 0, m.from(),
                    m.data().length + " bytes");
        }
        // Whether installed or already covered, our committed prefix matches the leader up to here.
        transport.send(m.from(), new AppendEntriesReply(id, storage.currentTerm(), true, m.lastIncludedIndex(),
                0, 0, m.readSeq()));
    }
}
