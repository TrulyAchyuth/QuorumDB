package io.quorumdb.raft;

import io.quorumdb.raft.Collaborators.Scheduler;
import io.quorumdb.raft.Collaborators.Transport;
import io.quorumdb.raft.RaftMessage.RequestVote;
import io.quorumdb.raft.RaftMessage.RequestVoteReply;
import io.quorumdb.raft.RaftNode.Progress;
import io.quorumdb.raft.RaftNode.Role;
import io.quorumdb.storage.RaftStorage;

import java.util.List;

/**
 * Leader election (Raft §5.2, §5.4.1) with PreVote and leader stickiness (thesis §9.6, §4.2.3), and the
 * role transitions to follower and leader. Operates on the state held by its {@link RaftNode}; like the
 * node, it runs on the node thread only.
 */
final class Elections {
    private final RaftNode n;
    private final RaftConfig cfg;
    private final String id;
    private final RaftStorage storage;
    private final Transport transport;
    private final Scheduler scheduler;

    Elections(RaftNode node) {
        this.n = node;
        this.cfg = node.cfg;
        this.id = node.id;
        this.storage = node.storage;
        this.transport = node.transport;
        this.scheduler = node.scheduler;
    }

    void resetElectionTimer() {
        if (n.electionTimer != null) n.electionTimer.cancel();
        long span = cfg.electionTimeoutMaxMs() - cfg.electionTimeoutMinMs();
        long timeout = cfg.electionTimeoutMinMs() + n.random.nextLong(span);
        n.electionTimer = scheduler.schedule(timeout, this::onElectionTimeout);
    }

    private void onElectionTimeout() {
        if (!n.running || n.role == Role.LEADER) return;
        if (!n.voters.contains(id)) { // not (or no longer) a voting member: never campaign
            resetElectionTimer();
            return;
        }
        n.guarded(() -> {
            n.emit(RaftEvent.Type.ELECTION_TIMEOUT, 0, 0, 0, n.leaderId, "no leader contact within election timeout");
            n.leaderId = null;
            campaign(cfg.preVote());
        });
    }

    private void campaign(boolean preVote) {
        n.votes.clear();
        n.votes.add(id);
        long campaignTerm;
        if (preVote) {
            // Pre-vote: ask whether we *could* win without bumping our term, so a node that
            // cannot reach a majority does not inflate terms and later depose a healthy leader.
            n.role = Role.PRE_CANDIDATE;
            campaignTerm = storage.currentTerm() + 1;
            n.emit(RaftEvent.Type.PRE_VOTE_STARTED, storage.lastIndex(), n.lastLogTerm(), 0, null, "for term " + campaignTerm);
        } else {
            n.role = Role.CANDIDATE;
            campaignTerm = storage.currentTerm() + 1;
            storage.saveHardState(campaignTerm, id); // vote for self, durably, before asking others
            n.emit(RaftEvent.Type.TERM_ADVANCED, 0, 0, 0, null, "campaigning");
            n.emit(RaftEvent.Type.ELECTION_STARTED, storage.lastIndex(), n.lastLogTerm(), 0, null, null);
        }
        resetElectionTimer();
        if (n.votes.size() >= n.quorum()) {
            wonRound(preVote);
            return;
        }
        RequestVote request = new RequestVote(id, campaignTerm, storage.lastIndex(), n.lastLogTerm(), preVote);
        for (String peer : n.otherVoters()) {
            n.emit(RaftEvent.Type.VOTE_REQUESTED, storage.lastIndex(), n.lastLogTerm(), 0, peer, preVote ? "pre-vote" : null);
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

    void onRequestVote(RequestVote m) {
        long now = scheduler.nowMs();
        boolean leaderIsFresh = n.role == Role.LEADER
                || (n.leaderId != null && now - n.lastLeaderContactMs < cfg.electionTimeoutMinMs());
        if ((cfg.preVote() || cfg.checkQuorum()) && leaderIsFresh) {
            // Leader stickiness (thesis §4.2.3): while we believe a live leader exists, a vote request
            // is disregarded entirely: no vote, no term change. The reply carries our term untouched.
            n.emit(RaftEvent.Type.VOTE_DENIED, 0, 0, 0, m.from(),
                    (m.preVote() ? "pre-vote: " : "") + "current leader still fresh");
            transport.send(m.from(), new RequestVoteReply(id, storage.currentTerm(), m.term(), false, m.preVote()));
            return;
        }
        boolean upToDate = m.lastLogTerm() > n.lastLogTerm()
                || (m.lastLogTerm() == n.lastLogTerm() && m.lastLogIndex() >= storage.lastIndex());

        if (m.preVote()) {
            // Granting a pre-vote changes no state at all.
            boolean grant = m.term() > storage.currentTerm() && upToDate;
            n.emit(grant ? RaftEvent.Type.VOTE_GRANTED : RaftEvent.Type.VOTE_DENIED, 0, 0, 0, m.from(),
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
            n.emit(RaftEvent.Type.VOTE_GRANTED, 0, 0, 0, m.from(), null);
        } else {
            String why = m.term() < storage.currentTerm() ? "stale term " + m.term()
                    : !upToDate ? "candidate log not up to date"
                    : "already voted for " + votedFor;
            n.emit(RaftEvent.Type.VOTE_DENIED, 0, 0, 0, m.from(), why);
        }
        transport.send(m.from(), new RequestVoteReply(id, storage.currentTerm(), m.term(), grant, false));
    }

    void onRequestVoteReply(RequestVoteReply m) {
        if (m.term() > storage.currentTerm()) {
            becomeFollower(m.term(), null, "saw higher term in vote reply from " + m.from());
            return;
        }
        boolean forCurrentRound = m.preVote()
                ? n.role == Role.PRE_CANDIDATE && m.requestTerm() == storage.currentTerm() + 1
                : n.role == Role.CANDIDATE && m.requestTerm() == storage.currentTerm();
        if (!forCurrentRound || !m.granted()) return;
        if (n.votes.add(m.from()) && n.votes.size() >= n.quorum()) {
            wonRound(m.preVote());
        }
    }

    void becomeFollower(long term, String leader, String reason) {
        boolean wasLeader = n.role == Role.LEADER;
        if (term > storage.currentTerm()) {
            storage.saveHardState(term, null);
            n.emit(RaftEvent.Type.TERM_ADVANCED, 0, 0, 0, leader, reason);
        }
        if (n.role != Role.FOLLOWER) {
            n.emit(RaftEvent.Type.STEPPED_DOWN, 0, 0, 0, leader, "was " + n.role + ": " + reason);
        }
        n.role = Role.FOLLOWER;
        n.leaderId = leader;
        n.votes.clear();
        // A held-back acknowledgement was verified against the previous leader's log, not the next one's.
        n.awaitingAckIndex = 0;
        n.awaitingAckReadSeq = 0;
        if (wasLeader) {
            if (n.heartbeatTimer != null) n.heartbeatTimer.cancel();
            n.heartbeatTimer = null;
            n.progress.clear();
            n.failPendingWork(reason);
        }
        resetElectionTimer();
    }

    private void becomeLeader() {
        n.role = Role.LEADER;
        n.leaderId = id;
        if (n.electionTimer != null) n.electionTimer.cancel();
        n.electionTimer = null;
        long now = scheduler.nowMs();
        n.progress.clear();
        for (String peer : n.otherVoters()) {
            Progress p = new Progress();
            p.nextIndex = storage.lastIndex() + 1;
            p.lastAckMs = now; // grace period for CheckQuorum
            n.progress.put(peer, p);
        }
        n.readSeq = 0;
        // A leader may only count replicas for entries of its own term (§5.4.2), so it appends a
        // no-op immediately; committing it also commits every earlier entry.
        LogEntry noop = new LogEntry(storage.lastIndex() + 1, storage.currentTerm(), LogEntry.Type.NOOP, new byte[0]);
        storage.append(List.of(noop));
        n.termStartIndex = noop.index();
        n.emit(RaftEvent.Type.LEADER_ELECTED, noop.index(), noop.term(), 0, null, "votes=" + n.votes);
        n.emitAppended(List.of(noop), "leader no-op");
        n.replication.broadcastAppend(true);
        n.heartbeatTimer = scheduler.schedule(cfg.heartbeatIntervalMs(), n.replication::onHeartbeatTimer);
        n.replication.advanceCommitIndex();
    }
}
