package io.quorumdb.raft;

import io.quorumdb.raft.Outcomes.Status;
import io.quorumdb.raft.RaftMessage.AppendEntries;
import io.quorumdb.raft.RaftMessage.AppendEntriesReply;
import io.quorumdb.raft.RaftMessage.InstallSnapshot;
import io.quorumdb.raft.RaftNode.Role;
import io.quorumdb.storage.MemoryStorage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.quorumdb.raft.RaftHarness.ELECTION_TIMEOUT;
import static io.quorumdb.raft.RaftHarness.HEARTBEAT;
import static io.quorumdb.raft.RaftHarness.append;
import static io.quorumdb.raft.RaftHarness.entry;
import static org.assertj.core.api.Assertions.assertThat;

/** Log replication, commitment, application, reads, snapshots and failure handling (Raft §5.3, §5.4.2, §7, §8). */
class RaftReplicationTest {

    // ---------------------------------------------------------------- follower side

    @Test
    void followerAppendsEntriesAndAcknowledgesMatchIndex() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 1, 0, 0, 0, entry(1, 1, "a"), entry(2, 1, "b")));
        AppendEntriesReply reply = h.lastSent(AppendEntriesReply.class, "n2");
        assertThat(reply.success()).isTrue();
        assertThat(reply.matchIndex()).isEqualTo(2);
        assertThat(h.disk.lastIndex()).isEqualTo(2);
        assertThat(h.applied).as("nothing committed yet").isEmpty();
    }

    @Test
    void followerRejectsWhenPreviousEntryIsMissing() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 1, 0, 0, 0, entry(1, 1, "a")));
        h.receive(append("n2", 1, 5, 1, 0, entry(6, 1, "f")));
        AppendEntriesReply reply = h.lastSent(AppendEntriesReply.class, "n2");
        assertThat(reply.success()).isFalse();
        assertThat(reply.conflictIndex()).as("leader should back up to our log end").isEqualTo(2);
        assertThat(reply.conflictTerm()).isZero();
        assertThat(h.disk.lastIndex()).isEqualTo(1);
    }

    @Test
    void followerRejectsWhenPreviousTermDiffersAndReportsWholeConflictingTerm() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 2, 0, 0, 0, entry(1, 1, "a"), entry(2, 2, "b"), entry(3, 2, "c"), entry(4, 2, "d")));
        h.receive(append("n3", 3, 4, 3, 0, entry(5, 3, "e")));
        AppendEntriesReply reply = h.lastSent(AppendEntriesReply.class, "n3");
        assertThat(reply.success()).isFalse();
        assertThat(reply.conflictTerm()).isEqualTo(2);
        assertThat(reply.conflictIndex()).as("first index of term 2").isEqualTo(2);
    }

    @Test
    void conflictingSuffixIsReplacedByLeadersEntries() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 1, 0, 0, 0, entry(1, 1, "a"), entry(2, 1, "stale-b"), entry(3, 1, "stale-c")));
        h.receive(append("n3", 2, 1, 1, 0, entry(2, 2, "new-b")));
        assertThat(h.disk.lastIndex()).as("entry 3 removed along with the conflicting entry 2").isEqualTo(2);
        assertThat(new String(h.disk.entry(2).data())).isEqualTo("new-b");
        assertThat(h.saw(RaftEvent.Type.LOG_TRUNCATED)).isTrue();
    }

    @Test
    void redeliveredOrReorderedAppendDoesNotDeleteNewerEntries() {
        RaftHarness h = RaftHarness.classic();
        AppendEntries first = append("n2", 1, 0, 0, 0, entry(1, 1, "a"));
        h.receive(first);
        h.receive(append("n2", 1, 1, 1, 0, entry(2, 1, "b"), entry(3, 1, "c")));
        h.receive(first); // an old message arrives late
        assertThat(h.disk.lastIndex()).as("matching entries are never truncated").isEqualTo(3);
        assertThat(h.lastSent(AppendEntriesReply.class, "n2").matchIndex())
                .as("only vouches for what this message proved").isEqualTo(1);
        assertThat(h.saw(RaftEvent.Type.LOG_TRUNCATED)).isFalse();
    }

    @Test
    void followerCommitsUpToTheLastEntryVerifiedByTheMessage() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 1, 0, 0, 0, entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")));
        // The leader says commit=3, but this message only proves our log matches up to index 1.
        h.receive(append("n2", 1, 0, 0, 3, entry(1, 1, "a")));
        assertThat(h.node.commitIndex()).isEqualTo(1);
        assertThat(h.applied).containsExactly("a");

        h.receive(append("n2", 1, 3, 1, 3));
        assertThat(h.node.commitIndex()).isEqualTo(3);
        assertThat(h.applied).containsExactly("a", "b", "c");
    }

    @Test
    void commitIndexNeverGoesBackwards() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 1, 0, 0, 2, entry(1, 1, "a"), entry(2, 1, "b")));
        h.receive(append("n2", 1, 0, 0, 1, entry(1, 1, "a"))); // stale message with older commit
        assertThat(h.node.commitIndex()).isEqualTo(2);
    }

    // ---------------------------------------------------------------- leader side

    @Test
    void leaderCommitsOnlyAfterAMajorityStoresTheEntry() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        List<Outcomes.Proposal> results = new ArrayList<>();
        h.node.propose("x".getBytes(), results::add);

        assertThat(h.disk.lastIndex()).as("no-op at 1, command at 2").isEqualTo(2);
        assertThat(h.node.commitIndex()).as("stored on the leader only").isZero();
        assertThat(results).as("not acknowledged before commit").isEmpty();

        h.ack("n2", 2);
        assertThat(h.node.commitIndex()).isEqualTo(2);
        assertThat(h.applied).containsExactly("x");
        assertThat(results).hasSize(1);
        assertThat(results.get(0).status()).isEqualTo(Status.OK);
        assertThat(results.get(0).index()).isEqualTo(2);
        assertThat(new String(results.get(0).result())).isEqualTo("result:x");
    }

    @Test
    void fiveNodeLeaderNeedsThreeCopies() {
        RaftHarness h = new RaftHarness(5, false, false, 0);
        h.advance(ELECTION_TIMEOUT);
        h.receive(new RaftMessage.RequestVoteReply("n2", 1, 1, true, false));
        h.receive(new RaftMessage.RequestVoteReply("n3", 1, 1, true, false));
        h.node.propose("x".getBytes(), r -> { });
        h.ack("n2", 2);
        assertThat(h.node.commitIndex()).as("2 of 5 copies").isZero();
        h.ack("n3", 2);
        assertThat(h.node.commitIndex()).as("3 of 5 copies").isEqualTo(2);
    }

    @Test
    void leaderDoesNotCommitEntriesFromEarlierTermsByCountingReplicas() {
        // Figure 8 of the Raft paper: an entry from an old term stored on a majority is NOT thereby committed.
        MemoryStorage disk = new MemoryStorage();
        disk.saveHardState(2, null);
        disk.append(List.of(entry(1, 2, "old-term-entry")));
        RaftHarness h = new RaftHarness(3, false, false, 0, disk);
        h.electLeader(); // becomes leader of term 3; appends its no-op at index 2
        assertThat(h.node.currentTerm()).isEqualTo(3);

        h.ack("n2", 1); // the old entry is now on a majority (n1, n2)
        assertThat(h.node.commitIndex()).as("old-term entry must not be committed directly").isZero();

        h.ack("n2", 2); // the current-term no-op reaches a majority: it and everything before it commit
        assertThat(h.node.commitIndex()).isEqualTo(2);
        assertThat(h.applied).containsExactly("old-term-entry");
    }

    @Test
    void leaderBacksUpUsingConflictHint() {
        MemoryStorage disk = new MemoryStorage();
        disk.saveHardState(3, null);
        disk.append(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 3, "c"), entry(4, 3, "d")));
        RaftHarness h = new RaftHarness(3, false, false, 0, disk);
        h.electLeader(); // term 4, no-op at 5; nextIndex for followers starts at 5
        // n2 has entries of term 2 where we have term 3: it reports conflictTerm=2 starting at index 3.
        h.receive(new AppendEntriesReply("n2", 4, false, 0, 3, 2, 0));
        AppendEntries retry = h.lastSent(AppendEntries.class, "n2");
        assertThat(retry.prevLogIndex()).as("we have no term-2 entries: jump to the follower's hint").isEqualTo(2);
        assertThat(retry.entries()).extracting(LogEntry::index).containsExactly(3L, 4L, 5L);

        // n3 is simply short.
        h.receive(new AppendEntriesReply("n3", 4, false, 0, 2, 0, 0));
        assertThat(h.lastSent(AppendEntries.class, "n3").prevLogIndex()).isEqualTo(1);
    }

    @Test
    void staleFailureReplyNeverMovesNextIndexBelowMatchIndex() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        h.node.propose("x".getBytes(), r -> { });
        h.ack("n2", 2);
        h.outbox.clear();
        h.receive(new AppendEntriesReply("n2", 1, false, 0, 1, 0, 0)); // delayed rejection from earlier
        AppendEntries resent = h.lastSent(AppendEntries.class, "n2");
        assertThat(resent.prevLogIndex()).as("n2 is known to match through 2").isEqualTo(2);
    }

    @Test
    void heartbeatRetransmitsUnacknowledgedEntries() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        h.node.propose("x".getBytes(), r -> { });
        h.outbox.clear();
        h.advance(HEARTBEAT); // the first transmission was lost; nobody answered
        assertThat(h.lastSent(AppendEntries.class, "n2").entries()).extracting(LogEntry::index).containsExactly(1L, 2L);
        assertThat(h.saw(RaftEvent.Type.HEARTBEAT_SENT)).isTrue();
    }

    @Test
    void proposalOnFollowerIsRejectedWithLeaderHint() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n3", 1, 0, 0, 0));
        List<Outcomes.Proposal> results = new ArrayList<>();
        h.node.propose("x".getBytes(), results::add);
        assertThat(results.get(0).status()).isEqualTo(Status.NOT_LEADER);
        assertThat(results.get(0).leaderHint()).isEqualTo("n3");
        assertThat(h.disk.lastIndex()).isZero();
    }

    @Test
    void pendingProposalBecomesUnknownWhenLeadershipIsLost() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        List<Outcomes.Proposal> results = new ArrayList<>();
        h.node.propose("x".getBytes(), results::add);
        h.receive(append("n3", 2, 0, 0, 0)); // a new leader appears before the entry commits
        assertThat(results).hasSize(1);
        assertThat(results.get(0).status()).as("it may still commit later: not a definite failure").isEqualTo(Status.UNKNOWN);
    }

    @Test
    void backPressureRejectsProposalsBeyondTheLimit() {
        RaftHarness h = RaftHarness.classic(); // maxPendingProposals = 100
        h.electLeader();
        List<Status> statuses = new ArrayList<>();
        for (int i = 0; i < 105; i++) h.node.propose(("c" + i).getBytes(), r -> statuses.add(r.status()));
        assertThat(statuses).hasSize(5).containsOnly(Status.BUSY);
    }

    // ---------------------------------------------------------------- linearizable reads

    @Test
    void readIndexWaitsForNoOpCommitAndAMajorityHeartbeat() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        List<Outcomes.Read> reads = new ArrayList<>();
        h.node.linearizableRead(reads::add);
        assertThat(reads).as("leadership not yet confirmed, no-op not yet committed").isEmpty();

        h.ack("n2", 1); // acknowledges the read round and commits the no-op
        assertThat(reads).hasSize(1);
        assertThat(reads.get(0).status()).isEqualTo(Status.OK);
        assertThat(reads.get(0).readIndex()).isEqualTo(1);
    }

    @Test
    void readIsNotServedOnAcknowledgementsThatPredateIt() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        h.ack("n2", 1);
        List<Outcomes.Read> reads = new ArrayList<>();
        h.node.linearizableRead(reads::add);
        // A reply to a heartbeat sent before the read arrived (old readSeq) proves nothing about now.
        h.receive(new AppendEntriesReply("n2", 1, true, 1, 0, 0, 0));
        assertThat(reads).isEmpty();
        h.ack("n2", 1); // echoes the read round's sequence
        assertThat(reads).hasSize(1);
    }

    @Test
    void deposedLeaderFailsPendingReads() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        List<Outcomes.Read> reads = new ArrayList<>();
        h.node.linearizableRead(reads::add);
        h.receive(append("n3", 2, 0, 0, 0));
        assertThat(reads).hasSize(1);
        assertThat(reads.get(0).status()).isEqualTo(Status.NOT_LEADER);
        assertThat(reads.get(0).leaderHint()).isEqualTo("n3");
    }

    @Test
    void concurrentReadsShareConfirmationRounds() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        h.ack("n2", 1);
        h.outbox.clear();
        List<Outcomes.Read> reads = new ArrayList<>();
        for (int i = 0; i < 10; i++) h.node.linearizableRead(reads::add);
        assertThat(h.sent(AppendEntries.class)).as("one round for the first read; the rest queue for the next").hasSize(2);
        h.ack("n2", 1);
        assertThat(reads).as("first round confirmed").hasSize(1);
        h.ack("n2", 1);
        assertThat(reads).as("second round served the other nine together").hasSize(10);
    }

    // ---------------------------------------------------------------- snapshots

    @Test
    void snapshotIsTakenAfterThresholdAndLogIsCompacted() {
        RaftHarness h = new RaftHarness(3, false, false, 5);
        h.electLeader();
        for (int i = 0; i < 6; i++) h.node.propose(("c" + i).getBytes(), r -> { });
        h.ack("n2", 7);
        assertThat(h.disk.snapshotIndex()).isEqualTo(7);
        assertThat(new String(h.disk.loadSnapshot())).isEqualTo("c0,c1,c2,c3,c4,c5");
        assertThat(h.saw(RaftEvent.Type.SNAPSHOT_CREATED)).isTrue();
    }

    @Test
    void leaderSendsSnapshotToFollowerThatNeedsCompactedEntries() {
        RaftHarness h = new RaftHarness(3, false, false, 5);
        h.electLeader();
        for (int i = 0; i < 6; i++) h.node.propose(("c" + i).getBytes(), r -> { });
        h.outbox.clear();
        h.ack("n2", 7); // snapshot through 7; n3 has nothing
        h.receive(new AppendEntriesReply("n3", 1, false, 0, 1, 0, 0));
        InstallSnapshot snapshot = h.lastSent(InstallSnapshot.class, "n3");
        assertThat(snapshot.lastIncludedIndex()).isEqualTo(7);
        assertThat(snapshot.lastIncludedTerm()).isEqualTo(1);

        h.receive(new AppendEntriesReply("n3", 1, true, 7, 0, 0, 0)); // n3 installed it
        h.node.propose("after".getBytes(), r -> { });
        assertThat(h.lastSent(AppendEntries.class, "n3").prevLogIndex()).isEqualTo(7);
    }

    /**
     * Regression: replies to heartbeats and read rounds cleared the follower's in-flight flag, and every
     * such reply re-sent the whole snapshot while the first one was still on its way. Under linearizable
     * read load a real cluster sent ~200 copies of a 300 KB snapshot within 30 ms, starving heartbeats.
     */
    @Test
    void snapshotIsNotResentWhileInFlightButIsRetransmittedAfterTimeout() {
        RaftHarness h = new RaftHarness(3, false, false, 5);
        h.electLeader();
        for (int i = 0; i < 6; i++) h.node.propose(("c" + i).getBytes(), r -> { });
        h.outbox.clear();
        h.ack("n2", 7); // snapshot through 7; n3 has nothing, so it is sent the snapshot right away
        h.receive(new AppendEntriesReply("n3", 1, false, 0, 1, 0, 0));
        assertThat(h.sent(InstallSnapshot.class)).hasSize(1);
        assertThat(h.lastSent(InstallSnapshot.class, "n3").lastIncludedIndex()).isEqualTo(7);

        // While the snapshot is in flight, n3 keeps answering heartbeats and read rounds it cannot match yet.
        for (int i = 0; i < 50; i++) {
            h.receive(new AppendEntriesReply("n3", 1, false, 0, 1, 0, i));
            h.receive(new AppendEntriesReply("n3", 1, true, 0, 0, 0, i));
            h.node.linearizableRead(r -> { }); // a read round: forced AppendEntries to every follower
        }
        h.advance(RaftHarness.HEARTBEAT * 3);
        assertThat(h.sent(InstallSnapshot.class)).as("snapshot sent once while in flight").hasSize(1);

        // If it was lost, it is sent again after a timeout, still only once.
        h.advance(RaftHarness.ELECTION_TIMEOUT * 2);
        h.receive(new AppendEntriesReply("n3", 1, false, 0, 1, 0, 0));
        h.receive(new AppendEntriesReply("n3", 1, false, 0, 1, 0, 0));
        assertThat(h.sent(InstallSnapshot.class)).as("retransmitted once after the timeout").hasSize(2);

        // Once n3 confirms the snapshot, replication continues with entries.
        h.receive(new AppendEntriesReply("n3", 1, true, 7, 0, 0, 0));
        h.node.propose("after".getBytes(), r -> { });
        assertThat(h.lastSent(AppendEntries.class, "n3").prevLogIndex()).isEqualTo(7);
        assertThat(h.sent(InstallSnapshot.class)).hasSize(2);
    }

    @Test
    void followerInstallsSnapshotAndContinuesFromIt() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 1, 0, 0, 0, entry(1, 1, "stale")));
        h.receive(new InstallSnapshot("n2", 2, 10, 2, "a,b,c".getBytes(), 0));
        assertThat(h.disk.snapshotIndex()).isEqualTo(10);
        assertThat(h.node.commitIndex()).isEqualTo(10);
        assertThat(h.node.lastApplied()).isEqualTo(10);
        assertThat(h.applied).containsExactly("a", "b", "c");
        assertThat(h.lastSent(AppendEntriesReply.class, "n2").matchIndex()).isEqualTo(10);

        h.receive(append("n2", 2, 10, 2, 11, entry(11, 2, "d")));
        assertThat(h.applied).containsExactly("a", "b", "c", "d");

        h.receive(new InstallSnapshot("n2", 2, 5, 1, "old".getBytes(), 0)); // stale snapshot must not roll us back
        assertThat(h.node.lastApplied()).isEqualTo(11);
        assertThat(h.applied).hasSize(4);
    }

    @Test
    void restartRestoresStateMachineFromSnapshotAndReplaysNothingUncommitted() {
        MemoryStorage disk = new MemoryStorage();
        RaftHarness before = new RaftHarness(3, false, false, 3, disk);
        before.receive(append("n2", 1, 0, 0, 4, entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c"), entry(4, 1, "d"),
                entry(5, 1, "uncommitted")));
        assertThat(disk.snapshotIndex()).isEqualTo(4);

        RaftHarness after = new RaftHarness(3, false, false, 3, disk);
        assertThat(after.applied).as("state comes back from the snapshot").containsExactly("a", "b", "c", "d");
        assertThat(after.node.commitIndex()).isEqualTo(4);
        assertThat(after.disk.lastIndex()).as("the uncommitted entry is still in the log, not applied").isEqualTo(5);
        assertThat(after.saw(RaftEvent.Type.RECOVERY_COMPLETED)).isTrue();
    }

    // ---------------------------------------------------------------- failures

    @Test
    void storageFailureMakesTheNodeFailStop() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        h.storage.setFailWrites(true);
        List<Outcomes.Proposal> results = new ArrayList<>();
        h.node.propose("x".getBytes(), results::add);

        assertThat(h.node.failure()).contains("injected storage failure");
        assertThat(h.node.isRunning()).isFalse();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).status()).as("never reported as success").isEqualTo(Status.UNKNOWN);
        assertThat(h.saw(RaftEvent.Type.NODE_FAILED)).isTrue();

        h.outbox.clear();
        h.receive(append("n2", 9, 0, 0, 0));
        h.advance(ELECTION_TIMEOUT * 3);
        assertThat(h.outbox).as("a failed node is silent").isEmpty();
    }

    @Test
    void followerDoesNotAcknowledgeEntriesItCouldNotPersist() {
        RaftHarness h = RaftHarness.classic();
        h.storage.setFailWrites(true);
        h.receive(append("n2", 1, 0, 0, 0, entry(1, 1, "a")));
        assertThat(h.sent(AppendEntriesReply.class)).isEmpty();
        assertThat(h.node.failure()).isNotNull();
    }

    @Test
    void stoppedNodeReportsUnknownForPendingWork() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        List<Status> statuses = new ArrayList<>();
        h.node.propose("x".getBytes(), r -> statuses.add(r.status()));
        h.node.stop();
        assertThat(statuses).containsExactly(Status.UNKNOWN);
        assertThat(h.node.role()).isEqualTo(Role.LEADER); // role is frozen; isLeader() reflects that it is not running
        assertThat(h.node.isLeader()).isFalse();
    }
}
