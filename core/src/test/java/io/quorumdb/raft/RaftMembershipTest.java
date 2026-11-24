package io.quorumdb.raft;

import io.quorumdb.raft.Outcomes.Status;
import io.quorumdb.raft.RaftMessage.AppendEntries;
import io.quorumdb.raft.RaftMessage.AppendEntriesReply;
import io.quorumdb.raft.RaftMessage.RequestVote;
import io.quorumdb.raft.RaftNode.Role;
import io.quorumdb.storage.MemoryStorage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.quorumdb.raft.RaftHarness.ELECTION_TIMEOUT;
import static io.quorumdb.raft.RaftHarness.append;
import static io.quorumdb.raft.RaftHarness.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Single-server membership changes (Raft dissertation §4.1). */
class RaftMembershipTest {

    private static LogEntry config(long index, long term, String... members) {
        return new LogEntry(index, term, LogEntry.Type.CONFIG, String.join("\n", members).getBytes());
    }

    /** Leader of term 1 with its no-op committed (a change needs one entry of its own term committed first). */
    private static RaftHarness leaderWithCommittedNoop() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        h.ack("n2", 1);
        assertThat(h.node.commitIndex()).isEqualTo(1);
        return h;
    }

    @Test
    void addedMemberCountsTowardsTheNewMajorityAndIsReplicatedTo() {
        RaftHarness h = leaderWithCommittedNoop();
        List<Outcomes.Proposal> results = new ArrayList<>();
        h.node.addMember("n4=host4:9104=http://host4:8084", results::add);
        assertThat(h.node.members()).as("in effect as soon as it is appended")
                .containsExactly("n1", "n2", "n3", "n4=host4:9104=http://host4:8084");
        assertThat(h.lastSent(AppendEntries.class, "n4").prevLogIndex()).as("probe, as for any new follower").isEqualTo(2);
        h.receive(new AppendEntriesReply("n4", 1, false, 0, 1, 0, 0)); // empty log: back up to index 1
        assertThat(h.lastSent(AppendEntries.class, "n4").entries()).extracting(LogEntry::index).containsExactly(1L, 2L);

        h.ack("n2", 2);
        assertThat(h.node.commitIndex()).as("2 of 4 is not a majority of the new configuration").isEqualTo(1);
        h.ack("n4", 2);
        assertThat(h.node.commitIndex()).isEqualTo(2);
        assertThat(results).extracting(Outcomes.Proposal::status).containsExactly(Status.OK);
        assertThat(h.saw(RaftEvent.Type.MEMBERSHIP_CHANGED)).isTrue();
    }

    @Test
    void oneChangeAtATimeAndOnlyAfterCommittingAnEntryOfTheLeadersTerm() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        List<Status> statuses = new ArrayList<>();
        h.node.addMember("n4", r -> statuses.add(r.status()));
        assertThat(statuses).as("no-op of this term not committed yet").containsExactly(Status.BUSY);

        h.ack("n2", 1);
        h.node.addMember("n4", r -> statuses.add(r.status()));
        h.node.addMember("n5", r -> statuses.add(r.status()));
        assertThat(statuses).as("second change while the first is uncommitted").containsExactly(Status.BUSY, Status.BUSY);
        assertThat(h.node.members()).containsExactly("n1", "n2", "n3", "n4");
    }

    @Test
    void invalidChangesAreRejectedWithoutTouchingTheLog() {
        RaftHarness h = leaderWithCommittedNoop();
        assertThatThrownBy(() -> h.node.addMember("n2=elsewhere:1", r -> { })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> h.node.removeMember("n9", r -> { })).isInstanceOf(IllegalArgumentException.class);
        assertThat(h.disk.lastIndex()).isEqualTo(1);
        assertThat(h.node.isRunning()).isTrue();
    }

    @Test
    void followerRejectsChangesWithNotLeader() {
        RaftHarness h = RaftHarness.classic();
        List<Status> statuses = new ArrayList<>();
        h.node.addMember("n4", r -> statuses.add(r.status()));
        assertThat(statuses).containsExactly(Status.NOT_LEADER);
    }

    @Test
    void leaderRemovingItselfDoesNotCountItselfStepsDownWhenCommittedAndNeverCampaignsAgain() {
        RaftHarness h = leaderWithCommittedNoop();
        List<Status> statuses = new ArrayList<>();
        h.node.removeMember("n1", r -> statuses.add(r.status()));
        assertThat(h.node.members()).containsExactly("n2", "n3");

        h.ack("n2", 2);
        assertThat(h.node.commitIndex()).as("needs 2 of {n2, n3}; the leader itself no longer counts").isEqualTo(1);
        h.ack("n3", 2);
        assertThat(h.node.commitIndex()).isEqualTo(2);
        assertThat(statuses).containsExactly(Status.OK);
        assertThat(h.node.role()).isEqualTo(Role.FOLLOWER);

        h.outbox.clear();
        h.advance(ELECTION_TIMEOUT * 5);
        assertThat(h.sent(RequestVote.class)).as("a removed node never campaigns").isEmpty();
    }

    @Test
    void followerUsesAppendedConfigurationAndRevertsItWhenTheEntryIsTruncated() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 1, 0, 0, 0, entry(1, 1, "a"), config(2, 1, "n1", "n2", "n3", "n4")));
        assertThat(h.node.members()).containsExactly("n1", "n2", "n3", "n4");

        h.receive(append("n3", 2, 1, 1, 0, entry(2, 2, "b"))); // a new leader overwrites the uncommitted change
        assertThat(h.node.members()).containsExactly("n1", "n2", "n3");
    }

    /** Regression: followers ignored the removed leader, so it could never commit its own removal. */
    @Test
    void followerKeepsAcceptingEntriesFromALeaderThatIsRemovingItself() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 1, 0, 0, 0, entry(1, 1, "a"), config(2, 1, "n1", "n3")));
        assertThat(h.node.members()).containsExactly("n1", "n3");
        assertThat(h.lastSent(AppendEntriesReply.class, "n2").matchIndex()).isEqualTo(2);

        h.receive(append("n2", 1, 2, 1, 2, entry(3, 1, "b")));
        assertThat(h.lastSent(AppendEntriesReply.class, "n2").matchIndex()).isEqualTo(3);
        assertThat(h.node.commitIndex()).as("the removal commits through the leader being removed").isEqualTo(2);
    }

    @Test
    void removedMembersAreIgnored() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 1, 0, 0, 0, config(1, 1, "n1", "n2")));
        h.outbox.clear();
        h.advance(ELECTION_TIMEOUT * 3); // leader n2 is silent, so n3's request would not be refused for stickiness
        h.outbox.clear();
        h.receive(new RequestVote("n3", 5, 10, 1, false));
        assertThat(h.sent(RaftMessage.RequestVoteReply.class)).isEmpty();
        assertThat(h.node.currentTerm()).as("a removed node cannot inflate our term").isLessThan(5);
    }

    @Test
    void joiningNodeDoesNotCampaignUntilAConfigurationAddsIt() {
        RaftHarness h = new RaftHarness(3, false, false, 0, new MemoryStorage(), true);
        assertThat(h.node.members()).containsExactly("n2", "n3");
        h.advance(ELECTION_TIMEOUT * 5);
        assertThat(h.sent(RequestVote.class)).isEmpty();

        h.receive(append("n2", 1, 0, 0, 0, config(1, 1, "n2", "n3", "n1")));
        assertThat(h.lastSent(AppendEntriesReply.class, "n2").success()).isTrue();
        h.advance(ELECTION_TIMEOUT * 3);
        assertThat(h.sent(RequestVote.class)).as("now a voter, it campaigns when the leader goes quiet").isNotEmpty();
    }

    @Test
    void snapshotKeepsTheConfigurationAcrossARestart() {
        MemoryStorage disk = new MemoryStorage();
        RaftHarness before = new RaftHarness(3, false, false, 3, disk);
        before.receive(append("n2", 1, 0, 0, 4, entry(1, 1, "a"), config(2, 1, "n1", "n2", "n3", "n4=h:1=http://h:2"),
                entry(3, 1, "b"), entry(4, 1, "c")));
        assertThat(disk.snapshotIndex()).as("snapshot covers the configuration entry").isEqualTo(4);

        RaftHarness after = new RaftHarness(3, false, false, 3, disk);
        assertThat(after.node.members()).containsExactly("n1", "n2", "n3", "n4=h:1=http://h:2");
        assertThat(after.applied).containsExactly("a", "b", "c");
    }
}
