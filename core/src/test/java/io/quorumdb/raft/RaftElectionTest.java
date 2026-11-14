package io.quorumdb.raft;

import io.quorumdb.raft.RaftMessage.AppendEntries;
import io.quorumdb.raft.RaftMessage.RequestVote;
import io.quorumdb.raft.RaftMessage.RequestVoteReply;
import io.quorumdb.raft.RaftNode.Role;
import io.quorumdb.storage.MemoryStorage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.quorumdb.raft.RaftHarness.ELECTION_TIMEOUT;
import static io.quorumdb.raft.RaftHarness.HEARTBEAT;
import static io.quorumdb.raft.RaftHarness.append;
import static io.quorumdb.raft.RaftHarness.entry;
import static org.assertj.core.api.Assertions.assertThat;

/** Leader election, voting rules and term handling (Raft §5.1, §5.2, §5.4.1). */
class RaftElectionTest {

    @Test
    void followerStartsElectionAfterTimeout() {
        RaftHarness h = RaftHarness.classic();
        assertThat(h.node.role()).isEqualTo(Role.FOLLOWER);
        h.advance(ELECTION_TIMEOUT - 1);
        assertThat(h.outbox).as("nothing before the timeout").isEmpty();

        h.advance(1);
        assertThat(h.node.role()).isEqualTo(Role.CANDIDATE);
        assertThat(h.node.currentTerm()).isEqualTo(1);
        assertThat(h.disk.votedFor()).as("votes for itself, durably").isEqualTo("n1");
        assertThat(h.sent(RequestVote.class)).hasSize(2).allSatisfy(v -> {
            assertThat(v.term()).isEqualTo(1);
            assertThat(v.preVote()).isFalse();
        });
    }

    @Test
    void candidateBecomesLeaderWithMajorityAndAppendsNoOp() {
        RaftHarness h = RaftHarness.classic();
        h.advance(ELECTION_TIMEOUT);
        h.outbox.clear();
        h.receive(new RequestVoteReply("n2", 1, 1, true, false));

        assertThat(h.node.role()).isEqualTo(Role.LEADER);
        assertThat(h.disk.lastIndex()).isEqualTo(1);
        assertThat(h.disk.entry(1).type()).isEqualTo(LogEntry.Type.NOOP);
        assertThat(h.sent(AppendEntries.class)).hasSize(2).allSatisfy(a -> {
            assertThat(a.prevLogIndex()).isZero();
            assertThat(a.entries()).hasSize(1);
        });
        assertThat(h.saw(RaftEvent.Type.LEADER_ELECTED)).isTrue();
    }

    @Test
    void candidateNeedsAMajorityAndCountsEachVoterOnce() {
        RaftHarness h = new RaftHarness(5, false, false, 0);
        h.advance(ELECTION_TIMEOUT);
        h.receive(new RequestVoteReply("n2", 1, 1, true, false));
        h.receive(new RequestVoteReply("n2", 1, 1, true, false)); // duplicate delivery
        h.receive(new RequestVoteReply("n3", 1, 1, false, false));
        assertThat(h.node.role()).as("2 of 5 votes is not a majority").isEqualTo(Role.CANDIDATE);
        h.receive(new RequestVoteReply("n4", 1, 1, true, false));
        assertThat(h.node.role()).isEqualTo(Role.LEADER);
    }

    @Test
    void votesFromAnEarlierElectionRoundAreIgnored() {
        RaftHarness h = RaftHarness.classic();
        h.advance(ELECTION_TIMEOUT);       // term 1
        h.advance(ELECTION_TIMEOUT);       // term 2: split vote, try again
        assertThat(h.node.currentTerm()).isEqualTo(2);
        h.receive(new RequestVoteReply("n2", 1, 1, true, false)); // late grant for term 1
        assertThat(h.node.role()).isEqualTo(Role.CANDIDATE);
    }

    @Test
    void preVoteDoesNotChangeTermUntilAMajorityWouldVote() {
        RaftHarness h = new RaftHarness(3, true, false, 0);
        h.advance(ELECTION_TIMEOUT);
        assertThat(h.node.role()).isEqualTo(Role.PRE_CANDIDATE);
        assertThat(h.node.currentTerm()).as("term untouched by pre-vote").isZero();
        assertThat(h.disk.votedFor()).isNull();
        assertThat(h.sent(RequestVote.class)).allSatisfy(v -> {
            assertThat(v.preVote()).isTrue();
            assertThat(v.term()).isEqualTo(1);
        });

        h.advance(ELECTION_TIMEOUT * 5); // nobody answers: it keeps asking, the term stays put
        assertThat(h.node.currentTerm()).isZero();

        h.receive(new RequestVoteReply("n2", 0, 1, true, true));
        assertThat(h.node.role()).isEqualTo(Role.CANDIDATE);
        assertThat(h.node.currentTerm()).isEqualTo(1);
    }

    @Test
    void grantingAPreVoteChangesNothing() {
        RaftHarness h = new RaftHarness(3, true, false, 0);
        h.receive(new RequestVote("n2", 5, 0, 0, true));
        RequestVoteReply reply = h.lastSent(RequestVoteReply.class, "n2");
        assertThat(reply.granted()).isTrue();
        assertThat(h.node.currentTerm()).isZero();
        assertThat(h.disk.votedFor()).isNull();
    }

    @Test
    void grantsAtMostOneVotePerTerm() {
        RaftHarness h = RaftHarness.classic();
        h.receive(new RequestVote("n2", 1, 0, 0, false));
        assertThat(h.lastSent(RequestVoteReply.class, "n2").granted()).isTrue();
        assertThat(h.disk.votedFor()).isEqualTo("n2");

        h.receive(new RequestVote("n3", 1, 0, 0, false));
        assertThat(h.lastSent(RequestVoteReply.class, "n3").granted()).as("already voted for n2 in term 1").isFalse();

        h.receive(new RequestVote("n2", 1, 0, 0, false));
        assertThat(h.lastSent(RequestVoteReply.class, "n2").granted()).as("re-delivery to the same candidate").isTrue();

        h.receive(new RequestVote("n3", 2, 0, 0, false));
        assertThat(h.lastSent(RequestVoteReply.class, "n3").granted()).as("new term, new vote").isTrue();
        assertThat(h.disk.currentTerm()).isEqualTo(2);
    }

    @Test
    void voteSurvivesRestart() {
        MemoryStorage disk = new MemoryStorage();
        RaftHarness before = new RaftHarness(3, false, false, 0, disk);
        before.receive(new RequestVote("n2", 3, 0, 0, false));
        assertThat(before.lastSent(RequestVoteReply.class, "n2").granted()).isTrue();

        RaftHarness after = new RaftHarness(3, false, false, 0, disk); // process restarted on the same disk
        assertThat(after.node.currentTerm()).isEqualTo(3);
        after.receive(new RequestVote("n3", 3, 0, 0, false));
        assertThat(after.lastSent(RequestVoteReply.class, "n3").granted())
                .as("must remember it already voted for n2 in term 3").isFalse();
    }

    @Test
    void refusesCandidateWhoseLogIsBehind() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 2, 0, 0, 0, entry(1, 1, "a"), entry(2, 2, "b")));
        h.outbox.clear();

        h.receive(new RequestVote("n3", 3, 5, 1, false)); // longer log but older last term
        assertThat(h.lastSent(RequestVoteReply.class, "n3").granted()).isFalse();
        assertThat(h.node.currentTerm()).as("still adopts the higher term").isEqualTo(3);

        h.receive(new RequestVote("n3", 4, 1, 2, false)); // same last term, shorter log
        assertThat(h.lastSent(RequestVoteReply.class, "n3").granted()).isFalse();

        h.receive(new RequestVote("n3", 5, 2, 2, false)); // exactly as up to date
        assertThat(h.lastSent(RequestVoteReply.class, "n3").granted()).isTrue();
    }

    @Test
    void leaderStepsDownWhenItSeesAHigherTerm() {
        RaftHarness h = RaftHarness.classic();
        h.electLeader();
        h.receive(append("n3", 5, 0, 0, 0));
        assertThat(h.node.role()).isEqualTo(Role.FOLLOWER);
        assertThat(h.node.currentTerm()).isEqualTo(5);
        assertThat(h.node.leaderId()).isEqualTo("n3");
        assertThat(h.disk.votedFor()).as("vote reset for the new term").isNull();
    }

    @Test
    void candidateYieldsToLeaderOfSameTerm() {
        RaftHarness h = RaftHarness.classic();
        h.advance(ELECTION_TIMEOUT);
        h.receive(append("n2", 1, 0, 0, 0));
        assertThat(h.node.role()).isEqualTo(Role.FOLLOWER);
        assertThat(h.node.leaderId()).isEqualTo("n2");
    }

    @Test
    void heartbeatsPreventElections() {
        RaftHarness h = RaftHarness.classic();
        for (int i = 0; i < 20; i++) {
            h.advance(ELECTION_TIMEOUT / 2);
            h.receive(append("n2", 1, 0, 0, 0));
        }
        assertThat(h.node.role()).isEqualTo(Role.FOLLOWER);
        assertThat(h.sent(RequestVote.class)).isEmpty();
    }

    @Test
    void followerWithFreshLeaderIgnoresVoteRequests() {
        RaftHarness h = new RaftHarness(3, true, true, 0);
        h.receive(append("n2", 1, 0, 0, 0));
        h.outbox.clear();
        h.receive(new RequestVote("n3", 9, 100, 9, false));
        assertThat(h.lastSent(RequestVoteReply.class, "n3").granted()).isFalse();
        assertThat(h.node.currentTerm()).as("a disruptive candidate cannot bump our term").isEqualTo(1);

        h.advance(ELECTION_TIMEOUT); // leader went quiet: now the request is considered
        h.receive(new RequestVote("n3", 9, 100, 9, false));
        assertThat(h.lastSent(RequestVoteReply.class, "n3").granted()).isTrue();
    }

    @Test
    void leaderWithoutQuorumContactStepsDown() {
        RaftHarness h = new RaftHarness(3, false, true, 0);
        h.electLeader();
        h.advance(ELECTION_TIMEOUT * 2 + HEARTBEAT);
        assertThat(h.node.role()).as("check-quorum").isEqualTo(Role.FOLLOWER);
        assertThat(h.node.currentTerm()).as("stepping down does not change the term").isEqualTo(1);
    }

    @Test
    void leaderKeepsLeadingWhileAMajorityAnswers() {
        RaftHarness h = new RaftHarness(3, false, true, 0);
        h.electLeader();
        for (int i = 0; i < 30; i++) {
            h.advance(HEARTBEAT);
            h.ack("n2", 1); // n3 is silent, but n1 + n2 is a majority
        }
        assertThat(h.node.role()).isEqualTo(Role.LEADER);
    }

    @Test
    void singleNodeClusterElectsItself() {
        RaftHarness h = new RaftHarness(1, true, true, 0);
        h.advance(ELECTION_TIMEOUT);
        assertThat(h.node.role()).isEqualTo(Role.LEADER);
        assertThat(h.node.commitIndex()).as("its own no-op commits immediately").isEqualTo(1);
    }

    @Test
    void messagesFromNonMembersAreIgnored() {
        RaftHarness h = RaftHarness.classic();
        h.receive(new RequestVote("intruder", 50, 0, 0, false));
        h.receive(append("intruder", 50, 0, 0, 0));
        assertThat(h.node.currentTerm()).isZero();
        assertThat(h.outbox).isEmpty();
    }

    @Test
    void staleTermMessagesAreRejectedWithCurrentTerm() {
        RaftHarness h = RaftHarness.classic();
        h.receive(append("n2", 5, 0, 0, 0));
        h.outbox.clear();
        h.receive(append("n3", 3, 0, 0, 0));
        RaftMessage.AppendEntriesReply reply = h.lastSent(RaftMessage.AppendEntriesReply.class, "n3");
        assertThat(reply.success()).isFalse();
        assertThat(reply.term()).isEqualTo(5);
        assertThat(h.node.leaderId()).isEqualTo("n2");

        h.receive(new RequestVote("n3", 4, 0, 0, false));
        assertThat(h.lastSent(RequestVoteReply.class, "n3").granted()).isFalse();
        assertThat(List.of(h.node.currentTerm())).containsExactly(5L);
    }
}
