package io.quorumdb.faultlab;

import io.quorumdb.faultlab.check.NodeObservation;
import io.quorumdb.faultlab.check.NodeObservation.LogRef;
import io.quorumdb.faultlab.check.SafetyChecker;
import io.quorumdb.faultlab.check.SafetyReport;
import io.quorumdb.faultlab.check.SafetyReport.Status;
import io.quorumdb.faultlab.check.SafetyReport.Verdict;
import io.quorumdb.raft.RaftEvent;
import io.quorumdb.raft.RaftEvent.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Feeds the checker hand-written histories: each violation must be caught, and nothing else flagged. */
class SafetyCheckerTest {
    private final SafetyChecker checker = new SafetyChecker(false);
    private long seq;

    private RaftEvent event(String node, Type type, long term, long index, long entryTerm, long checksum) {
        RaftEvent e = new RaftEvent(++seq, seq * 10, node, type, term, index, entryTerm, checksum, null, null);
        checker.onEvent(e);
        return e;
    }

    private Status status(String check) {
        return check(check).status();
    }

    private SafetyReport.Check check(String id) {
        return checker.report().checks().stream().filter(c -> c.id().equals(id)).findFirst().orElseThrow();
    }

    private static NodeObservation observe(String node, String role, long term, long commit, long snapshotIndex, LogRef... log) {
        return new NodeObservation(node, 1000, term, role, commit, commit, snapshotIndex, 0, List.of(log), 0, true);
    }

    @Test
    void emptyHistoryIsNotExercisedRatherThanPassed() {
        SafetyReport report = checker.report();
        assertThat(report.verdict()).isEqualTo(Verdict.SAFE);
        assertThat(report.checks()).allSatisfy(c -> assertThat(c.status()).isEqualTo(Status.NOT_EXERCISED));
    }

    @Test
    void cleanHistoryPasses() {
        event("n1", Type.LEADER_ELECTED, 1, 1, 1, 0);
        for (String n : List.of("n1", "n2", "n3")) {
            event(n, Type.RECOVERY_COMPLETED, 1, 0, 0, 0);
            event(n, Type.ENTRY_COMMITTED, 1, 1, 1, 111);
            event(n, Type.ENTRY_APPLIED, 1, 1, 1, 111);
            event(n, Type.ENTRY_COMMITTED, 1, 2, 1, 222);
            event(n, Type.ENTRY_APPLIED, 1, 2, 1, 222);
        }
        event("n2", Type.LEADER_ELECTED, 2, 3, 2, 0);
        checker.onObservations(List.of(
                observe("n2", "LEADER", 2, 2, 0, new LogRef(1, 1, 111), new LogRef(2, 1, 222), new LogRef(3, 2, 333)),
                observe("n1", "FOLLOWER", 2, 2, 0, new LogRef(1, 1, 111), new LogRef(2, 1, 222))));
        SafetyReport report = checker.report();
        assertThat(report.verdict()).isEqualTo(Verdict.SAFE);
        assertThat(status("ELECTION_SAFETY")).isEqualTo(Status.PASS);
        assertThat(status("LEADER_COMPLETENESS")).isEqualTo(Status.PASS);
        assertThat(check("COMMIT_AGREEMENT").examined()).isEqualTo(6);
    }

    @Test
    void twoLeadersInOneTermViolatesElectionSafety() {
        RaftEvent a = event("n1", Type.LEADER_ELECTED, 3, 1, 3, 0);
        event("n1", Type.LEADER_ELECTED, 3, 1, 3, 0); // the same node again is fine
        assertThat(status("ELECTION_SAFETY")).isEqualTo(Status.PASS);
        RaftEvent b = event("n2", Type.LEADER_ELECTED, 3, 1, 3, 0);

        SafetyReport.Check c = check("ELECTION_SAFETY");
        assertThat(c.status()).isEqualTo(Status.VIOLATED);
        assertThat(c.violations().get(0).evidence()).containsExactly(a.seq(), b.seq());
        assertThat(c.violations().get(0).nodes()).containsExactly("n1", "n2");
        assertThat(checker.report().verdict()).isEqualTo(Verdict.VIOLATION);
    }

    @Test
    void differentEntriesCommittedAtSameIndexViolatesCommitAgreement() {
        event("n1", Type.ENTRY_COMMITTED, 1, 1, 1, 111);
        event("n2", Type.ENTRY_COMMITTED, 2, 1, 2, 999);
        assertThat(status("COMMIT_AGREEMENT")).isEqualTo(Status.VIOLATED);
        assertThat(check("COMMIT_AGREEMENT").violations().get(0).index()).isEqualTo(1);
    }

    @Test
    void commitIndexJumpOrRegressionViolatesProgression() {
        event("n1", Type.RECOVERY_COMPLETED, 1, 0, 0, 0);
        event("n1", Type.ENTRY_COMMITTED, 1, 1, 1, 1);
        event("n1", Type.ENTRY_COMMITTED, 1, 3, 1, 3); // skipped 2
        assertThat(status("COMMIT_PROGRESSION")).isEqualTo(Status.VIOLATED);
    }

    @Test
    void restartLegitimatelyResetsCommitAndApplyPosition() {
        event("n1", Type.RECOVERY_COMPLETED, 1, 0, 0, 0);
        event("n1", Type.ENTRY_COMMITTED, 1, 1, 1, 1);
        event("n1", Type.ENTRY_APPLIED, 1, 1, 1, 1);
        event("n1", Type.RECOVERY_COMPLETED, 1, 0, 0, 0); // restart without snapshot: replays from 1
        event("n1", Type.ENTRY_COMMITTED, 1, 1, 1, 1);
        event("n1", Type.ENTRY_APPLIED, 1, 1, 1, 1);
        event("n1", Type.SNAPSHOT_INSTALLED, 1, 50, 1, 0);
        event("n1", Type.ENTRY_COMMITTED, 1, 51, 1, 51);
        event("n1", Type.ENTRY_APPLIED, 1, 51, 1, 51);
        assertThat(checker.report().verdict()).isEqualTo(Verdict.SAFE);
    }

    @Test
    void applyingOutOfOrderOrBeforeCommitViolatesStateMachineSafety() {
        event("n1", Type.RECOVERY_COMPLETED, 1, 0, 0, 0);
        event("n1", Type.ENTRY_COMMITTED, 1, 1, 1, 1);
        event("n1", Type.ENTRY_APPLIED, 1, 1, 1, 1);
        event("n1", Type.ENTRY_APPLIED, 1, 2, 1, 2); // index 2 was never committed on n1
        assertThat(status("STATE_MACHINE_SAFETY")).isEqualTo(Status.VIOLATED);
        assertThat(check("STATE_MACHINE_SAFETY").violations().get(0).message()).contains("before committing");
    }

    @Test
    void applyingDifferentEntriesAtAnIndexViolatesStateMachineSafety() {
        event("n1", Type.ENTRY_APPLIED, 1, 1, 1, 111);
        event("n2", Type.ENTRY_APPLIED, 1, 1, 1, 222);
        assertThat(status("STATE_MACHINE_SAFETY")).isEqualTo(Status.VIOLATED);
    }

    @Test
    void divergentStateAfterSamePrefixIsDetected() {
        NodeObservation a = new NodeObservation("n1", 5, 1, "FOLLOWER", 4, 4, 0, 0, List.of(), 0xAAAA, true);
        NodeObservation b = new NodeObservation("n2", 5, 1, "FOLLOWER", 4, 4, 0, 0, List.of(), 0xBBBB, true);
        checker.onObservations(List.of(a, b));
        assertThat(status("STATE_MACHINE_SAFETY")).isEqualTo(Status.VIOLATED);
        assertThat(check("STATE_MACHINE_SAFETY").violations().get(0).message()).contains("state divergence");
    }

    @Test
    void termGoingBackwardsIsDetected() {
        event("n1", Type.TERM_ADVANCED, 5, 0, 0, 0);
        event("n1", Type.RECOVERY_COMPLETED, 3, 0, 0, 0); // came back with an older term: it was not persisted
        assertThat(status("TERM_MONOTONICITY")).isEqualTo(Status.VIOLATED);
    }

    @Test
    void messageTermsDoNotCountAsNodeTerms() {
        event("n1", Type.TERM_ADVANCED, 2, 0, 0, 0);
        event("n1", Type.MESSAGE_SENT, 3, 0, 0, 0); // a pre-vote advertises term 3 without entering it
        event("n1", Type.HEARTBEAT_SENT, 2, 0, 0, 0);
        assertThat(status("TERM_MONOTONICITY")).isEqualTo(Status.PASS);
    }

    @Test
    void sameIndexAndTermWithDifferentContentViolatesLogMatching() {
        checker.onObservations(List.of(
                observe("n1", "FOLLOWER", 1, 0, 0, new LogRef(1, 1, 111)),
                observe("n2", "FOLLOWER", 1, 0, 0, new LogRef(1, 1, 999))));
        assertThat(status("LOG_MATCHING")).isEqualTo(Status.VIOLATED);
    }

    @Test
    void matchingEntryWithDifferentPrefixViolatesLogMatching() {
        checker.onObservations(List.of(
                observe("n1", "FOLLOWER", 3, 0, 0, new LogRef(1, 1, 1), new LogRef(2, 3, 5)),
                observe("n2", "FOLLOWER", 3, 0, 0, new LogRef(1, 2, 2), new LogRef(2, 3, 5))));
        assertThat(status("LOG_MATCHING")).isEqualTo(Status.VIOLATED);
        assertThat(check("LOG_MATCHING").violations().get(0).message()).contains("differ at earlier index 1");
    }

    @Test
    void divergentUncommittedSuffixesAreNotAViolation() {
        // Different terms at the same index is exactly what happens with competing leaders; it is legal.
        checker.onObservations(List.of(
                observe("n1", "FOLLOWER", 3, 1, 0, new LogRef(1, 1, 1), new LogRef(2, 2, 22)),
                observe("n2", "LEADER", 3, 1, 0, new LogRef(1, 1, 1), new LogRef(2, 3, 33))));
        assertThat(checker.report().verdict()).isEqualTo(Verdict.SAFE);
    }

    @Test
    void laterLeaderMissingCommittedEntryViolatesLeaderCompleteness() {
        event("n1", Type.ENTRY_COMMITTED, 1, 1, 1, 111);
        event("n1", Type.ENTRY_COMMITTED, 1, 2, 1, 222);
        event("n3", Type.LEADER_ELECTED, 2, 2, 2, 0);
        checker.onObservations(List.of(observe("n3", "LEADER", 2, 0, 0, new LogRef(1, 1, 111), new LogRef(2, 2, 777))));
        SafetyReport.Check c = check("LEADER_COMPLETENESS");
        assertThat(c.status()).isEqualTo(Status.VIOLATED);
        assertThat(c.violations().get(0).index()).isEqualTo(2);
    }

    @Test
    void committedEntryReplacedInAFollowerLogViolatesDurability() {
        event("n1", Type.ENTRY_COMMITTED, 1, 1, 1, 111);
        checker.onObservations(List.of(observe("n2", "FOLLOWER", 2, 1, 0, new LogRef(1, 2, 999))));
        assertThat(status("COMMIT_DURABILITY")).isEqualTo(Status.VIOLATED);
    }

    @Test
    void compactedEntriesAreNotReportedMissing() {
        event("n1", Type.ENTRY_COMMITTED, 1, 1, 1, 111);
        event("n1", Type.ENTRY_COMMITTED, 1, 2, 1, 222);
        checker.onObservations(List.of(observe("n1", "LEADER", 2, 2, 2))); // both entries live in the snapshot now
        assertThat(checker.report().verdict()).isEqualTo(Verdict.SAFE);
    }

    @Test
    void missingEventsInALiveAuditMakeTheVerdictInconclusive() {
        SafetyChecker live = new SafetyChecker(true);
        live.onEvent(new RaftEvent(1, 0, "n1", Type.LEADER_ELECTED, 1, 1, 1, 0, null, null));
        live.onEvent(new RaftEvent(5, 9, "n1", Type.HEARTBEAT_SENT, 1, 0, 0, 0, null, null)); // 2..4 were not observed
        SafetyReport report = live.report();
        assertThat(report.verdict()).isEqualTo(Verdict.INCONCLUSIVE);
        assertThat(report.observationGaps()).singleElement().asString().contains("2..4");
        assertThat(report.checks()).allSatisfy(c -> assertThat(c.status()).isEqualTo(Status.INCONCLUSIVE));
    }

    @Test
    void assumptionBreachIsReportedAlongsideViolations() {
        checker.noteAssumptionBreach("n2", "n2 lost its disk");
        event("n2", Type.TERM_ADVANCED, 5, 0, 0, 0);
        assertThat(checker.report().assumptionBreaches()).containsExactly("n2 lost its disk");
    }
}
