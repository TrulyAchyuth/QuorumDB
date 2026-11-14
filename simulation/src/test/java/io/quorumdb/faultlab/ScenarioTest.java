package io.quorumdb.faultlab;

import io.quorumdb.faultlab.analysis.RunComparison;
import io.quorumdb.faultlab.analysis.RunResult;
import io.quorumdb.faultlab.analysis.RunResult.Headline;
import io.quorumdb.faultlab.check.LinearizabilityChecker;
import io.quorumdb.faultlab.check.SafetyReport;
import io.quorumdb.faultlab.scenario.Scenario;
import io.quorumdb.faultlab.scenario.Scenario.Cluster;
import io.quorumdb.faultlab.scenario.Scenario.FaultStep;
import io.quorumdb.faultlab.scenario.Scenario.FaultType;
import io.quorumdb.faultlab.scenario.Scenario.ReadMode;
import io.quorumdb.faultlab.scenario.Scenario.Workload;
import io.quorumdb.faultlab.scenario.ScenarioLibrary;
import io.quorumdb.faultlab.scenario.ScenarioRunner;
import io.quorumdb.raft.RaftEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end FaultLab behaviour: scenarios, determinism, verdict classification, explanations. */
class ScenarioTest {

    static Stream<String> safeScenarios() {
        return ScenarioLibrary.all().keySet().stream().filter(k -> !k.equals("disk-loss"));
    }

    @ParameterizedTest
    @MethodSource("safeScenarios")
    void builtInScenarioIsSafeLinearizableAndRecovers(String key) {
        RunResult r = ScenarioRunner.run(ScenarioLibrary.all().get(key), false);
        assertThat(r.safety().verdict()).as(describe(r)).isEqualTo(SafetyReport.Verdict.SAFE);
        assertThat(r.linearizability().result()).isEqualTo(LinearizabilityChecker.Result.LINEARIZABLE);
        assertThat(r.liveness().status()).as(String.join(" ", r.liveness().findings())).isEqualTo(RunResult.Liveness.Status.OK);
        assertThat(r.headline()).isIn(Headline.HEALTHY, Headline.EXPECTED_UNAVAILABILITY);
        assertThat(r.metrics().opsOk()).isPositive();
        // Evidence, not vacuous truth: the core checks actually examined something.
        assertThat(r.safety().checks()).filteredOn(c -> List.of("ELECTION_SAFETY", "COMMIT_AGREEMENT", "LOG_MATCHING",
                "STATE_MACHINE_SAFETY", "COMMIT_DURABILITY").contains(c.id()))
                .allSatisfy(c -> assertThat(c.examined()).isPositive());
    }

    @Test
    void sameScenarioAndSeedReproduceTheExactEventTrace() {
        Scenario chaos = ScenarioLibrary.all().get("chaos");
        RunResult a = ScenarioRunner.run(chaos);
        RunResult b = ScenarioRunner.run(chaos);
        assertThat(b.traceHash()).isEqualTo(a.traceHash());
        assertThat(b.events()).isEqualTo(a.events());
        assertThat(b.metrics()).isEqualTo(a.metrics());
        assertThat(b.explanation()).isEqualTo(a.explanation());

        RunResult other = ScenarioRunner.run(chaos.withSeed(chaos.seed() + 1));
        assertThat(other.traceHash()).isNotEqualTo(a.traceHash());
    }

    @Test
    void leaderCrashElectsNewLeaderAndOldLeaderRejoins() {
        RunResult r = ScenarioRunner.run(ScenarioLibrary.all().get("leader-crash"));
        RunResult.FaultRecord crash = r.faults().get(0);
        assertThat(crash.hitLeader()).isTrue();
        assertThat(r.metrics().elections()).hasSize(2);
        assertThat(r.metrics().elections().get(1).leader()).isNotEqualTo(crash.leaderAtInjection());
        assertThat(r.metrics().recoveries().get(0).newLeaderAfterMs()).isBetween(100L, 1000L);
        assertThat(r.metrics().recoveries().get(0).catchUpMs()).as("old leader caught up after restart").isGreaterThanOrEqualTo(0);
        assertThat(r.finalState()).allSatisfy(n -> assertThat(n.lastApplied()).isEqualTo(r.finalState().get(0).lastApplied()));
        assertThat(r.finalState()).extracting(RunResult.NodeEnd::stateDigest).containsOnly(r.finalState().get(0).stateDigest());
    }

    @Test
    void isolatedLeaderCommitsNothingAndLaterDiscardsItsEntries() {
        RunResult r = ScenarioRunner.run(ScenarioLibrary.all().get("leader-isolated"));
        RunResult.FaultRecord fault = r.faults().get(0);
        String oldLeader = fault.leaderAtInjection();
        // While isolated, the old leader must not emit a single commit event.
        assertThat(r.events()).filteredOn(e -> e.node().equals(oldLeader) && e.type() == RaftEvent.Type.ENTRY_COMMITTED
                && e.timeMs() > fault.injectedAtMs() + 20 && e.timeMs() < fault.healedAtMs()).isEmpty();
        assertThat(r.events()).anySatisfy(e -> {
            assertThat(e.type()).isEqualTo(RaftEvent.Type.STEPPED_DOWN);
            assertThat(e.node()).isEqualTo(oldLeader);
            assertThat(e.detail()).contains("check-quorum");
        });
        assertThat(r.metrics().elections()).hasSizeGreaterThanOrEqualTo(2);
    }

    @Test
    void losingTheMajorityIsReportedAsExpectedUnavailabilityNotAsAViolation() {
        Scenario s = new Scenario("permanent majority loss", "", 3, 5L, 6_000L, null, null,
                List.of(FaultStep.of(FaultType.CRASH, 2_000, null, "n1", "n2")));
        RunResult r = ScenarioRunner.run(s);
        assertThat(r.safety().verdict()).isEqualTo(SafetyReport.Verdict.SAFE);
        assertThat(r.liveness().status()).isEqualTo(RunResult.Liveness.Status.EXPECTED_UNAVAILABILITY);
        assertThat(r.liveness().quorumAvailableAtEnd()).isFalse();
        assertThat(r.headline()).isEqualTo(Headline.EXPECTED_UNAVAILABILITY);
        assertThat(r.metrics().opsFailed() + r.metrics().opsIndeterminate()).isPositive();
        assertThat(r.metrics().failedOutsideFaults()).isZero();
        // Nothing may commit after the majority is gone.
        long commitAtFault = r.faults().get(0).commitAtInjection();
        assertThat(r.metrics().finalCommitIndex()).isLessThanOrEqualTo(commitAtFault + 2);
    }

    @Test
    void diskLossIsDetectedAsSafetyViolationWithEvidenceAndFlaggedAsBrokenAssumption() {
        RunResult r = ScenarioRunner.run(ScenarioLibrary.all().get("disk-loss"));
        assertThat(r.headline()).isEqualTo(Headline.SAFETY_VIOLATION);
        assertThat(r.safety().assumptionBreaches()).isNotEmpty();
        SafetyReport.Check agreement = r.safety().checks().stream().filter(c -> c.id().equals("COMMIT_AGREEMENT")).findFirst().orElseThrow();
        assertThat(agreement.status()).isEqualTo(SafetyReport.Status.VIOLATED);
        SafetyReport.Violation v = agreement.violations().get(0);
        assertThat(v.evidence()).hasSize(2);
        // The cited events exist and really are two commits of different entries at the same index.
        RaftEvent first = r.events().get((int) (v.evidence().get(0) - 1));
        RaftEvent second = r.events().get((int) (v.evidence().get(1) - 1));
        assertThat(first.type()).isEqualTo(RaftEvent.Type.ENTRY_COMMITTED);
        assertThat(second.type()).isEqualTo(RaftEvent.Type.ENTRY_COMMITTED);
        assertThat(first.index()).isEqualTo(second.index());
        assertThat(first.checksum()).isNotEqualTo(second.checksum());
        assertThat(r.linearizability().result()).as("clients observed the lost writes")
                .isEqualTo(LinearizabilityChecker.Result.NOT_LINEARIZABLE);
    }

    /**
     * Read consistency under a partition, for all three read modes.
     *
     * <p>Setup: the leader is isolated for 3 s with CheckQuorum off, so it keeps believing it leads while
     * the majority elects a new leader and accepts writes. The workload is read-heavy on a single key, so
     * some clients are still reading from the old leader while others already write through the new one.
     *
     * <p>Guarantee under test: a LINEARIZABLE (ReadIndex) read must never return a value older than a write
     * that was acknowledged before the read began. That must hold for every seed. The weaker modes
     * make no such promise, and the black-box checker must be able to tell: LEADER_LOCAL reads served by
     * the deposed leader, and STALE reads served by lagging followers, show up as non-linearizable client
     * histories, while Raft's own safety properties (which concern the log, not reads) still hold.
     */
    private static Scenario readScenario(ReadMode mode, long seed) {
        Cluster noCheckQuorum = new Cluster(null, null, null, false, false, null, null, null);
        return new Scenario(mode + " reads under partition", "", 3, seed, 8_000L, noCheckQuorum,
                new Workload(6, 20L, 1, 0.9, 0.0, null, mode),
                List.of(FaultStep.of(FaultType.ISOLATE, 2_000, 3_000L, "leader")));
    }

    @Test
    void readIndexReadsAreLinearizableUnderPartitionForEverySeed() {
        for (long seed = 1; seed <= 25; seed++) {
            RunResult r = ScenarioRunner.run(readScenario(ReadMode.LINEARIZABLE, seed), false);
            assertThat(r.linearizability().result()).as("seed " + seed).isEqualTo(LinearizabilityChecker.Result.LINEARIZABLE);
            assertThat(r.safety().verdict()).as("seed " + seed).isEqualTo(SafetyReport.Verdict.SAFE);
            assertThat(r.linearizability().operationsChecked()).as("the check was not vacuous").isGreaterThan(500);
        }
    }

    @Test
    void unconfirmedLeaderReadsAreFlaggedExactlyWhenAStaleReadOccurred() {
        // Ground truth from the trace: the deposed leader's data is frozen at the moment of isolation, so
        // any read it serves that was invoked after a write was acknowledged through the new leader is
        // stale. The black-box checker must flag exactly those runs, with no misses and no false alarms.
        int staleRuns = 0;
        for (long seed = 1; seed <= 25; seed++) {
            RunResult r = ScenarioRunner.run(readScenario(ReadMode.LEADER_LOCAL, seed), false);
            assertThat(r.safety().verdict()).as("Raft itself stays safe, seed " + seed).isEqualTo(SafetyReport.Verdict.SAFE);
            RunResult.FaultRecord fault = r.faults().get(0);
            String oldLeader = fault.leaderAtInjection();
            long firstAckedWriteViaNewLeader = r.events().stream()
                    .filter(e -> e.type() == RaftEvent.Type.CLIENT_RESPONSE && e.timeMs() > fault.injectedAtMs()
                            && e.timeMs() < fault.healedAtMs() && e.detail().startsWith("OK PUT"))
                    .mapToLong(RaftEvent::timeMs).min().orElse(Long.MAX_VALUE);
            boolean staleReadHappened = r.events().stream()
                    .filter(e -> e.type() == RaftEvent.Type.CLIENT_RESPONSE && e.timeMs() < fault.healedAtMs()
                            && e.detail().contains("leader-local from " + oldLeader))
                    .anyMatch(e -> invokedAt(e) > firstAckedWriteViaNewLeader);
            boolean flagged = r.linearizability().result() == LinearizabilityChecker.Result.NOT_LINEARIZABLE;
            assertThat(flagged).as("seed " + seed + ": checker verdict must match what actually happened")
                    .isEqualTo(staleReadHappened);
            if (flagged) {
                staleRuns++;
                assertThat(r.headline()).isEqualTo(Headline.SAFETY_VIOLATION);
                assertThat(r.linearizability().keys()).isNotEmpty().allSatisfy(k -> assertThat(k.problemOps()).isNotEmpty());
            }
        }
        assertThat(staleRuns).as("the scenario must actually produce stale reads in many seeds").isGreaterThanOrEqualTo(10);
    }

    /** Invocation time of a client operation, from its response event ("... after N ms; ..."). */
    private static long invokedAt(RaftEvent response) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("after (\\d+) ms").matcher(response.detail());
        assertThat(m.find()).isTrue();
        return response.timeMs() - Long.parseLong(m.group(1));
    }

    @Test
    void followerLocalReadsAreCaughtAsStale() {
        int detected = 0;
        for (long seed = 1; seed <= 25; seed++) {
            RunResult r = ScenarioRunner.run(readScenario(ReadMode.STALE, seed), false);
            assertThat(r.safety().verdict()).as("seed " + seed).isEqualTo(SafetyReport.Verdict.SAFE);
            if (r.linearizability().result() == LinearizabilityChecker.Result.NOT_LINEARIZABLE) detected++;
        }
        assertThat(detected).as("reads from arbitrary (possibly lagging or isolated) nodes are not linearizable")
                .isGreaterThanOrEqualTo(23);
    }

    @Test
    void preVotePreventsTermInflationFromAnIsolatedNode() {
        RunResult without = ScenarioRunner.run(ScenarioLibrary.all().get("rejoin-no-prevote"));
        RunResult with = ScenarioRunner.run(ScenarioLibrary.all().get("rejoin-prevote"));
        assertThat(without.metrics().finalTerm()).isGreaterThan(5);
        assertThat(without.metrics().leaderChanges()).as("the returning node deposes the leader").isGreaterThanOrEqualTo(1);
        assertThat(with.metrics().finalTerm()).isEqualTo(1);
        assertThat(with.metrics().leaderChanges()).isZero();

        RunComparison cmp = RunComparison.compare(without, with);
        assertThat(cmp.rows()).anySatisfy(row -> {
            assertThat(row.metric()).isEqualTo("Final term");
            assertThat(row.better()).isEqualTo("b");
        });
        assertThat(cmp.observations()).anySatisfy(o -> assertThat(o).contains("Cluster settings differ"));
    }

    @Test
    void snapshotScenarioRestoresLaggingFollowerViaInstallSnapshot() {
        RunResult r = ScenarioRunner.run(ScenarioLibrary.all().get("snapshot-catch-up"));
        assertThat(r.metrics().snapshotsCreated()).isPositive();
        assertThat(r.metrics().snapshotsInstalled()).isPositive();
        assertThat(r.finalState()).extracting(RunResult.NodeEnd::stateDigest).containsOnly(r.finalState().get(0).stateDigest());
        assertThat(r.finalState()).allSatisfy(n -> assertThat(n.snapshotIndex()).isPositive());
    }

    @Test
    void storageFailureMakesLeaderFailStopWithoutLosingAcknowledgedWrites() {
        RunResult r = ScenarioRunner.run(ScenarioLibrary.all().get("storage-failure"));
        assertThat(r.events()).anySatisfy(e -> assertThat(e.type()).isEqualTo(RaftEvent.Type.NODE_FAILED));
        assertThat(r.linearizability().result()).isEqualTo(LinearizabilityChecker.Result.LINEARIZABLE);
    }

    @Test
    void explanationCitesOnlyEventsThatExistAndSupportTheClaim() {
        RunResult r = ScenarioRunner.run(ScenarioLibrary.all().get("leader-isolated"));
        assertThat(r.explanation()).isNotEmpty();
        for (RunResult.Finding f : r.explanation()) {
            for (long seq : f.evidence()) {
                assertThat(seq).isBetween(1L, (long) r.events().size());
                assertThat(r.events().get((int) (seq - 1)).seq()).isEqualTo(seq);
            }
        }
        RunResult.Finding injected = r.explanation().stream().filter(f -> f.title().startsWith("Fault injected")).findFirst().orElseThrow();
        assertThat(injected.detail()).contains("became leader of term 2").contains("check-quorum");
        // The "became leader" sentence must be backed by a LEADER_ELECTED event among the evidence.
        assertThat(injected.evidence()).anySatisfy(seq ->
                assertThat(r.events().get((int) (seq - 1)).type()).isEqualTo(RaftEvent.Type.LEADER_ELECTED));
        RunResult.Finding healed = r.explanation().stream().filter(f -> f.title().startsWith("Fault healed")).findFirst().orElseThrow();
        assertThat(healed.detail()).contains("removed entries from its log");
    }

    @Test
    void conditionTriggersFireWhenTheConditionHolds() {
        Scenario s = new Scenario("conditional", "", 3, 9L, 6_000L, null, null, List.of(
                new FaultStep(FaultType.CRASH, List.of("leader"), null,
                        Scenario.Trigger.when(Scenario.Condition.OPS_COMPLETED_AT_LEAST, 50, 100),
                        Scenario.Trigger.when(Scenario.Condition.COMMIT_INDEX_AT_LEAST, 150, 0), null, null, null)));
        RunResult r = ScenarioRunner.run(s);
        RunResult.FaultRecord f = r.faults().get(0);
        assertThat(f.injectedAtMs()).isPositive();
        assertThat(f.healedAtMs()).isGreaterThan(f.injectedAtMs());
        assertThat(f.commitAtInjection()).isLessThan(150);
    }

    @Test
    void invalidScenariosAreRejectedWithAUsefulMessage() {
        assertThatThrownBy(() -> new Scenario(null, null, 12, null, null, null, null, null).normalized())
                .hasMessageContaining("nodes must be between 1 and 9");
        assertThatThrownBy(() -> new Scenario(null, null, 3, null, null, null, null,
                List.of(FaultStep.of(FaultType.CRASH, 100, null, "n9"))).normalized())
                .hasMessageContaining("unknown target 'n9'");
        assertThatThrownBy(() -> new Scenario(null, null, 3, null, null, null, null,
                List.of(FaultStep.of(FaultType.DROP, 100, null, "n1"))).normalized())
                .hasMessageContaining("dropProbability");
        assertThatThrownBy(() -> new Scenario(null, null, 3, null, null,
                new Cluster(100L, 200L, 90L, null, null, null, null, null), null, null).normalized())
                .hasMessageContaining("heartbeatIntervalMs");
    }

    @Test
    void singleNodeClusterWorks() {
        RunResult r = ScenarioRunner.run(new Scenario("single", "", 1, 2L, 2_000L, null, null, List.of()));
        assertThat(r.headline()).isEqualTo(Headline.HEALTHY);
        assertThat(r.metrics().opsOk()).isPositive();
    }

    private static String describe(RunResult r) {
        StringBuilder sb = new StringBuilder("seed=" + r.scenario().seed() + " ");
        r.safety().checks().forEach(c -> c.violations().forEach(v -> sb.append(c.id()).append(": ").append(v.message()).append("; ")));
        return sb.toString();
    }
}
