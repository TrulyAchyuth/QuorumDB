package io.quorumdb.faultlab;

import io.quorumdb.faultlab.analysis.RunResult;
import io.quorumdb.faultlab.check.LinearizabilityChecker;
import io.quorumdb.faultlab.check.SafetyReport;
import io.quorumdb.faultlab.scenario.Scenario;
import io.quorumdb.faultlab.scenario.Scenario.Cluster;
import io.quorumdb.faultlab.scenario.Scenario.FaultStep;
import io.quorumdb.faultlab.scenario.Scenario.FaultType;
import io.quorumdb.faultlab.scenario.Scenario.Trigger;
import io.quorumdb.faultlab.scenario.Scenario.Workload;
import io.quorumdb.faultlab.scenario.ScenarioRunner;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.ShrinkingMode;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based testing: generate random fault schedules (crashes, restarts, isolation, partitions,
 * message loss, delay, disk-write failures, in any overlap) and require that no generated schedule
 * ever breaks a safety property or linearizability, and that the cluster recovers once faults heal.
 *
 * <p>Every failure message contains the full scenario including its seed; re-running that scenario
 * reproduces the failure exactly. jqwik additionally shrinks a failing schedule to a smaller one.
 */
class RandomFaultProperties {

    @Provide
    Arbitrary<Scenario> scenarios() {
        Arbitrary<Integer> nodes = Arbitraries.of(3, 5);
        Arbitrary<Long> seed = Arbitraries.longs().between(1, 1_000_000);
        Arbitrary<Boolean> preVote = Arbitraries.of(true, false);
        Arbitrary<Boolean> checkQuorum = Arbitraries.of(true, false);
        Arbitrary<Long> snapshotThreshold = Arbitraries.of(0L, 20L, 100L);
        Arbitrary<List<FaultStep>> faults = fault().list().ofMinSize(1).ofMaxSize(8);
        return Combinators.combine(nodes, seed, preVote, checkQuorum, snapshotThreshold, faults)
                .as((n, s, pv, cq, snap, f) -> new Scenario("generated", "", n, s, 8_000L,
                        new Cluster(null, null, null, pv, cq, snap, 1L, 15L),
                        new Workload(4, 25L, 4, 0.5, 0.1, 1200L, null), f));
    }

    private Arbitrary<FaultStep> fault() {
        Arbitrary<FaultType> type = Arbitraries.of(FaultType.CRASH, FaultType.RESTART, FaultType.ISOLATE,
                FaultType.PARTITION, FaultType.DELAY, FaultType.DROP, FaultType.STORAGE_FAILURE);
        Arbitrary<Long> at = Arbitraries.longs().between(200, 6_000);
        Arbitrary<Long> duration = Arbitraries.longs().between(100, 1_500);
        // Only n1..n3 and selectors: valid for both 3- and 5-node clusters.
        Arbitrary<List<String>> targets = Arbitraries.of("leader", "follower", "random", "n1", "n2", "n3")
                .list().ofMinSize(1).ofMaxSize(2).uniqueElements();
        Arbitrary<Long> delay = Arbitraries.longs().between(20, 200);
        Arbitrary<Double> drop = Arbitraries.doubles().between(0.05, 0.6);
        return Combinators.combine(type, at, duration, targets, delay, drop).as((t, a, d, tg, dl, dr) ->
                new FaultStep(t, tg, null, Trigger.at(a), null, t == FaultType.RESTART ? null : d,
                        t == FaultType.DELAY ? dl : null, t == FaultType.DROP ? dr : null));
    }

    @Property(tries = 300, shrinking = ShrinkingMode.BOUNDED)
    void noFaultScheduleBreaksSafetyOrLinearizability(@ForAll("scenarios") Scenario scenario) {
        RunResult r = ScenarioRunner.run(scenario, false);
        String context = "reproduce with: " + scenario;
        assertThat(violations(r)).as(context).isEmpty();
        assertThat(r.safety().verdict()).as(context).isEqualTo(SafetyReport.Verdict.SAFE);
        assertThat(r.linearizability().result()).as(context).isEqualTo(LinearizabilityChecker.Result.LINEARIZABLE);
    }

    @Property(tries = 150, shrinking = ShrinkingMode.BOUNDED)
    void clusterRecoversOnceAllFaultsHaveHealed(@ForAll("scenarios") Scenario scenario) {
        // Every generated fault has a bounded duration and ends before the run does, so afterwards a
        // leader must exist and all nodes must converge on the same applied state.
        RunResult r = ScenarioRunner.run(scenario, false);
        String context = String.join(" ", r.liveness().findings()) + " | reproduce with: " + scenario;
        assertThat(r.liveness().quorumAvailableAtEnd()).as(context).isTrue();
        assertThat(r.liveness().leaderAtEnd()).as(context).isNotNull();
        assertThat(r.liveness().converged()).as(context).isTrue();
        assertThat(r.finalState()).extracting(RunResult.NodeEnd::stateDigest).as(context)
                .containsOnly(r.finalState().get(0).stateDigest());
    }

    @Property(tries = 40)
    void everyRunIsReproducibleFromItsScenarioAndSeed(@ForAll("scenarios") Scenario scenario) {
        RunResult first = ScenarioRunner.run(scenario, true);
        RunResult second = ScenarioRunner.run(scenario, true);
        assertThat(second.traceHash()).isEqualTo(first.traceHash());
        assertThat(second.events().size()).isEqualTo(first.events().size());
    }

    private static List<String> violations(RunResult r) {
        List<String> out = new ArrayList<>();
        r.safety().checks().forEach(c -> c.violations().forEach(v -> out.add(c.id() + ": " + v.message())));
        if (r.linearizability() != null) r.linearizability().keys().forEach(k -> out.add("key " + k.key() + ": " + k.explanation()));
        return out;
    }
}
