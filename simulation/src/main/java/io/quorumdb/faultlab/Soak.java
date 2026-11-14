package io.quorumdb.faultlab;

import io.quorumdb.faultlab.analysis.RunResult;
import io.quorumdb.faultlab.check.LinearizabilityChecker;
import io.quorumdb.faultlab.scenario.Scenario;
import io.quorumdb.faultlab.scenario.Scenario.Cluster;
import io.quorumdb.faultlab.scenario.Scenario.FaultStep;
import io.quorumdb.faultlab.scenario.Scenario.FaultType;
import io.quorumdb.faultlab.scenario.Scenario.Trigger;
import io.quorumdb.faultlab.scenario.Scenario.Workload;
import io.quorumdb.faultlab.scenario.ScenarioRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

/**
 * Soak test: runs many randomly generated fault scenarios through the simulator and reports any
 * run that violates safety, linearizability or recovery. Fully determined by the master seed, so
 * {@code Soak <runs> <masterSeed>} always generates the same scenarios; a failing run is printed
 * with its index and can be regenerated with {@link #generate(long, int)}.
 *
 * <pre>./gradlew :simulation:soak -Pruns=5000 -PmasterSeed=1</pre>
 */
public final class Soak {
    private Soak() {}

    private static final FaultType[] TYPES = {FaultType.CRASH, FaultType.RESTART, FaultType.ISOLATE, FaultType.PARTITION,
            FaultType.DELAY, FaultType.DROP, FaultType.STORAGE_FAILURE};
    private static final String[] TARGETS = {"leader", "follower", "random", "n1", "n2", "n3"};

    /** The {@code index}-th scenario of the soak identified by {@code masterSeed}. */
    public static Scenario generate(long masterSeed, int index) {
        Random r = new Random(masterSeed * 1_000_003L + index);
        int nodes = r.nextBoolean() ? 3 : 5;
        List<FaultStep> faults = new ArrayList<>();
        int count = 1 + r.nextInt(8);
        for (int i = 0; i < count; i++) {
            FaultType type = TYPES[r.nextInt(TYPES.length)];
            List<String> targets = new ArrayList<>();
            targets.add(TARGETS[r.nextInt(TARGETS.length)]);
            if (r.nextInt(3) == 0) {
                String second = TARGETS[r.nextInt(TARGETS.length)];
                if (!targets.contains(second)) targets.add(second);
            }
            long at = 200 + r.nextInt(5_800);
            Long duration = type == FaultType.RESTART ? null : (long) (100 + r.nextInt(1_400));
            faults.add(new FaultStep(type, targets, null, Trigger.at(at), null, duration,
                    type == FaultType.DELAY ? (long) (20 + r.nextInt(180)) : null,
                    type == FaultType.DROP ? 0.05 + r.nextDouble() * 0.55 : null));
        }
        long[] snapshotThresholds = {0, 20, 100};
        return new Scenario("soak-" + masterSeed + "-" + index, "", nodes, 1 + (long) r.nextInt(1_000_000), 8_000L,
                new Cluster(null, null, null, r.nextBoolean(), r.nextBoolean(), snapshotThresholds[r.nextInt(3)], 1L, 15L),
                new Workload(4, 25L, 4, 0.5, 0.1, 1200L, null), faults);
    }

    public static void main(String[] args) {
        int runs = args.length > 0 ? Integer.parseInt(args[0]) : 1000;
        long masterSeed = args.length > 1 ? Long.parseLong(args[1]) : 1;
        long started = System.nanoTime();
        long events = 0, ops = 0, elections = 0, faultsInjected = 0, virtualMs = 0;
        int failures = 0;
        TreeMap<String, Integer> headlines = new TreeMap<>();
        for (int i = 0; i < runs; i++) {
            Scenario scenario = generate(masterSeed, i);
            RunResult result = ScenarioRunner.run(scenario, false);
            events += result.events().size();
            ops += result.metrics().opsTotal();
            elections += result.metrics().elections().size();
            faultsInjected += result.faults().size();
            virtualMs += result.metrics().durationMs();
            headlines.merge(result.headline().name(), 1, Integer::sum);
            boolean safe = !result.safety().hasViolations();
            boolean linearizable = result.linearizability().result() == LinearizabilityChecker.Result.LINEARIZABLE;
            boolean recovered = result.liveness().leaderAtEnd() != null && result.liveness().converged();
            if (!safe || !linearizable || !recovered) {
                failures++;
                System.out.println("FAILED run " + i + " safe=" + safe + " linearizable=" + linearizable + " recovered="
                        + recovered + "\n  " + scenario + "\n  " + result.liveness().findings());
                result.safety().checks().forEach(c -> c.violations().forEach(v ->
                        System.out.println("  " + c.id() + ": " + v.message())));
            }
        }
        double seconds = (System.nanoTime() - started) / 1e9;
        System.out.printf("soak masterSeed=%d runs=%d failures=%d%n", masterSeed, runs, failures);
        System.out.printf("  simulated time: %.1f hours of cluster time in %.1f s wall clock%n", virtualMs / 3_600_000.0, seconds);
        System.out.printf("  faults injected: %d, elections: %d, client operations: %d, events audited: %d%n",
                faultsInjected, elections, ops, events);
        System.out.println("  headlines: " + headlines);
        if (failures > 0) System.exit(1);
    }
}
