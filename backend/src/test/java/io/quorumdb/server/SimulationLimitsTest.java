package io.quorumdb.server;

import io.quorumdb.faultlab.analysis.RunResult;
import io.quorumdb.faultlab.scenario.ScenarioLibrary;
import io.quorumdb.faultlab.scenario.ScenarioRunner;
import io.quorumdb.server.faultlab.RunStore;
import io.quorumdb.server.faultlab.SimulationLimits;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Heap-derived limits for simulated runs and the event-bounded run store. */
class SimulationLimitsTest {
    private static final long MB = 1024 * 1024;

    @Test
    void runningSimulationsFitTheirShareOfTheHeapEvenAtTheLimit() {
        for (long heap : new long[] {256 * MB, 512 * MB, 1024 * MB, 4096 * MB}) {
            for (int cpus : new int[] {1, 2, 4, 10, 64}) {
                SimulationLimits l = SimulationLimits.forHeap(heap, cpus);
                long worstCase = l.maxEventsPerRun() * l.maxConcurrent() * l.bytesPerEvent();
                assertThat(worstCase).as("heap %d MB, %d cpus: %s", heap / MB, cpus, l).isLessThanOrEqualTo(heap * 2 / 5);
                assertThat(l.maxStoredEvents() * l.bytesPerEvent()).as("stored runs").isLessThanOrEqualTo(heap / 4);
                assertThat(l.maxConcurrent()).isBetween(1, 4);
            }
        }
    }

    @Test
    void everyBuiltInScenarioFitsOneRunFromA256MbHeapUp() {
        long largest = 0;
        for (var e : ScenarioLibrary.all().entrySet()) {
            largest = Math.max(largest, ScenarioRunner.run(e.getValue(), true).events().size());
        }
        for (long heap : new long[] {256 * MB, 512 * MB}) {
            for (int cpus : new int[] {1, 4, 10, 64}) {
                assertThat(SimulationLimits.forHeap(heap, cpus).maxEventsPerRun()).as("heap %d MB, %d cpus", heap / MB, cpus)
                        .isGreaterThanOrEqualTo(largest);
            }
        }
    }

    @Test
    void hugeHeapIsStillCappedPerRun() {
        assertThat(SimulationLimits.forHeap(64L * 1024 * MB, 16).maxEventsPerRun()).isEqualTo(3_000_000);
    }

    @Test
    void storeEvictsOldestRunsByTotalEventsAndKeepsTheNewest() {
        RunResult run = ScenarioRunner.run(ScenarioLibrary.all().get("leader-crash"), false);
        long size = run.events().size();
        RunStore store = new RunStore(new SimulationLimits(size * 10, 1, size * 3, 1));
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 5; i++) ids.add(store.put(run, null).id());
        assertThat(store.storedEvents()).isEqualTo(3 * size);
        assertThat(store.list()).extracting(RunStore.Summary::id).containsExactly(ids.get(4), ids.get(3), ids.get(2));
        assertThat(store.result(ids.get(0))).isNull();

        // A single run larger than the whole budget is still kept (it was just computed), alone.
        RunStore tiny = new RunStore(new SimulationLimits(size * 10, 1, 1, 1));
        String a = tiny.put(run, null).id();
        String b = tiny.put(run, null).id();
        assertThat(tiny.result(a)).isNull();
        assertThat(tiny.result(b)).isNotNull();
        assertThat(tiny.storedEvents()).isEqualTo(size);
    }
}
