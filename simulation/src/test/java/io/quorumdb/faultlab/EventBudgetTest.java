package io.quorumdb.faultlab;

import io.quorumdb.faultlab.analysis.RunResult;
import io.quorumdb.faultlab.scenario.Scenario;
import io.quorumdb.faultlab.scenario.ScenarioLibrary;
import io.quorumdb.faultlab.scenario.ScenarioRunner;
import io.quorumdb.faultlab.scenario.ScenarioRunner.RunTooLargeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The per-run event budget that keeps a single simulated run from exhausting the heap (the API passes
 * a budget derived from the node's heap; library and soak runs are unlimited).
 */
class EventBudgetTest {

    private static final Scenario CHAOS = ScenarioLibrary.all().get("chaos");

    @Test
    void runOverBudgetIsAbandonedEarlyWithATypedError() {
        RunResult full = ScenarioRunner.run(CHAOS, true);
        long budget = full.events().size() / 10;
        assertThatThrownBy(() -> ScenarioRunner.run(CHAOS, true, budget))
                .isInstanceOfSatisfying(RunTooLargeException.class, e -> {
                    assertThat(e.maxEvents()).isEqualTo(budget);
                    // Stopped while the budget ran out, not after simulating the whole scenario.
                    assertThat(e.atMs()).isLessThan(CHAOS.normalized().durationMs());
                    assertThat(e.getMessage()).contains(String.valueOf(budget));
                });
    }

    @Test
    void budgetIsNotMistakenForANodeFailure() {
        // The budget is detected inside Raft event callbacks. An exception there would be caught by the
        // node's fail-stop guard and turn into a simulated NODE_FAILED, letting the run "succeed".
        assertThatThrownBy(() -> ScenarioRunner.run(CHAOS, true, 500)).isInstanceOf(RunTooLargeException.class);
    }

    @Test
    void runWithinBudgetIsIdenticalToAnUnlimitedRun() {
        RunResult unlimited = ScenarioRunner.run(CHAOS, true);
        RunResult exact = ScenarioRunner.run(CHAOS, true, unlimited.events().size());
        assertThat(exact.traceHash()).isEqualTo(unlimited.traceHash());
        assertThat(exact.events()).hasSameSizeAs(unlimited.events());
        assertThat(exact.headline()).isEqualTo(unlimited.headline());
    }

    @Test
    void budgetMustBePositive() {
        assertThatThrownBy(() -> ScenarioRunner.run(CHAOS, true, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
