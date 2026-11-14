// Measures the retained heap per trace event of simulated FaultLab runs (input for SimulationLimits).
//   ./gradlew -q :simulation:classes
//   java -Xmx4g -cp core/build/classes/java/main:simulation/build/classes/java/main scripts/MeasureEventSize.java
// Runs each scenario in this JVM, keeps the result reachable, and divides the post-GC heap growth by the
// number of events in the trace. Everything a stored run holds (events, history, metrics, reports) is
// included, which is what the limit has to cover.
import io.quorumdb.faultlab.analysis.RunResult;
import io.quorumdb.faultlab.scenario.Scenario;
import io.quorumdb.faultlab.scenario.ScenarioLibrary;
import io.quorumdb.faultlab.scenario.ScenarioRunner;

import java.util.ArrayList;
import java.util.List;

public class MeasureEventSize {
    static long used() throws InterruptedException {
        Runtime rt = Runtime.getRuntime();
        for (int i = 0; i < 4; i++) {
            System.gc();
            Thread.sleep(100);
        }
        return rt.totalMemory() - rt.freeMemory();
    }

    public static void main(String[] args) throws Exception {
        List<Object> keep = new ArrayList<>();
        ScenarioRunner.run(ScenarioLibrary.all().get("leader-crash"), true); // warm up classes
        for (String key : List.of("chaos", "leader-crash", "rolling-restart")) {
            for (boolean messages : new boolean[] {false, true}) {
                Scenario s = ScenarioLibrary.all().get(key);
                long before = used();
                RunResult r = ScenarioRunner.run(s, messages);
                keep.add(r);
                long after = used();
                System.out.printf("%-16s messages=%-5s events=%,9d retained=%,7d KB  bytes/event=%.0f%n", key, messages,
                        r.events().size(), (after - before) / 1024, (double) (after - before) / r.events().size());
            }
        }
        // A large run: 9 nodes, 120 s, 16 clients.
        Scenario base = ScenarioLibrary.all().get("chaos");
        Scenario big = new Scenario("big", null, 9, 7L, 120_000L, null,
                new Scenario.Workload(16, 5L, 200, 0.5, 0.0, null, null), List.of());
        for (boolean messages : new boolean[] {false, true}) {
            long before = used();
            RunResult r = ScenarioRunner.run(big, messages);
            keep.add(r);
            long after = used();
            System.out.printf("%-16s messages=%-5s events=%,9d retained=%,7d KB  bytes/event=%.0f%n", "big-9n-120s", messages,
                    r.events().size(), (after - before) / 1024, (double) (after - before) / r.events().size());
        }
        System.out.println("kept " + keep.size() + " runs");
    }
}
