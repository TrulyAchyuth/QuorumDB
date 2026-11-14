package io.quorumdb.server.faultlab;

/**
 * Memory limits for simulated FaultLab runs, derived from the JVM's maximum heap.
 *
 * <p>Simulated runs are open to any API client (they do not touch the real cluster) and are held in
 * memory with their full event trace. Scenario validation alone bounds a run only loosely (up to 9
 * nodes, 600 s, 32 clients, 1 ms heartbeats), which allows traces of tens of millions of events: far
 * more than the heap of a node. These limits keep the worst case inside the heap:
 *
 * <ul>
 *   <li>{@code maxEventsPerRun}: a run whose trace grows beyond this is abandoned (HTTP 422).</li>
 *   <li>{@code maxConcurrent}: simulations running at once, so that all of them together fit the
 *       simulation share of the heap even if each one reaches the per-run limit.</li>
 *   <li>{@code maxStoredEvents}: total events of finished runs kept in memory; older runs are evicted
 *       first.</li>
 * </ul>
 *
 * @param bytesPerEvent measured retained size of one event in a run, including its share of client
 *                      history, checker state and samples (see BYTES_PER_EVENT)
 */
public record SimulationLimits(long maxEventsPerRun, int maxConcurrent, long maxStoredEvents, long bytesPerEvent) {

    /**
     * Heap to budget per trace event. A finished run retains 94-154 bytes per event (measured on JDK 21 with
     * scripts/MeasureEventSize.java; message events are the larger ones), but a run in progress needs more
     * (checker state, pending messages, sampling), so the budget is about 2.6 times the retained maximum.
     * Validated end to end on real nodes with 256 MB and 512 MB heaps under the worst concurrent load these
     * limits allow (docs/evidence/simulation-limits.md).
     */
    static final long BYTES_PER_EVENT = 400;

    /** Never allow more than this per run, however large the heap: replies and pages stay manageable. */
    static final long MAX_EVENTS_PER_RUN_CAP = 3_000_000;

    /**
     * Every built-in scenario must fit one run: the largest ("chaos" with message events) produces about
     * 87,000 events. On small heaps fewer simulations run at once so that each still gets this much.
     */
    static final long MIN_EVENTS_PER_RUN = 100_000;

    public static SimulationLimits forHeap(long maxHeapBytes, int processors) {
        // 40% of the heap for running simulations, 25% for finished runs; the rest is the node itself.
        long simulationBytes = maxHeapBytes * 2 / 5;
        long storedBytes = maxHeapBytes / 4;
        int concurrent = Math.max(1, Math.min(processors / 2, 4));
        long perRun = Math.min(MAX_EVENTS_PER_RUN_CAP, simulationBytes / concurrent / BYTES_PER_EVENT);
        while (perRun < MIN_EVENTS_PER_RUN && concurrent > 1) {
            concurrent--;
            perRun = Math.min(MAX_EVENTS_PER_RUN_CAP, simulationBytes / concurrent / BYTES_PER_EVENT);
        }
        perRun = Math.max(perRun, MIN_EVENTS_PER_RUN);
        // The store always keeps the newest run whatever its size, so this need not cover a whole run.
        long stored = storedBytes / BYTES_PER_EVENT;
        return new SimulationLimits(perRun, concurrent, stored, BYTES_PER_EVENT);
    }

    public static SimulationLimits forThisJvm() {
        return forHeap(Runtime.getRuntime().maxMemory(), Runtime.getRuntime().availableProcessors());
    }
}
