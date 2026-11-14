package io.quorumdb.server.faultlab;

import io.quorumdb.faultlab.analysis.RunMetrics;
import io.quorumdb.faultlab.analysis.RunResult;
import io.quorumdb.faultlab.check.LinearizabilityChecker;
import io.quorumdb.faultlab.check.SafetyReport;
import io.quorumdb.faultlab.scenario.Scenario;
import io.quorumdb.raft.RaftEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * In-memory store of recent FaultLab runs on this node (bounded; oldest evicted first). Runs are
 * reproducible from their scenario and seed, so losing the store loses no information that cannot
 * be regenerated; use the export endpoint to keep a run.
 */
@Component
public class RunStore {
    private static final int CAPACITY = 40;

    /** Compact listing entry. {@code replayOf}: id of the run this one replays, if any. */
    public record Summary(String id, String mode, String name, long createdAtMs, String headline, String safety,
                          String linearizability, String liveness, int nodes, long seed, long durationMs, long opsTotal,
                          double availability, int elections, int faults, int events, String traceHash, String replayOf) {}

    /** Everything about a run except its (large) event list, which is paged separately. */
    public record Detail(Summary summary, Scenario scenario, List<RunResult.FaultRecord> faults, SafetyReport safety,
                         LinearizabilityChecker.Report linearizability, RunResult.Liveness liveness, RunMetrics metrics,
                         List<RunResult.Finding> explanation, List<RunResult.NodeEnd> finalState, List<String> nodeIds) {}

    private record Stored(Summary summary, RunResult result) {}

    private final LinkedHashMap<String, Stored> runs = new LinkedHashMap<>();
    private final long maxStoredEvents;
    private long storedEvents;

    public RunStore(SimulationLimits limits) {
        this.maxStoredEvents = limits.maxStoredEvents();
    }

    /**
     * Stores a run, evicting the oldest ones while more than {@value #CAPACITY} runs or more than
     * {@code maxStoredEvents} events in total are held. The newest run is always kept.
     */
    public synchronized Summary put(RunResult result, String replayOf) {
        String id = (result.mode() == RunResult.Mode.LIVE ? "live-" : "sim-") + UUID.randomUUID().toString().substring(0, 8);
        Summary s = summarize(id, result, replayOf);
        runs.put(id, new Stored(s, result));
        storedEvents += result.events().size();
        while (runs.size() > 1 && (runs.size() > CAPACITY || storedEvents > maxStoredEvents)) {
            Stored evicted = runs.remove(runs.keySet().iterator().next());
            storedEvents -= evicted.result().events().size();
        }
        return s;
    }

    public synchronized long storedEvents() {
        return storedEvents;
    }

    public synchronized RunResult result(String id) {
        Stored s = runs.get(id);
        return s == null ? null : s.result();
    }

    public synchronized Summary summary(String id) {
        Stored s = runs.get(id);
        return s == null ? null : s.summary();
    }

    /** Newest first. */
    public synchronized List<Summary> list() {
        List<Summary> out = new ArrayList<>();
        runs.values().forEach(s -> out.add(0, s.summary()));
        return out;
    }

    public Detail detail(String id) {
        Summary s = summary(id);
        RunResult r = result(id);
        if (s == null) return null;
        List<String> nodeIds = new ArrayList<>();
        r.finalState().forEach(n -> nodeIds.add(n.node()));
        return new Detail(s, r.scenario(), r.faults(), r.safety(), r.linearizability(), r.liveness(), r.metrics(),
                r.explanation(), r.finalState(), nodeIds);
    }

    private static Summary summarize(String id, RunResult r, String replayOf) {
        return new Summary(id, r.mode().name(), r.scenario().name(), System.currentTimeMillis(), r.headline().name(),
                r.safety().verdict().name(), r.linearizability() == null ? "NOT_CHECKED" : r.linearizability().result().name(),
                r.liveness().status().name(), r.scenario().nodes(), r.scenario().seed(), r.metrics().durationMs(),
                r.metrics().opsTotal(), r.metrics().availability(), r.metrics().elections().size(), r.faults().size(),
                r.events().size(), r.traceHash(), replayOf);
    }

    /** A page of events filtered by node, type and time; {@code total} counts all matches. */
    public record EventPage(int total, int offset, List<RaftEvent> events) {}

    public EventPage events(String id, int offset, int limit, List<String> nodes, List<String> types, Long fromMs, Long toMs) {
        RunResult r = result(id);
        if (r == null) return null;
        List<RaftEvent> matching = new ArrayList<>();
        for (RaftEvent e : r.events()) {
            if (nodes != null && !nodes.isEmpty() && !nodes.contains(e.node())) continue;
            if (types != null && !types.isEmpty() && !types.contains(e.type().name())) continue;
            if (fromMs != null && e.timeMs() < fromMs) continue;
            if (toMs != null && e.timeMs() > toMs) continue;
            matching.add(e);
        }
        int from = Math.min(offset, matching.size());
        int to = Math.min(matching.size(), from + limit);
        return new EventPage(matching.size(), from, List.copyOf(matching.subList(from, to)));
    }

    public Map<String, Object> export(String id) {
        RunResult r = result(id);
        if (r == null) return null;
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("format", "quorumdb-faultlab-run/1");
        out.put("summary", summary(id));
        out.put("result", r);
        return out;
    }
}
