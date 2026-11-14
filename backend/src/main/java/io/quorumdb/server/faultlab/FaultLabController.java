package io.quorumdb.server.faultlab;

import io.quorumdb.faultlab.analysis.RunComparison;
import io.quorumdb.faultlab.analysis.RunResult;
import io.quorumdb.faultlab.scenario.Scenario;
import io.quorumdb.faultlab.scenario.ScenarioLibrary;
import io.quorumdb.faultlab.scenario.ScenarioRunner;
import io.quorumdb.server.api.AdminAuth;
import io.quorumdb.server.api.ApiException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * FaultLab API. Simulated runs execute in-process on the deterministic simulator and never touch the
 * live cluster, so they need no token. Live experiments disrupt the real cluster and require the
 * admin token.
 */
@RestController
@RequestMapping("/api/v1/faultlab")
public class FaultLabController {

    /** @param recordMessages include every simulated message in the timeline (larger runs) */
    public record RunRequest(Scenario scenario, String preset, Long seed, Boolean recordMessages) {}

    /** Result of replaying a run: for simulated runs, whether the replay was event-for-event identical. */
    public record Replay(String originalId, String replayId, String mode, boolean identical, String originalHash,
                         String replayHash, int originalEvents, int replayEvents, String note) {}

    private final RunStore store;
    private final LiveExperimentService live;
    private final AdminAuth auth;
    private final SimulationLimits limits;
    private final Semaphore simulations;

    public FaultLabController(RunStore store, LiveExperimentService live, AdminAuth auth, SimulationLimits limits) {
        this.store = store;
        this.live = live;
        this.auth = auth;
        this.limits = limits;
        this.simulations = new Semaphore(limits.maxConcurrent());
    }

    @GetMapping("/scenarios")
    public Map<String, Scenario> scenarios() {
        Map<String, Scenario> out = new LinkedHashMap<>();
        ScenarioLibrary.all().forEach((k, v) -> out.put(k, v.normalized()));
        return out;
    }

    @PostMapping("/runs")
    public ResponseEntity<RunStore.Detail> run(@RequestBody RunRequest request) {
        Scenario scenario = resolve(request);
        RunResult result = simulate(scenario, request.recordMessages() == null || request.recordMessages());
        RunStore.Summary s = store.put(result, null);
        return ResponseEntity.status(HttpStatus.CREATED).body(store.detail(s.id()));
    }

    @GetMapping("/runs")
    public List<RunStore.Summary> runs() {
        return store.list();
    }

    @GetMapping("/runs/{id}")
    public RunStore.Detail run(@PathVariable String id) {
        RunStore.Detail d = store.detail(id);
        if (d == null) throw ApiException.notFound("NO_SUCH_RUN", "no run " + id + " on this node");
        return d;
    }

    @GetMapping("/runs/{id}/events")
    public RunStore.EventPage events(@PathVariable String id, @RequestParam(defaultValue = "0") int offset,
                                     @RequestParam(defaultValue = "500") int limit,
                                     @RequestParam(required = false) String nodes,
                                     @RequestParam(required = false) String types,
                                     @RequestParam(required = false) Long fromMs, @RequestParam(required = false) Long toMs) {
        if (offset < 0 || limit < 1 || limit > 5000) throw ApiException.badRequest("need offset >= 0 and 1 <= limit <= 5000");
        RunStore.EventPage page = store.events(id, offset, limit, split(nodes), split(types), fromMs, toMs);
        if (page == null) throw ApiException.notFound("NO_SUCH_RUN", "no run " + id + " on this node");
        return page;
    }

    @GetMapping("/runs/{id}/export")
    public ResponseEntity<Map<String, Object>> export(@PathVariable String id) {
        Map<String, Object> body = store.export(id);
        if (body == null) throw ApiException.notFound("NO_SUCH_RUN", "no run " + id + " on this node");
        return ResponseEntity.ok().header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"faultlab-" + id + ".json\"")
                .body(body);
    }

    /**
     * Replays a run. A simulated run is re-executed from its scenario and seed and must reproduce the
     * identical event trace (same SHA-256). A live run cannot be replayed exactly (real time, real
     * network); its scenario is instead executed in the simulator so the two can be compared.
     */
    @PostMapping("/runs/{id}/replay")
    public ResponseEntity<Replay> replay(@PathVariable String id) {
        RunResult original = store.result(id);
        if (original == null) throw ApiException.notFound("NO_SUCH_RUN", "no run " + id + " on this node");
        boolean recordMessages = original.events().stream()
                .anyMatch(e -> e.type() == io.quorumdb.raft.RaftEvent.Type.MESSAGE_SENT);
        RunResult replay = simulate(original.scenario(), recordMessages);
        RunStore.Summary s = store.put(replay, id);
        boolean sim = original.mode() == RunResult.Mode.SIMULATION;
        boolean identical = sim && replay.traceHash().equals(original.traceHash());
        String note = !sim ? "live runs are not deterministic; the scenario was re-run in the simulator for comparison"
                : identical ? "replay reproduced the identical event trace"
                : "replay diverged from the original trace: determinism is broken (please report)";
        return ResponseEntity.status(HttpStatus.CREATED).body(new Replay(id, s.id(), original.mode().name(), identical,
                original.traceHash(), replay.traceHash(), original.events().size(), replay.events().size(), note));
    }

    @GetMapping("/compare")
    public RunComparison compare(@RequestParam String a, @RequestParam String b) {
        RunResult x = store.result(a);
        RunResult y = store.result(b);
        if (x == null || y == null) throw ApiException.notFound("NO_SUCH_RUN", "unknown run " + (x == null ? a : b));
        return RunComparison.compare(x, y);
    }

    // ---- live experiments ----

    @PostMapping("/live-runs")
    public ResponseEntity<LiveExperimentService.Status> startLive(@RequestBody RunRequest request, HttpServletRequest http) {
        auth.require(http);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(live.start(resolve(request)));
    }

    @GetMapping("/live-runs/current")
    public LiveExperimentService.Status liveStatus() {
        return live.status();
    }

    private Scenario resolve(RunRequest request) {
        if (request == null) throw ApiException.badRequest("body required");
        Scenario scenario;
        if (request.preset() != null) {
            scenario = ScenarioLibrary.all().get(request.preset());
            if (scenario == null) throw ApiException.badRequest("unknown preset '" + request.preset() + "'");
        } else if (request.scenario() != null) {
            scenario = request.scenario();
        } else {
            throw ApiException.badRequest("give either 'preset' or 'scenario'");
        }
        if (request.seed() != null) scenario = scenario.withSeed(request.seed());
        return scenario.normalized(); // validates; throws IllegalArgumentException -> 400
    }

    private RunResult simulate(Scenario scenario, boolean recordMessages) {
        try {
            if (!simulations.tryAcquire(30, TimeUnit.SECONDS)) {
                throw new ApiException(HttpStatus.TOO_MANY_REQUESTS, "BUSY", "too many simulations running; retry shortly");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "INTERRUPTED", "interrupted");
        }
        try {
            return ScenarioRunner.run(scenario, recordMessages, limits.maxEventsPerRun());
        } catch (ScenarioRunner.RunTooLargeException e) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, "RUN_TOO_LARGE", "this scenario produced more than "
                    + e.maxEvents() + " events by " + e.atMs() + " ms of simulated time, the limit for one run on this node. "
                    + "Shorten durationMs, use fewer clients or nodes, raise opIntervalMs or heartbeatIntervalMs, or set "
                    + "recordMessages to false (message events are usually most of the trace).");
        } finally {
            simulations.release();
        }
    }

    private static List<String> split(String csv) {
        return csv == null || csv.isBlank() ? null : Arrays.stream(csv.split(",")).map(String::trim).toList();
    }
}
