package io.quorumdb.server.faultlab;

import io.quorumdb.faultlab.analysis.Explainer;
import io.quorumdb.faultlab.analysis.RunAnalyzer;
import io.quorumdb.faultlab.analysis.RunMetrics;
import io.quorumdb.faultlab.analysis.RunResult;
import io.quorumdb.faultlab.check.LinearizabilityChecker;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Kind;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Op;
import io.quorumdb.faultlab.check.LinearizabilityChecker.Outcome;
import io.quorumdb.faultlab.check.NodeObservation;
import io.quorumdb.faultlab.check.SafetyChecker;
import io.quorumdb.faultlab.check.SafetyReport;
import io.quorumdb.faultlab.scenario.Scenario;
import io.quorumdb.raft.RaftEvent;
import io.quorumdb.server.api.AdminAuth;
import io.quorumdb.server.api.ApiException;
import io.quorumdb.server.api.ClusterService;
import io.quorumdb.server.api.NodeView;
import io.quorumdb.server.api.PeerClient;
import io.quorumdb.server.config.ClusterConfig;
import io.quorumdb.server.faults.LiveFaultRequest;
import io.quorumdb.server.faults.LiveFaultService;
import io.quorumdb.server.node.EventLog;
import io.quorumdb.server.node.NodeRuntime;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs a FaultLab scenario against the <em>real</em> cluster and audits it with the same independent
 * checkers used in simulation.
 *
 * <ol>
 *   <li>Simulated-protocol clients issue real HTTP requests to random members (so forwarding is
 *       exercised) and record exactly what they observed: invocation time, completion time, result.</li>
 *   <li>The scenario's faults are injected at their scheduled times through {@link LiveFaultService}.</li>
 *   <li>A collector continuously pulls every node's event log and samples every node's durable log.
 *       Missed events (ring-buffer overrun, unreachable node) are recorded as observation gaps, which
 *       downgrade the safety verdict to INCONCLUSIVE instead of silently passing.</li>
 *   <li>At the end all faults are healed (always, also on error), the cluster is given time to
 *       converge, and the run is audited: safety properties, linearizability, liveness, metrics.</li>
 * </ol>
 *
 * <p>Differences from simulation, all stated in the result: time is wall-clock, faults are applied
 * over the network with real latency, events from different nodes are ordered by their local clocks
 * (accurate on one host, approximate across hosts), and runs cannot be replayed deterministically.
 * Condition triggers and DISK_LOSS are simulator-only.
 */
@Service
public class LiveExperimentService {
    private static final Logger log = LoggerFactory.getLogger(LiveExperimentService.class);
    private static final long MAX_DURATION_MS = 120_000;
    private static final long SETTLE_MS = 4_000;
    /** Entries per durable-log request during the live audit (the endpoint allows up to 10,000). */
    private static final int LOG_PAGE = 5_000;

    /** Progress of the current (or last) live run. */
    public record Status(String state, String scenario, long startedAtMs, long elapsedMs, long durationMs,
                         long opsCompleted, int faultsInjected, String runId, String error) {}

    private final ClusterConfig cluster;
    private final ClusterService clusterService;
    private final LiveFaultService faults;
    private final NodeRuntime runtime;
    private final RunStore store;
    private final AdminAuth auth;
    private final JsonMapper json;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500))
            .version(HttpClient.Version.HTTP_1_1).executor(Executors.newVirtualThreadPerTaskExecutor()).build();
    private final ExecutorService runner = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "faultlab-live");
        t.setDaemon(true);
        return t;
    });
    private final AtomicReference<Status> status = new AtomicReference<>(
            new Status("IDLE", null, 0, 0, 0, 0, 0, null, null));

    public LiveExperimentService(ClusterConfig cluster, ClusterService clusterService, LiveFaultService faults,
                                 NodeRuntime runtime, RunStore store, AdminAuth auth, JsonMapper json) {
        this.cluster = cluster;
        this.clusterService = clusterService;
        this.faults = faults;
        this.runtime = runtime;
        this.store = store;
        this.auth = auth;
        this.json = json;
    }

    public Status status() {
        Status s = status.get();
        if (s.state().equals("RUNNING")) {
            return new Status(s.state(), s.scenario(), s.startedAtMs(), System.currentTimeMillis() - s.startedAtMs(),
                    s.durationMs(), s.opsCompleted(), s.faultsInjected(), null, null);
        }
        return s;
    }

    public synchronized Status start(Scenario requested) {
        if (status.get().state().equals("RUNNING")) {
            throw new ApiException(HttpStatus.CONFLICT, "BUSY", "a live experiment is already running");
        }
        Scenario scenario = validate(requested);
        Status s = new Status("RUNNING", scenario.name(), System.currentTimeMillis(), 0, scenario.durationMs(), 0, 0, null, null);
        status.set(s);
        runner.submit(() -> {
            try {
                RunResult result = new Run(scenario).execute();
                RunStore.Summary summary = store.put(result, null);
                status.set(new Status("COMPLETE", scenario.name(), s.startedAtMs(),
                        System.currentTimeMillis() - s.startedAtMs(), scenario.durationMs(), result.metrics().opsTotal(),
                        result.faults().size(), summary.id(), null));
            } catch (Throwable t) {
                log.error("live experiment failed", t);
                status.set(new Status("FAILED", scenario.name(), s.startedAtMs(), System.currentTimeMillis() - s.startedAtMs(),
                        scenario.durationMs(), 0, 0, null, t.getClass().getSimpleName() + ": " + t.getMessage()));
            }
        });
        return s;
    }

    private Scenario validate(Scenario requested) {
        Scenario s = new Scenario(requested.name(), requested.description(), cluster.size(), requested.seed(),
                requested.durationMs(), requested.cluster(), requested.workload(), requested.faults()).normalized();
        if (s.durationMs() > MAX_DURATION_MS) throw ApiException.badRequest("live runs are limited to " + MAX_DURATION_MS + " ms");
        for (Scenario.FaultStep f : s.faults()) {
            if (f.start().atMs() == null || f.end() != null) {
                throw ApiException.badRequest("live runs support time-based faults only (start.atMs + durationMs)");
            }
            if (f.type() == Scenario.FaultType.DISK_LOSS) {
                throw ApiException.badRequest("DISK_LOSS is simulator-only: it would destroy a real node's data");
            }
        }
        return s;
    }

    @PreDestroy
    void stop() {
        runner.shutdownNow();
    }

    // =====================================================================================

    /** One live run. */
    private final class Run {
        final Scenario scenario;
        final long start = System.currentTimeMillis();
        private final long startNanos = System.nanoTime();

        /**
         * Milliseconds on a monotonic clock, anchored to the wall-clock start of the run. Client operations
         * are timed with this: the linearizability check depends on their real-time order, and the wall
         * clock can step backwards (observed in Docker on Colima: a sequential client's operations were
         * recorded 70+ ms out of order, which produced a false NOT_LINEARIZABLE verdict).
         */
        long now() {
            return start + (System.nanoTime() - startNanos) / 1_000_000;
        }
        final long end;
        final List<Op> history = Collections.synchronizedList(new ArrayList<>());
        final List<RaftEvent> clientEvents = Collections.synchronizedList(new ArrayList<>());
        final List<RunResult.FaultRecord> faultRecords = Collections.synchronizedList(new ArrayList<>());
        final List<RaftEvent> faultEvents = Collections.synchronizedList(new ArrayList<>());
        final List<RunMetrics.Sample> samples = Collections.synchronizedList(new ArrayList<>());
        final Map<String, List<RaftEvent>> nodeEvents = new LinkedHashMap<>();
        final Map<String, Long> lastSeq = new HashMap<>();
        final Map<String, Long> logFrom = new HashMap<>();
        final List<List<NodeObservation>> observationRounds = new ArrayList<>();
        final List<String> gaps = Collections.synchronizedList(new ArrayList<>());
        final AtomicLong opIds = new AtomicLong();
        final AtomicLong okCount = new AtomicLong();
        final AtomicLong notOkCount = new AtomicLong();
        volatile boolean clientsRunning = true;

        Run(Scenario scenario) {
            this.scenario = scenario;
            this.end = start + scenario.durationMs();
            for (ClusterConfig.Member m : cluster.members()) {
                nodeEvents.put(m.id(), new ArrayList<>());
                lastSeq.put(m.id(), -1L);
            }
        }

        RunResult execute() throws Exception {
            // Baseline: only events that happen from now on belong to this run.
            for (ClusterConfig.Member m : cluster.members()) {
                EventLog.Page page = fetchEvents(m.id(), -1, 1);
                lastSeq.put(m.id(), page == null ? 0 : page.latestSeq());
                NodeView v = clusterService.all().stream().filter(n -> n.id().equals(m.id())).findFirst().orElse(null);
                logFrom.put(m.id(), Math.max(1, (v == null ? 1 : v.lastLogIndex()) - 50));
            }
            faults.healAll(); // start from a clean network
            ExecutorService clients = Executors.newVirtualThreadPerTaskExecutor();
            Scenario.Workload w = scenario.workload();
            for (int i = 1; i <= w.clients(); i++) {
                int clientNo = i;
                clients.submit(() -> clientLoop("client-" + clientNo, new Random(scenario.seed() * 31 + clientNo)));
            }
            ExecutorService faultThreads = Executors.newVirtualThreadPerTaskExecutor();
            for (int i = 0; i < scenario.faults().size(); i++) {
                int step = i;
                faultThreads.submit(() -> runFault(step, scenario.faults().get(step)));
            }
            try {
                long nextObserve = 0;
                while (now() < end) {
                    collectEvents();
                    if (now() >= nextObserve) {
                        observe();
                        nextObserve = now() + 500;
                    }
                    publishProgress();
                    Thread.sleep(150);
                }
            } finally {
                clientsRunning = false;
                faultThreads.shutdownNow();
                clients.shutdown();
                clients.awaitTermination(scenario.workload().opTimeoutMs() + 2_000, TimeUnit.MILLISECONDS);
                clients.shutdownNow();
                // Always leave the cluster clean, whatever happened above.
                try {
                    faults.healAll();
                } catch (RuntimeException e) {
                    gaps.add("could not heal all faults at the end of the run: " + e.getMessage());
                }
            }
            closeOpenFaults();
            // Settle: let the cluster converge, keep collecting.
            long settleUntil = now() + SETTLE_MS;
            while (now() < settleUntil) {
                collectEvents();
                Thread.sleep(200);
                if (converged(clusterService.all())) break;
            }
            Thread.sleep(300);
            collectEvents();
            observe();
            return analyze();
        }

        void publishProgress() {
            Status s = status.get();
            status.set(new Status("RUNNING", s.scenario(), s.startedAtMs(), now() - start,
                    s.durationMs(), history.size(), faultRecords.size(), null, null));
        }

        // ---------------------------------------------------------------- clients

        void clientLoop(String client, Random random) {
            Scenario.Workload w = scenario.workload();
            List<ClusterConfig.Member> members = cluster.members();
            long seq = 0;
            long clientSeq = 0;
            while (clientsRunning && now() < end) {
                try {
                    Thread.sleep(w.opIntervalMs() / 2 + random.nextInt(w.opIntervalMs().intValue() + 1));
                } catch (InterruptedException e) {
                    return;
                }
                long opId = opIds.incrementAndGet();
                // Keys are private to this run: the linearizability model starts every key empty, so a key
                // left over from an earlier run would make its first read look like a value nobody wrote.
                String key = "faultlab-" + start + "-" + random.nextInt(w.keys());
                double roll = random.nextDouble();
                Kind kind = roll < w.readRatio() ? Kind.GET : roll < w.readRatio() + w.deleteRatio() ? Kind.DELETE : Kind.PUT;
                String value = kind == Kind.PUT ? client + "-" + opId + "-" + start : null;
                ClusterConfig.Member target = members.get(random.nextInt(members.size()));
                long invoked = now();
                clientEvents.add(new RaftEvent(++clientSeq, invoked, client, RaftEvent.Type.CLIENT_REQUEST, 0, opId, 0, 0,
                        target.id(), kind + " " + key + (value == null ? "" : "=" + value)));
                Outcome outcome;
                String read = null;
                String note;
                try {
                    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(target.httpBaseUrl() + "/api/v1/kv/" + key
                            + (kind == Kind.GET ? "?consistency=" + readMode(w) : "")))
                            .timeout(Duration.ofMillis(w.opTimeoutMs()));
                    switch (kind) {
                        case PUT -> b.PUT(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(Map.of("value", value))))
                                .header("Content-Type", "application/json")
                                .header("X-Client-Id", client + "-" + start).header("X-Request-Seq", Long.toString(++seq));
                        case DELETE -> b.DELETE().header("X-Client-Id", client + "-" + start)
                                .header("X-Request-Seq", Long.toString(++seq));
                        case GET -> b.GET();
                    }
                    HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                    int code = r.statusCode();
                    String error = code >= 400 ? errorCode(r.body()) : null;
                    if (kind == Kind.GET) {
                        if (code == 200) {
                            outcome = Outcome.OK;
                            read = json.readTree(r.body()).get("value").asString();
                        } else if (code == 404 && "NOT_FOUND".equals(error)) {
                            outcome = Outcome.OK;
                        } else {
                            outcome = Outcome.FAILED;
                        }
                    } else if (code / 100 == 2 || (kind == Kind.DELETE && code == 404 && error == null)) {
                        outcome = Outcome.OK;
                    } else if ("OUTCOME_UNKNOWN".equals(error) || "LEADER_UNREACHABLE".equals(error) || code == 504) {
                        outcome = Outcome.INDETERMINATE;
                    } else {
                        outcome = Outcome.FAILED; // NO_LEADER, BUSY, ...: the write was never appended
                    }
                    note = "HTTP " + code + (error == null ? "" : " " + error) + " via " + target.id();
                } catch (ConnectException e) {
                    outcome = Outcome.FAILED; // never reached the node
                    note = "connection refused by " + target.id();
                } catch (HttpTimeoutException e) {
                    outcome = kind == Kind.GET ? Outcome.FAILED : Outcome.INDETERMINATE;
                    note = "timed out at " + target.id();
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    outcome = kind == Kind.GET ? Outcome.FAILED : Outcome.INDETERMINATE;
                    note = e.getClass().getSimpleName() + " from " + target.id();
                }
                long completed = now();
                history.add(new Op(opId, client, kind, key, kind == Kind.GET ? read : value, invoked,
                        outcome == Outcome.INDETERMINATE ? Long.MAX_VALUE : completed, outcome));
                (outcome == Outcome.OK ? okCount : notOkCount).incrementAndGet();
                clientEvents.add(new RaftEvent(++clientSeq, completed, client, RaftEvent.Type.CLIENT_RESPONSE, 0, opId, 0, 0,
                        target.id(), outcome + " " + kind + " " + key
                        + (kind == Kind.GET && outcome == Outcome.OK ? " -> " + (read == null ? "(absent)" : read) : "")
                        + " after " + (completed - invoked) + " ms; " + note));
            }
        }

        String readMode(Scenario.Workload w) {
            return switch (w.readMode()) {
                case LINEARIZABLE -> "linearizable";
                case LEADER_LOCAL -> "leader";
                case STALE -> "stale";
            };
        }

        String errorCode(String body) {
            try {
                JsonNode n = json.readTree(body);
                return n.has("error") && n.get("error").isString() ? n.get("error").asString() : null;
            } catch (RuntimeException e) {
                return null;
            }
        }

        // ---------------------------------------------------------------- faults

        void runFault(int step, Scenario.FaultStep f) {
            try {
                long wait = start + f.start().atMs() - now();
                if (wait > 0) Thread.sleep(wait);
                if (now() >= end) return;
                List<NodeView> views = clusterService.all();
                NodeView leader = clusterService.leader(views);
                LiveFaultRequest request = new LiveFaultRequest(f.type().name(), f.targets(), f.groups(), f.durationMs(),
                        f.delayMs(), f.dropProbability());
                LiveFaultService.Injection injection = faults.inject(request);
                long injectedAt = injection.injectedAtMs();
                int quorum = cluster.quorum();
                boolean hitLeader = leader != null && switch (f.type()) {
                    case CRASH, RESTART, ISOLATE, STORAGE_FAILURE -> injection.resolvedTargets().contains(leader.id());
                    case PARTITION -> injection.directives().getOrDefault(leader.id(), List.of()).stream()
                            .anyMatch(d -> cluster.size() - d.peers().size() < quorum);
                    default -> false;
                };
                long healedAt = f.type() == Scenario.FaultType.RESTART ? injectedAt
                        : f.durationMs() == null ? -1 : injectedAt + f.durationMs();
                synchronized (faultRecords) {
                    faultRecords.add(new RunResult.FaultRecord(step, f.type(), injection.resolvedTargets(),
                            injection.description(), injectedAt, healedAt, leader == null ? null : leader.id(),
                            leader == null ? 0 : leader.term(), leader == null ? 0 : leader.commitIndex(), hitLeader));
                }
                faultEvents.add(new RaftEvent(0, injectedAt, "faultlab", RaftEvent.Type.FAULT_INJECTED, 0, 0, 0, 0,
                        String.join(",", injection.resolvedTargets()), injection.description()
                        + (injection.unreachable().isEmpty() ? "" : " (unreachable: " + injection.unreachable() + ")")));
                if (healedAt > injectedAt) {
                    Thread.sleep(Math.max(0, healedAt - now()));
                    faultEvents.add(new RaftEvent(0, healedAt, "faultlab", RaftEvent.Type.FAULT_HEALED, 0, 0, 0, 0,
                            String.join(",", injection.resolvedTargets()), injection.description()));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                gaps.add("fault step " + step + " (" + f.type() + ") could not be injected: " + e.getMessage());
            }
        }

        /** Faults still open at the end were healed by the final HEAL; record when. */
        void closeOpenFaults() {
            long now = now();
            synchronized (faultRecords) {
                for (int i = 0; i < faultRecords.size(); i++) {
                    RunResult.FaultRecord f = faultRecords.get(i);
                    if (f.healedAtMs() < 0 || f.healedAtMs() > now) {
                        faultRecords.set(i, new RunResult.FaultRecord(f.step(), f.type(), f.targets(), f.description(),
                                f.injectedAtMs(), now, f.leaderAtInjection(), f.termAtInjection(), f.commitAtInjection(),
                                f.hitLeader()));
                        faultEvents.add(new RaftEvent(0, now, "faultlab", RaftEvent.Type.FAULT_HEALED, 0, 0, 0, 0,
                                String.join(",", f.targets()), f.description() + " (healed at end of run)"));
                    }
                }
            }
        }

        // ---------------------------------------------------------------- observation

        void collectEvents() {
            for (ClusterConfig.Member m : cluster.members()) {
                while (true) {
                    long after = lastSeq.get(m.id());
                    EventLog.Page page = fetchEvents(m.id(), after, 5000);
                    if (page == null) break; // unreachable now; we will catch up later if its buffer allows
                    if (page.truncated()) {
                        gaps.add(m.id() + ": events after seq " + after + " were overwritten before they could be read");
                    }
                    for (RaftEvent e : page.events()) {
                        if (e.seq() != lastSeq.get(m.id()) + 1 && lastSeq.get(m.id()) >= 0 && !page.truncated()) {
                            gaps.add(m.id() + ": events " + (lastSeq.get(m.id()) + 1) + ".." + (e.seq() - 1) + " missing");
                        }
                        nodeEvents.get(m.id()).add(e);
                        lastSeq.put(m.id(), e.seq());
                    }
                    if (page.events().size() < 5000) break;
                }
            }
        }

        EventLog.Page fetchEvents(String node, long after, int limit) {
            String path = "/api/v1/events?after=" + after + "&limit=" + limit;
            try {
                if (node.equals(cluster.self().id())) {
                    return after < 0 ? new EventLog.Page(List.of(), 0, runtime.events().latestSeq(), false)
                            : runtime.events().after(after, limit);
                }
                HttpResponse<byte[]> r = http.send(HttpRequest.newBuilder(URI.create(cluster.member(node).httpBaseUrl() + path))
                        .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
                return r.statusCode() == 200 ? json.readValue(r.body(), EventLog.Page.class) : null;
            } catch (Exception e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                return null;
            }
        }

        void observe() {
            List<NodeView> views = clusterService.all();
            List<NodeObservation> round = new ArrayList<>();
            for (ClusterConfig.Member m : cluster.members()) {
                NodeRuntime.LogView view = fetchLog(m.id(), logFrom.get(m.id()));
                long now = now() - start;
                if (view == null) {
                    round.add(new NodeObservation(m.id(), now, 0, "UNREACHABLE", 0, 0, 0, 0, List.of(), 0, false));
                    continue;
                }
                List<NodeObservation.LogRef> refs = new ArrayList<>();
                for (long[] e : view.entries()) refs.add(new NodeObservation.LogRef(e[0], e[1], e[2]));
                // Entries before the window are treated like compacted ones: not compared this round.
                round.add(new NodeObservation(m.id(), now, view.term(), view.role(), view.commitIndex(), view.lastApplied(),
                        view.from() - 1, view.from() - 1 <= view.snapshotIndex() ? view.snapshotTerm() : 0, refs,
                        view.stateDigest(), view.running()));
                // Next window: a little before what this node had committed. A node that is down reports
                // commit index 0; keep its window then, rather than restarting from index 1.
                if (view.running()) {
                    long last = view.entries().isEmpty() ? view.from() - 1 : view.entries().get(view.entries().size() - 1)[0];
                    logFrom.put(m.id(), Math.max(1, Math.min(view.commitIndex(), last) - 200));
                }
            }
            observationRounds.add(round);
            NodeView leader = clusterService.leader(views);
            long lag = leader == null ? 0 : leader.followers().values().stream().mapToLong(NodeView.Follower::lag).max().orElse(0);
            long term = views.stream().mapToLong(NodeView::term).max().orElse(0);
            int up = (int) views.stream().filter(v -> v.reachable() && !"DOWN".equals(v.health())).count();
            if (now() <= end) {
                samples.add(new RunMetrics.Sample(now() - start, leader == null ? 0 : leader.commitIndex(),
                        term, leader == null ? null : leader.id(), up, lag, okCount.get(), notOkCount.get()));
            }
        }

        /**
         * The node's durable log from {@code from} to its end. Pages are fetched until the end is reached:
         * an observation claims "this is the log from here on", so a truncated window would make every
         * committed entry past the first page look missing (a false durability violation).
         */
        NodeRuntime.LogView fetchLog(String node, long from) {
            NodeRuntime.LogView first = fetchLogPage(node, from);
            if (first == null || first.entries().size() < LOG_PAGE) return first;
            List<long[]> all = new ArrayList<>(first.entries());
            NodeRuntime.LogView page = first;
            while (page.entries().size() == LOG_PAGE) {
                long next = page.entries().get(page.entries().size() - 1)[0] + 1;
                page = fetchLogPage(node, next);
                if (page == null) return null; // became unreachable mid-read: no consistent observation
                if (page.snapshotIndex() != first.snapshotIndex() || page.from() != next) {
                    return null; // compacted or truncated between pages: skip this round rather than guess
                }
                all.addAll(page.entries());
            }
            return new NodeRuntime.LogView(first.term(), first.role(), first.commitIndex(), first.lastApplied(),
                    first.snapshotIndex(), first.snapshotTerm(), first.from(), all, first.stateDigest(), first.running());
        }

        NodeRuntime.LogView fetchLogPage(String node, long from) {
            try {
                if (node.equals(cluster.self().id())) return runtime.logView(from, LOG_PAGE);
                HttpResponse<byte[]> r = http.send(HttpRequest.newBuilder(URI.create(cluster.member(node).httpBaseUrl()
                                + "/api/v1/internal/log?from=" + from + "&limit=" + LOG_PAGE)).header("Authorization", auth.header())
                        .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
                return r.statusCode() == 200 ? json.readValue(r.body(), NodeRuntime.LogView.class) : null;
            } catch (Exception e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                return null;
            }
        }

        boolean converged(List<NodeView> views) {
            NodeView leader = clusterService.leader(views);
            if (leader == null) return false;
            return views.stream().allMatch(v -> v.reachable() && "UP".equals(v.health())
                    && v.lastApplied() == leader.commitIndex());
        }

        // ---------------------------------------------------------------- audit

        RunResult analyze() {
            // Merge per-node streams (each in its own sequence order) by time, then renumber globally.
            List<List<RaftEvent>> streams = new ArrayList<>(nodeEvents.values());
            List<RaftEvent> clients;
            synchronized (clientEvents) {
                clients = new ArrayList<>(clientEvents);
            }
            Map<String, List<RaftEvent>> byClient = new LinkedHashMap<>();
            for (RaftEvent e : clients) byClient.computeIfAbsent(e.node(), k -> new ArrayList<>()).add(e);
            streams.addAll(byClient.values());
            List<RaftEvent> faultList;
            synchronized (faultEvents) {
                faultList = new ArrayList<>(faultEvents);
            }
            faultList.sort(Comparator.comparingLong(RaftEvent::timeMs));
            streams.add(faultList);
            List<RaftEvent> merged = mergeByTime(streams);
            List<RaftEvent> events = new ArrayList<>(merged.size());
            long seq = 0;
            for (RaftEvent e : merged) {
                events.add(new RaftEvent(++seq, Math.max(0, e.timeMs() - start), e.node(), e.type(), e.term(), e.index(),
                        e.entryTerm(), e.checksum(), e.peer(), e.detail()));
            }

            SafetyChecker checker = new SafetyChecker(false);
            for (String gap : gaps) checker.noteObservationGap(gap);
            int round = 0;
            for (RaftEvent e : events) {
                checker.onEvent(e);
                while (round < observationRounds.size() && observationRounds.get(round).get(0).timeMs() <= e.timeMs()) {
                    checker.onObservations(observationRounds.get(round++));
                }
            }
            while (round < observationRounds.size()) checker.onObservations(observationRounds.get(round++));
            SafetyReport safety = checker.report();

            List<Op> ops;
            synchronized (history) {
                ops = new ArrayList<>();
                for (Op o : history) {
                    ops.add(new Op(o.id(), o.client(), o.kind(), o.key(), o.value(), o.invokeMs() - start,
                            o.completeMs() == Long.MAX_VALUE ? Long.MAX_VALUE : o.completeMs() - start, o.outcome()));
                }
            }
            LinearizabilityChecker.Report lin = scenario.workload().clients() == 0 ? null : new LinearizabilityChecker().check(ops);
            List<RunResult.FaultRecord> rebased = new ArrayList<>();
            synchronized (faultRecords) {
                for (RunResult.FaultRecord f : faultRecords) {
                    rebased.add(new RunResult.FaultRecord(f.step(), f.type(), f.targets(), f.description(),
                            f.injectedAtMs() - start, f.healedAtMs() < 0 ? -1 : f.healedAtMs() - start, f.leaderAtInjection(),
                            f.termAtInjection(), f.commitAtInjection(), f.hitLeader()));
                }
            }
            rebased.sort(Comparator.comparingLong(RunResult.FaultRecord::injectedAtMs));
            long grace = 2 * runtime.props().electionTimeoutMaxMs() + scenario.workload().opTimeoutMs();
            long messages = 0, dropped = 0;
            for (NodeView v : clusterService.all()) {
                for (var link : v.links().values()) {
                    messages += link.sent();
                    dropped += link.dropped();
                }
            }
            RunMetrics metrics = RunAnalyzer.analyze(events, rebased, ops, List.copyOf(samples), scenario.durationMs(),
                    cluster.size(), grace, new RunAnalyzer.MessageCounts(messages, messages - dropped, dropped));
            List<NodeView> finalViews = clusterService.all();
            RunResult.Liveness liveness = liveness(finalViews, metrics);
            List<RunResult.NodeEnd> finalState = finalViews.stream().map(v -> new RunResult.NodeEnd(v.id(), v.reachable(),
                    v.role(), v.term(), v.lastLogIndex(), v.commitIndex(), v.lastApplied(), v.snapshotIndex(),
                    v.storage() == null ? 0 : v.storage().keys(), 0)).toList();
            RunResult.Headline headline = RunAnalyzer.headline(safety, lin, liveness, metrics);
            List<RunResult.Finding> explanation = new ArrayList<>(Explainer.explain(events, safety, lin, liveness));
            explanation.add(new RunResult.Finding("NOTE", scenario.durationMs(), "Live run",
                    "This run used the real cluster: wall-clock time, real gRPC traffic and real fault injection. Events "
                            + "from different nodes are ordered by their local clocks. It cannot be replayed exactly; "
                            + "replaying it runs the same scenario in the deterministic simulator for comparison.", List.of()));
            return new RunResult(RunResult.Mode.LIVE, scenario, RunAnalyzer.traceHash(events), events, rebased, safety, lin,
                    liveness, metrics, explanation, headline, finalState);
        }

        RunResult.Liveness liveness(List<NodeView> views, RunMetrics metrics) {
            List<String> findings = new ArrayList<>();
            NodeView leader = clusterService.leader(views);
            long serving = views.stream().filter(v -> v.reachable() && !"DOWN".equals(v.health())).count();
            boolean quorum = serving >= cluster.quorum();
            boolean converged = converged(views);
            RunResult.Liveness.Status s;
            if (!quorum) {
                s = RunResult.Liveness.Status.EXPECTED_UNAVAILABILITY;
                findings.add("Only " + serving + " of " + cluster.size() + " nodes were serving at the end of the run.");
            } else if (leader == null) {
                s = RunResult.Liveness.Status.ISSUE;
                findings.add("All faults were healed and a quorum was serving, but no leader existed " + SETTLE_MS
                        + " ms later.");
            } else if (!converged) {
                s = RunResult.Liveness.Status.ISSUE;
                findings.add("Nodes had not converged on leader " + leader.id() + "'s commit index " + leader.commitIndex()
                        + " after the settle period.");
            } else {
                s = RunResult.Liveness.Status.OK;
                findings.add("After all faults were healed, leader " + leader.id() + " (term " + leader.term()
                        + ") was in place and every node had applied index " + leader.commitIndex() + ".");
            }
            if (metrics.failedOutsideFaults() > 0) {
                findings.add(metrics.failedOutsideFaults() + " request(s) did not succeed with no fault active or recently healed.");
                if (s == RunResult.Liveness.Status.OK) s = RunResult.Liveness.Status.ISSUE;
            }
            if (metrics.failedDuringFaults() > 0) {
                findings.add(metrics.failedDuringFaults() + " request(s) did not succeed during or shortly after a fault: "
                        + "expected unavailability.");
            }
            return new RunResult.Liveness(s, quorum, leader == null ? null : leader.id(), converged, findings);
        }
    }

    /** K-way merge of streams that are each already in order, by event time (stable across streams). */
    static List<RaftEvent> mergeByTime(List<List<RaftEvent>> streams) {
        int[] pos = new int[streams.size()];
        List<RaftEvent> out = new ArrayList<>();
        while (true) {
            int best = -1;
            for (int i = 0; i < streams.size(); i++) {
                if (pos[i] < streams.get(i).size() && (best < 0
                        || streams.get(i).get(pos[i]).timeMs() < streams.get(best).get(pos[best]).timeMs())) best = i;
            }
            if (best < 0) return out;
            out.add(streams.get(best).get(pos[best]++));
        }
    }
}
