package io.quorumdb.server.api;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.quorumdb.raft.RaftEvent;
import io.quorumdb.server.config.ClusterConfig;
import io.quorumdb.server.config.NodeProperties;
import io.quorumdb.server.metrics.RequestMetrics;
import io.quorumdb.server.node.EventLog;
import io.quorumdb.server.node.NodeRuntime;
import io.quorumdb.server.node.OperationLog;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Cluster state, health, metrics and history. Read-only; safe to expose to the dashboard. */
@RestController
@RequestMapping("/api/v1")
public class ClusterController {

    public record Health(String status, String node, String role, String leaderId, long term, String failure) {}

    /**
     * @param health HEALTHY: leader + every node up and following it; DEGRADED: leader, but some nodes are down,
     *               unreachable or without a leader;
     *               UNAVAILABLE: no leader (no writes or linearizable reads possible)
     */
    public record ClusterSummary(String clusterId, String health, int size, int quorum, String leaderId, long term,
                                 long commitIndex, long appliedIndex, int healthyNodes, int unavailableNodes,
                                 long maxReplicationLag, String observedBy, long observedAtMs, List<NodeView> nodes) {}

    public record Leader(String leaderId, long term, String httpUrl, long commitIndex) {}

    public record Metrics(String node, int windowSeconds, Map<String, RequestMetrics.Window> requests,
                          List<RequestMetrics.Point> perSecond, long term, long commitIndex, long appliedIndex,
                          long lastLogIndex, int pendingProposals, int queuedTasks, long diskBytes,
                          long heapUsedBytes, long heapMaxBytes, double processCpuLoad, int availableProcessors) {}

    public record Config(String nodeId, String clusterId, List<ClusterConfig.Member> members, int grpcPort,
                         long electionTimeoutMinMs, long electionTimeoutMaxMs, long heartbeatIntervalMs,
                         long snapshotThreshold, boolean preVote, boolean checkQuorum, boolean fsync,
                         long requestTimeoutMs, int maxKeyBytes, int maxValueBytes, boolean faultInjectionEnabled,
                         int eventBufferSize, String dataDir) {}

    private final NodeRuntime runtime;
    private final ClusterService clusterService;
    private final ClusterConfig cluster;
    private final NodeProperties props;
    private final RequestMetrics metrics;
    private final OperationLog ops;
    private final PrometheusMeterRegistry prometheus;
    private final AdminAuth auth;

    public ClusterController(NodeRuntime runtime, ClusterService clusterService, ClusterConfig cluster, NodeProperties props,
                             RequestMetrics metrics, OperationLog ops, PrometheusMeterRegistry prometheus, AdminAuth auth) {
        this.runtime = runtime;
        this.clusterService = clusterService;
        this.cluster = cluster;
        this.props = props;
        this.metrics = metrics;
        this.ops = ops;
        this.prometheus = prometheus;
        this.auth = auth;
    }

    /** Liveness/readiness of this node. 200 while it participates in Raft, 503 if crashed or failed. */
    @GetMapping("/health")
    public ResponseEntity<Health> health() {
        NodeView v = clusterService.local();
        Health h = new Health(v.health(), v.id(), v.role(), v.leaderId(), v.term(), v.failure());
        return ResponseEntity.status("DOWN".equals(v.health()) ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.OK).body(h);
    }

    /** This node only. Used by peers to assemble the cluster view. */
    @GetMapping("/node")
    public NodeView node() {
        return clusterService.local();
    }

    @GetMapping("/nodes")
    public List<NodeView> nodes() {
        return clusterService.all();
    }

    @GetMapping("/cluster")
    public ClusterSummary cluster() {
        List<NodeView> views = clusterService.all();
        NodeView leader = clusterService.leader(views);
        // Healthy = serving and following a known leader. A node that is up but cut off from the leader
        // (DEGRADED) does not count, so the cluster reports DEGRADED while any member is partitioned away.
        int healthy = (int) views.stream().filter(v -> v.reachable() && "UP".equals(v.health())).count();
        long term = views.stream().mapToLong(NodeView::term).max().orElse(0);
        long lag = leader == null ? 0 : leader.followers().values().stream().mapToLong(NodeView.Follower::lag).max().orElse(0);
        String health = leader == null ? "UNAVAILABLE" : healthy == views.size() ? "HEALTHY" : "DEGRADED";
        return new ClusterSummary(cluster.clusterId(), health, cluster.size(), cluster.quorum(),
                leader == null ? null : leader.id(), term, leader == null ? 0 : leader.commitIndex(),
                leader == null ? 0 : leader.lastApplied(), healthy, views.size() - healthy, lag, cluster.self().id(),
                System.currentTimeMillis(), views);
    }

    @GetMapping("/leader")
    public ResponseEntity<?> leader() {
        NodeView leader = clusterService.leader(clusterService.all());
        if (leader == null) {
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "NO_LEADER", "no leader is currently known");
        }
        return ResponseEntity.ok(new Leader(leader.id(), leader.term(), leader.httpUrl(), leader.commitIndex()));
    }

    @GetMapping("/metrics")
    public Metrics metrics(@RequestParam(defaultValue = "10") int window) {
        if (window < 1 || window > 59) throw ApiException.badRequest("window must be between 1 and 59 seconds");
        NodeView v = clusterService.local();
        Runtime rt = Runtime.getRuntime();
        double cpu = -1;
        if (java.lang.management.ManagementFactory.getOperatingSystemMXBean()
                instanceof com.sun.management.OperatingSystemMXBean os) {
            cpu = os.getProcessCpuLoad();
        }
        return new Metrics(v.id(), window, metrics.summary(window), metrics.series(), v.term(), v.commitIndex(),
                v.lastApplied(), v.lastLogIndex(), v.pendingProposals(), runtime.queuedTasks(),
                v.storage() == null ? 0 : v.storage().diskBytes(), rt.totalMemory() - rt.freeMemory(), rt.maxMemory(), cpu,
                rt.availableProcessors());
    }

    /**
     * Request metrics of every reachable node. Per-second counts are summed across nodes (each request is
     * counted once, by the node that served it); latency percentiles cannot be merged, so they are
     * reported per node.
     */
    public record ClusterMetrics(List<RequestMetrics.Point> perSecond, Map<String, Metrics> nodes, List<String> unreachable) {}

    @GetMapping("/cluster/metrics")
    public ClusterMetrics clusterMetrics(@RequestParam(defaultValue = "10") int window) {
        Map<String, Metrics> nodes = new java.util.LinkedHashMap<>();
        nodes.put(cluster.self().id(), metrics(window));
        nodes.putAll(clusterService.peerMetrics(window));
        List<String> unreachable = cluster.members().stream().map(ClusterConfig.Member::id)
                .filter(id -> !nodes.containsKey(id)).toList();
        Map<Long, long[]> sums = new java.util.TreeMap<>();
        for (Metrics m : nodes.values()) {
            for (RequestMetrics.Point p : m.perSecond()) {
                long[] s = sums.computeIfAbsent(p.second(), k -> new long[3]);
                s[0] += p.writes();
                s[1] += p.reads();
                s[2] += p.errors();
            }
        }
        List<RequestMetrics.Point> series = new ArrayList<>();
        sums.forEach((sec, s) -> series.add(new RequestMetrics.Point(sec, s[0], s[1], s[2])));
        // Drop the oldest second if nodes' windows are misaligned (it would be partial).
        if (nodes.size() > 1 && series.size() > 59) series.remove(0);
        return new ClusterMetrics(series, nodes, unreachable);
    }

    @GetMapping(value = "/metrics/prometheus", produces = "text/plain; version=0.0.4; charset=utf-8")
    public String prometheus() {
        return prometheus.scrape();
    }

    /**
     * This node's events. {@code after} pages forward by sequence number; {@code truncated} in the
     * response tells the caller that events it asked for were already overwritten.
     */
    @GetMapping("/events")
    public EventLog.Page events(@RequestParam(defaultValue = "-1") long after, @RequestParam(defaultValue = "500") int limit,
                                @RequestParam(required = false) String types) {
        if (limit < 1 || limit > 5000) throw ApiException.badRequest("limit must be between 1 and 5000");
        EventLog log = runtime.events();
        Set<RaftEvent.Type> wanted = parseTypes(types);
        if (after < 0) {
            // "Latest N": filter first, so frequent replication events cannot crowd out rare ones.
            return new EventLog.Page(wanted == null ? log.latest(limit) : log.latest(limit, wanted), 0, log.latestSeq(), false);
        }
        EventLog.Page page = log.after(after, limit);
        if (wanted == null) return page;
        return new EventLog.Page(page.events().stream().filter(e -> wanted.contains(e.type())).toList(), page.oldestSeq(),
                page.latestSeq(), page.truncated());
    }

    private static Set<RaftEvent.Type> parseTypes(String types) {
        if (types == null || types.isBlank()) return null;
        Set<RaftEvent.Type> wanted = EnumSet.noneOf(RaftEvent.Type.class);
        for (String t : types.split(",")) {
            try {
                wanted.add(RaftEvent.Type.valueOf(t.trim().toUpperCase(Locale.ROOT)));
            } catch (IllegalArgumentException e) {
                throw ApiException.badRequest("unknown event type '" + t.trim() + "'");
            }
        }
        return wanted;
    }

    /** Recent events of every reachable node merged by time (for the timeline). */
    @GetMapping("/cluster/events")
    public List<RaftEvent> clusterEvents(@RequestParam(defaultValue = "300") int limit,
                                         @RequestParam(required = false) String types, HttpServletRequest request) {
        if (limit < 1 || limit > 2000) throw ApiException.badRequest("limit must be between 1 and 2000");
        Set<RaftEvent.Type> wanted = parseTypes(types);
        List<RaftEvent> merged = new ArrayList<>(wanted == null ? runtime.events().latest(limit) : runtime.events().latest(limit, wanted));
        merged.addAll(clusterService.peerEvents(limit, types));
        merged.sort(Comparator.comparingLong(RaftEvent::timeMs).thenComparing(RaftEvent::node).thenComparingLong(RaftEvent::seq));
        return merged.size() > limit ? merged.subList(merged.size() - limit, merged.size()) : merged;
    }

    @GetMapping("/operations")
    public List<OperationLog.Operation> operations(@RequestParam(defaultValue = "200") int limit,
                                                   @RequestParam(required = false) String type,
                                                   @RequestParam(required = false) String status) {
        if (limit < 1 || limit > 2000) throw ApiException.badRequest("limit must be between 1 and 2000");
        return ops.recent(limit, type, status);
    }

    /** Non-sensitive configuration. The admin token is never returned, only whether one is set. */
    @GetMapping("/config")
    public Config config() {
        return new Config(cluster.self().id(), cluster.clusterId(), cluster.members(), props.grpcPort(),
                props.electionTimeoutMinMs(), props.electionTimeoutMaxMs(), props.heartbeatIntervalMs(),
                props.snapshotThreshold(), props.preVote(), props.checkQuorum(), props.fsync(), props.requestTimeoutMs(),
                props.maxKeyBytes(), props.maxValueBytes(), auth.enabled(), props.eventBufferSize(), props.dataDir());
    }
}
