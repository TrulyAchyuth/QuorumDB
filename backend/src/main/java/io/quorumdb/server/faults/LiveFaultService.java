package io.quorumdb.server.faults;

import io.quorumdb.raft.RaftEvent;
import io.quorumdb.server.api.AdminAuth;
import io.quorumdb.server.api.ApiException;
import io.quorumdb.server.api.ClusterService;
import io.quorumdb.server.api.NodeView;
import io.quorumdb.server.api.PeerClient;
import io.quorumdb.server.config.ClusterConfig;
import io.quorumdb.server.node.NodeRuntime;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Live fault injection on a real cluster.
 *
 * <p>The node that receives a fault request is the coordinator: it resolves selectors such as
 * {@code leader} against the current cluster view, turns the fault into per-node
 * {@link FaultDirective}s and sends each node its part. Network faults are applied on both ends of a
 * link, so a partition is symmetric even if one side missed its directive. Each node expires its own
 * directives, so faults with a duration heal even if the coordinator crashes.
 *
 * <p>A live CRASH stops the Raft node and discards its volatile state while the HTTP server stays up
 * to report it; recovery uses the same code path as a process start. For real process kills use
 * {@code docker kill} or {@code scripts/cluster.sh kill}.
 */
@Service
public class LiveFaultService {
    private static final Logger log = LoggerFactory.getLogger(LiveFaultService.class);
    private static final Duration PEER_TIMEOUT = Duration.ofSeconds(3);

    /** A directive that is in force on this node. */
    public record Active(FaultDirective directive, long appliedAtMs) {}

    /** What the coordinator did: the directives per node and which nodes could not be reached. */
    public record Injection(String faultId, String type, String description, List<String> resolvedTargets,
                            Map<String, List<FaultDirective>> directives, List<String> unreachable, long injectedAtMs,
                            long expiresAtMs) {}

    private final NodeRuntime runtime;
    private final ClusterConfig cluster;
    private final ClusterService clusterService;
    private final PeerClient peers;
    private final AdminAuth auth;
    private final JsonMapper json;
    private final Map<String, Active> active = new ConcurrentHashMap<>();
    private final ScheduledExecutorService expiry = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "fault-expiry");
        t.setDaemon(true);
        return t;
    });

    public LiveFaultService(NodeRuntime runtime, ClusterConfig cluster, ClusterService clusterService, PeerClient peers,
                            AdminAuth auth, JsonMapper json) {
        this.runtime = runtime;
        this.cluster = cluster;
        this.clusterService = clusterService;
        this.peers = peers;
        this.auth = auth;
        this.json = json;
        expiry.scheduleWithFixedDelay(this::expire, 100, 100, TimeUnit.MILLISECONDS);
    }

    // =====================================================================================
    // Coordinator
    // =====================================================================================

    public Injection inject(LiveFaultRequest request) {
        if (request == null || request.type() == null) throw ApiException.badRequest("type is required");
        String type = request.type().toUpperCase(Locale.ROOT);
        long now = System.currentTimeMillis();
        if (request.durationMs() != null && (request.durationMs() < 1 || request.durationMs() > 3_600_000)) {
            throw ApiException.badRequest("durationMs must be between 1 and 3600000");
        }
        long expiresAt = request.durationMs() == null ? 0 : now + request.durationMs();
        String id = UUID.randomUUID().toString().substring(0, 8);
        List<NodeView> views = clusterService.all();
        List<String> all = cluster.members().stream().map(ClusterConfig.Member::id).toList();
        Map<String, List<FaultDirective>> plan = new LinkedHashMap<>();
        List<String> targets;
        String description;

        switch (type) {
            case "HEAL" -> {
                targets = all;
                description = "HEAL all faults";
                for (String n : all) add(plan, n, new FaultDirective(id, FaultDirective.Action.HEAL, List.of(), 0, 0, 0, description));
            }
            case "CRASH", "RESTART", "STORAGE_FAILURE" -> {
                targets = resolve(request.targets(), views, all);
                description = type + " " + targets + duration(request);
                FaultDirective.Action action = FaultDirective.Action.valueOf(type);
                for (String t : targets) add(plan, t, new FaultDirective(id, action, List.of(), 0, 0,
                        type.equals("RESTART") ? 0 : expiresAt, description));
            }
            case "ISOLATE" -> {
                targets = resolve(request.targets(), views, all);
                description = "ISOLATE " + targets + duration(request);
                for (String n : all) {
                    List<String> cut = targets.contains(n) ? all.stream().filter(o -> !o.equals(n)).toList() : targets;
                    if (!cut.isEmpty()) add(plan, n, new FaultDirective(id, FaultDirective.Action.BLOCK, cut, 0, 0, expiresAt, description));
                }
            }
            case "PARTITION" -> {
                List<List<String>> groups = new ArrayList<>();
                if (request.groups() != null && request.groups().size() >= 2) {
                    for (List<String> g : request.groups()) groups.add(resolve(g, views, all));
                } else {
                    List<String> side = resolve(request.targets(), views, all);
                    groups.add(side);
                    groups.add(all.stream().filter(n -> !side.contains(n)).toList());
                }
                Set<String> seen = new HashSet<>();
                for (List<String> g : groups) for (String n : g) {
                    if (!seen.add(n)) throw ApiException.badRequest("node " + n + " is in two groups");
                }
                // Nodes not named in any group form one more group.
                List<String> rest = all.stream().filter(n -> !seen.contains(n)).toList();
                if (!rest.isEmpty()) groups.add(rest);
                targets = all;
                description = "PARTITION " + groups + duration(request);
                for (List<String> g : groups) {
                    for (String n : g) {
                        List<String> cut = all.stream().filter(o -> !g.contains(o)).toList();
                        add(plan, n, new FaultDirective(id, FaultDirective.Action.BLOCK, cut, 0, 0, expiresAt, description));
                    }
                }
            }
            case "DELAY" -> {
                if (request.delayMs() == null || request.delayMs() < 1 || request.delayMs() > 60_000) {
                    throw ApiException.badRequest("DELAY needs delayMs between 1 and 60000");
                }
                targets = resolve(request.targets(), views, all);
                description = "DELAY " + request.delayMs() + " ms on " + targets + duration(request);
                for (String t : targets) add(plan, t, new FaultDirective(id, FaultDirective.Action.DELAY, List.of(),
                        request.delayMs(), 0, expiresAt, description));
            }
            case "DROP" -> {
                if (request.dropProbability() == null || request.dropProbability() <= 0 || request.dropProbability() > 1) {
                    throw ApiException.badRequest("DROP needs dropProbability in (0, 1]");
                }
                targets = resolve(request.targets(), views, all);
                description = "DROP " + Math.round(request.dropProbability() * 100) + "% on " + targets + duration(request);
                for (String t : targets) add(plan, t, new FaultDirective(id, FaultDirective.Action.DROP, List.of(), 0,
                        request.dropProbability(), expiresAt, description));
            }
            default -> throw ApiException.badRequest("unknown fault type '" + request.type() + "'");
        }

        List<String> unreachable = new ArrayList<>();
        for (Map.Entry<String, List<FaultDirective>> e : plan.entrySet()) {
            for (FaultDirective d : e.getValue()) {
                if (!deliver(e.getKey(), d)) {
                    unreachable.add(e.getKey());
                    break;
                }
            }
        }
        log.info("injected fault {}: {} (unreachable: {})", id, description, unreachable);
        return new Injection(id, type, description, targets, plan, unreachable, now, expiresAt);
    }

    private static String duration(LiveFaultRequest r) {
        return r.durationMs() == null ? "" : " for " + r.durationMs() + " ms";
    }

    private static void add(Map<String, List<FaultDirective>> plan, String node, FaultDirective d) {
        plan.computeIfAbsent(node, k -> new ArrayList<>()).add(d);
    }

    private List<String> resolve(List<String> selectors, List<NodeView> views, List<String> all) {
        if (selectors == null || selectors.isEmpty()) throw ApiException.badRequest("targets are required");
        NodeView leader = clusterService.leader(views);
        List<String> out = new ArrayList<>();
        for (String s : selectors) {
            String id = switch (s) {
                case "leader" -> {
                    if (leader == null) throw new ApiException(HttpStatus.CONFLICT, "NO_LEADER", "there is no leader to target");
                    yield leader.id();
                }
                case "follower" -> {
                    List<String> followers = views.stream().filter(v -> v.reachable() && "FOLLOWER".equals(v.role())
                            && !out.contains(v.id())).map(NodeView::id).toList();
                    if (followers.isEmpty()) throw new ApiException(HttpStatus.CONFLICT, "NO_FOLLOWER", "no follower to target");
                    yield followers.get(ThreadLocalRandom.current().nextInt(followers.size()));
                }
                case "random" -> {
                    List<String> candidates = all.stream().filter(n -> !out.contains(n)).toList();
                    yield candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
                }
                default -> {
                    if (!all.contains(s)) throw ApiException.badRequest("unknown node '" + s + "'");
                    yield s;
                }
            };
            if (!out.contains(id)) out.add(id);
        }
        return out;
    }

    private boolean deliver(String node, FaultDirective d) {
        if (node.equals(cluster.self().id())) {
            apply(d);
            return true;
        }
        try {
            PeerClient.Response r = peers.sendSync(node, "POST", "/api/v1/internal/faults", json.writeValueAsBytes(d),
                    Map.of("Content-Type", "application/json", "Authorization", auth.header()), PEER_TIMEOUT);
            return r.status() / 100 == 2;
        } catch (java.io.IOException e) {
            return false;
        }
    }

    /** Heals all faults on every reachable node. */
    public Injection healAll() {
        return inject(new LiveFaultRequest("HEAL", null, null, null, null, null));
    }

    // =====================================================================================
    // Local application
    // =====================================================================================

    public synchronized void apply(FaultDirective d) {
        long now = System.currentTimeMillis();
        if (d.expiresAtMs() > 0 && d.expiresAtMs() <= now) return; // arrived after it was due to end
        runtime.recordExternal(RaftEvent.Type.FAULT_INJECTED, d.description() + " [" + d.action() + "]");
        switch (d.action()) {
            case HEAL -> {
                for (Active a : List.copyOf(active.values())) revert(a.directive(), "healed");
                runtime.faults().healAll();
                return;
            }
            case BLOCK -> {
                runtime.faults().block(Set.copyOf(d.peers()));
                d.peers().forEach(p -> runtime.recordExternal(RaftEvent.Type.NODE_DISCONNECTED, "link to " + p + " cut"));
            }
            case DELAY -> runtime.faults().setDelay(d.delayMs());
            case DROP -> runtime.faults().setDrop(d.dropProbability());
            case CRASH -> runtime.crash();
            case RESTART -> runtime.restart();
            case STORAGE_FAILURE -> runtime.setStorageFailure(true);
        }
        if (d.action() != FaultDirective.Action.RESTART) {
            active.put(d.faultId() + "/" + d.action(), new Active(d, now));
        }
    }

    private synchronized void expire() {
        long now = System.currentTimeMillis();
        for (Active a : List.copyOf(active.values())) {
            if (a.directive().expiresAtMs() > 0 && a.directive().expiresAtMs() <= now) {
                try {
                    revert(a.directive(), "expired");
                } catch (RuntimeException e) {
                    log.warn("failed to revert fault {}", a.directive().faultId(), e);
                }
            }
        }
    }

    private void revert(FaultDirective d, String why) {
        active.remove(d.faultId() + "/" + d.action());
        switch (d.action()) {
            case BLOCK -> {
                // Only unblock peers that no other active directive still blocks.
                Set<String> stillBlocked = new HashSet<>();
                active.values().stream().filter(a -> a.directive().action() == FaultDirective.Action.BLOCK)
                        .forEach(a -> stillBlocked.addAll(a.directive().peers()));
                Set<String> release = new HashSet<>(d.peers());
                release.removeAll(stillBlocked);
                runtime.faults().unblock(release);
                release.forEach(p -> runtime.recordExternal(RaftEvent.Type.NODE_RECOVERED, "link to " + p + " restored"));
            }
            case DELAY -> runtime.faults().setDelay(0);
            case DROP -> runtime.faults().setDrop(0);
            case CRASH -> runtime.restart();
            case STORAGE_FAILURE -> {
                runtime.setStorageFailure(false);
                if (runtime.status() == null || !runtime.status().running()) runtime.restart();
            }
            default -> { }
        }
        runtime.recordExternal(RaftEvent.Type.FAULT_HEALED, d.description() + " [" + d.action() + " " + why + "]");
    }

    public List<Active> active() {
        return List.copyOf(active.values());
    }

    public List<String> activeDescriptions() {
        return active.values().stream().map(a -> a.directive().description() + " [" + a.directive().action() + "]")
                .distinct().toList();
    }

    @PreDestroy
    void stop() {
        expiry.shutdownNow();
    }
}
