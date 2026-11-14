package io.quorumdb.server.api;

import io.quorumdb.raft.RaftNode;
import io.quorumdb.raft.RaftStatus;
import io.quorumdb.server.config.ClusterConfig;
import io.quorumdb.server.faults.LiveFaultService;
import io.quorumdb.server.node.NodeRuntime;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Builds this node's {@link NodeView} and collects the views of all members. */
@Service
public class ClusterService {
    private static final Duration PEER_TIMEOUT = Duration.ofMillis(800);
    private static final long CACHE_MS = 250;

    private final NodeRuntime runtime;
    private final ClusterConfig cluster;
    private final PeerClient peers;
    private final JsonMapper json;
    private final LiveFaultService faults;
    private volatile List<NodeView> cached;
    private volatile long cachedAt;

    public ClusterService(NodeRuntime runtime, ClusterConfig cluster, PeerClient peers, JsonMapper json,
                          @Lazy LiveFaultService faults) {
        this.runtime = runtime;
        this.cluster = cluster;
        this.peers = peers;
        this.json = json;
        this.faults = faults;
    }

    public NodeView local() {
        RaftStatus s = runtime.status();
        NodeRuntime.Storage storage = runtime.storageInfo();
        boolean serving = s != null && s.running() && !runtime.isCrashed();
        String health = !serving ? "DOWN" : s.leaderId() == null ? "DEGRADED" : "UP";
        Map<String, NodeView.Follower> followers = new LinkedHashMap<>();
        if (serving && s.role() == RaftNode.Role.LEADER) {
            s.peers().forEach((peer, p) -> followers.put(peer, new NodeView.Follower(p.nextIndex(), p.matchIndex(),
                    Math.max(0, s.lastLogIndex() - p.matchIndex()), p.lastAckAgoMs())));
        }
        var nf = runtime.faults();
        NodeView.Faults faultView = new NodeView.Faults(List.copyOf(nf.blockedPeers()), nf.globalDelayMs(),
                nf.globalDropProbability(), storage.failingWrites(), runtime.isCrashed(), faults.activeDescriptions());
        ClusterConfig.Member self = cluster.self();
        return new NodeView(self.id(), self.httpBaseUrl(), self.raftAddress(), true, health,
                serving ? s.role().name() : "DOWN", s == null ? 0 : s.term(), serving ? s.leaderId() : null,
                s == null ? null : s.votedFor(), serving ? s.commitIndex() : 0, serving ? s.lastApplied() : 0,
                s == null ? 0 : s.lastLogIndex(), s == null ? 0 : s.lastLogTerm(), s == null ? 0 : s.snapshotIndex(),
                s == null ? 0 : s.pendingProposals(), s == null ? null : s.failure(),
                System.currentTimeMillis() - runtime.startedAtMs(), followers, runtime.transport().peerViews(), faultView,
                new NodeView.StorageView(storage.diskBytes(), storage.snapshotIndex(), storage.lastIndex(), storage.keys(),
                        storage.fsync()), System.currentTimeMillis());
    }

    /** Views of all members, ordered by id. Unreachable peers are included as such. Cached briefly. */
    public List<NodeView> all() {
        long now = System.currentTimeMillis();
        List<NodeView> c = cached;
        if (c != null && now - cachedAt < CACHE_MS) return c;
        Map<String, CompletableFuture<NodeView>> pending = new LinkedHashMap<>();
        for (ClusterConfig.Member m : cluster.members()) {
            if (m.id().equals(cluster.self().id())) {
                pending.put(m.id(), CompletableFuture.completedFuture(local()));
                continue;
            }
            pending.put(m.id(), peers.send(m.id(), "GET", "/api/v1/node", null, Map.of(), PEER_TIMEOUT)
                    .thenApply(r -> r.status() == 200 ? json.readValue(r.body(), NodeView.class)
                            : NodeView.unreachable(m.id(), m.httpBaseUrl(), m.raftAddress(), "HTTP " + r.status()))
                    .exceptionally(e -> NodeView.unreachable(m.id(), m.httpBaseUrl(), m.raftAddress(),
                            rootMessage(e))));
        }
        List<NodeView> out = new ArrayList<>();
        for (CompletableFuture<NodeView> f : pending.values()) out.add(f.join());
        cached = List.copyOf(out);
        cachedAt = now;
        return cached;
    }

    /** The node that a majority-backed view says is leader: the serving leader with the highest term. */
    public NodeView leader(List<NodeView> views) {
        NodeView best = null;
        for (NodeView v : views) {
            if (v.reachable() && "LEADER".equals(v.role()) && (best == null || v.term() > best.term())) best = v;
        }
        return best;
    }

    /** Request metrics of every reachable peer. */
    public Map<String, ClusterController.Metrics> peerMetrics(int window) {
        Map<String, CompletableFuture<ClusterController.Metrics>> pending = new LinkedHashMap<>();
        for (String peer : cluster.peerIds()) {
            pending.put(peer, peers.send(peer, "GET", "/api/v1/metrics?window=" + window, null, Map.of(), PEER_TIMEOUT)
                    .thenApply(r -> r.status() == 200 ? json.readValue(r.body(), ClusterController.Metrics.class) : null)
                    .exceptionally(e -> null));
        }
        Map<String, ClusterController.Metrics> out = new LinkedHashMap<>();
        pending.forEach((peer, f) -> {
            ClusterController.Metrics m = f.join();
            if (m != null) out.put(peer, m);
        });
        return out;
    }

    /** The latest events of every reachable peer (best effort, short timeout). */
    public List<io.quorumdb.raft.RaftEvent> peerEvents(int limit, String types) {
        List<CompletableFuture<List<io.quorumdb.raft.RaftEvent>>> pending = new ArrayList<>();
        for (String peer : cluster.peerIds()) {
            pending.add(peers.send(peer, "GET", "/api/v1/events?limit=" + limit
                    + (types == null || types.isBlank() ? "" : "&types=" + java.net.URLEncoder.encode(types, java.nio.charset.StandardCharsets.UTF_8)), null, Map.of(), PEER_TIMEOUT)
                    .thenApply(r -> r.status() == 200
                            ? json.readValue(r.body(), io.quorumdb.server.node.EventLog.Page.class).events()
                            : List.<io.quorumdb.raft.RaftEvent>of())
                    .exceptionally(e -> List.of()));
        }
        List<io.quorumdb.raft.RaftEvent> out = new ArrayList<>();
        for (var f : pending) out.addAll(f.join());
        return out;
    }

    private static String rootMessage(Throwable e) {
        while (e.getCause() != null) e = e.getCause();
        return e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
    }
}
