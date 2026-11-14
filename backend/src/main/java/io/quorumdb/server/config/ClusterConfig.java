package io.quorumdb.server.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Validated static cluster membership, parsed from {@link NodeProperties}. */
public record ClusterConfig(String clusterId, Member self, Map<String, Member> peers) {

    /** One cluster member and how to reach it. */
    public record Member(String id, String raftAddress, String httpBaseUrl) {}

    private static final Pattern ID = Pattern.compile("[A-Za-z0-9_-]{1,32}");
    private static final Pattern HOST_PORT = Pattern.compile("[A-Za-z0-9._-]+:\\d{1,5}");

    public static ClusterConfig from(NodeProperties p) {
        require(ID.matcher(p.nodeId()).matches(), "quorumdb.node-id must match " + ID.pattern());
        require(ID.matcher(p.clusterId()).matches(), "quorumdb.cluster-id must match " + ID.pattern());
        require(p.grpcPort() > 0 && p.grpcPort() < 65536, "quorumdb.grpc-port must be a valid port");
        require(p.heartbeatIntervalMs() * 2 <= p.electionTimeoutMinMs()
                        && p.electionTimeoutMinMs() < p.electionTimeoutMaxMs(),
                "need heartbeat-interval-ms * 2 <= election-timeout-min-ms < election-timeout-max-ms");
        require(p.maxKeyBytes() > 0 && p.maxValueBytes() > 0, "size limits must be positive");
        require(p.adminToken().isEmpty() || p.adminToken().length() >= 16,
                "quorumdb.admin-token must be empty (disabled) or at least 16 characters");
        Map<String, Member> peers = new LinkedHashMap<>();
        for (String spec : p.peers().split(",")) {
            if (spec.isBlank()) continue;
            String[] parts = spec.trim().split("=", 3);
            require(parts.length == 3, "peer '" + spec + "' must look like id=host:grpcPort=http://host:port");
            require(ID.matcher(parts[0]).matches(), "invalid peer id '" + parts[0] + "'");
            require(HOST_PORT.matcher(parts[1]).matches(), "invalid raft address '" + parts[1] + "'");
            require(parts[2].startsWith("http://") || parts[2].startsWith("https://"), "invalid http url '" + parts[2] + "'");
            require(!parts[0].equals(p.nodeId()), "peers must not include this node (" + p.nodeId() + ")");
            require(peers.put(parts[0], new Member(parts[0], parts[1], stripSlash(parts[2]))) == null,
                    "duplicate peer id '" + parts[0] + "'");
        }
        Member self = new Member(p.nodeId(), "localhost:" + p.grpcPort(), stripSlash(p.advertisedHttp()));
        return new ClusterConfig(p.clusterId(), self, Map.copyOf(peers));
    }

    public List<String> peerIds() {
        return List.copyOf(new ArrayList<>(peers.keySet()).stream().sorted().toList());
    }

    /** All members including this node, ordered by id. */
    public List<Member> members() {
        List<Member> all = new ArrayList<>(peers.values());
        all.add(self);
        all.sort((a, b) -> a.id().compareTo(b.id()));
        return all;
    }

    public Member member(String id) {
        return id.equals(self.id()) ? self : peers.get(id);
    }

    public int size() {
        return peers.size() + 1;
    }

    public int quorum() {
        return size() / 2 + 1;
    }

    private static String stripSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static void require(boolean ok, String message) {
        if (!ok) throw new IllegalArgumentException("invalid QuorumDB configuration: " + message);
    }
}
