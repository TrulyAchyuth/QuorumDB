package io.quorumdb.server.api;

import io.quorumdb.server.config.ClusterConfig;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** HTTP calls to other members: request forwarding to the leader and management fan-out. */
@Component
public class PeerClient {
    public static final String FORWARDED_HEADER = "X-QuorumDB-Forwarded-By";
    private static final Set<String> COPIED_REQUEST_HEADERS = Set.of("content-type", "x-client-id", "x-request-seq",
            "authorization");

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500))
            .version(HttpClient.Version.HTTP_1_1).build();
    private final ClusterConfig cluster;

    public PeerClient(ClusterConfig cluster) {
        this.cluster = cluster;
    }

    public record Response(int status, Map<String, java.util.List<String>> headers, byte[] body) {}

    public CompletableFuture<Response> send(String peerId, String method, String pathAndQuery, byte[] body,
                                            Map<String, String> headers, Duration timeout) {
        ClusterConfig.Member member = cluster.member(peerId);
        if (member == null) return CompletableFuture.failedFuture(new IllegalArgumentException("unknown member " + peerId));
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(member.httpBaseUrl() + pathAndQuery)).timeout(timeout)
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body))
                .header(FORWARDED_HEADER, cluster.self().id());
        headers.forEach((k, v) -> {
            if (COPIED_REQUEST_HEADERS.contains(k.toLowerCase())) b.header(k, v);
        });
        return http.sendAsync(b.build(), HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(r -> new Response(r.statusCode(), r.headers().map(), r.body()));
    }

    public Response sendSync(String peerId, String method, String pathAndQuery, byte[] body, Map<String, String> headers,
                             Duration timeout) throws IOException {
        try {
            return send(peerId, method, pathAndQuery, body, headers, timeout).join();
        } catch (java.util.concurrent.CompletionException e) {
            throw e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
        }
    }
}
