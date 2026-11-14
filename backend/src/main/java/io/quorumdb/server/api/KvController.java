package io.quorumdb.server.api;

import io.quorumdb.kv.KvCommand;
import io.quorumdb.kv.KvResult;
import io.quorumdb.kv.KvStateMachine;
import io.quorumdb.raft.Outcomes;
import io.quorumdb.server.config.ClusterConfig;
import io.quorumdb.server.config.NodeProperties;
import io.quorumdb.server.metrics.RequestMetrics;
import io.quorumdb.server.node.NodeRuntime;
import io.quorumdb.server.node.NodeRuntime.ReadMode;
import io.quorumdb.server.node.OperationLog;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Key-value API. See CONSISTENCY_MODEL.md for the exact guarantees.
 *
 * <ul>
 *   <li>Writes are acknowledged (2xx) only after the entry is committed by a majority and applied on
 *       the leader. Any node accepts a write and forwards it to the leader.</li>
 *   <li>Reads take {@code ?consistency=linearizable} (default; ReadIndex on the leader),
 *       {@code leader} (leader's local state, no confirmation; may be stale during a partition) or
 *       {@code stale} (this node's local state, any role; may be arbitrarily stale).</li>
 *   <li>{@code X-Client-Id} + {@code X-Request-Seq} make a write idempotent: retrying the same pair
 *       never applies it twice.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/v1")
public class KvController {
    private static final Pattern CLIENT_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");
    private static final Duration FORWARD_TIMEOUT = Duration.ofSeconds(10);

    /** PUT body. {@code expectedVersion}: omit for unconditional, 0 = create only, n = compare-and-set. */
    public record PutRequest(String value, Long expectedVersion) {}

    public record BatchOp(String op, String key, String value, Long expectedVersion) {}

    public record BatchRequest(List<BatchOp> ops) {}

    public record WriteResponse(String key, boolean applied, boolean existed, long version, long index, long term,
                                boolean duplicate, String servedBy) {}

    public record ReadResponse(String key, String value, long version, String consistency, long readIndex,
                               String servedBy) {}

    public record ScanResponse(List<Item> items, String consistency, long readIndex, String servedBy) {
        public record Item(String key, String value, long version) {}
    }

    public record BatchResponse(boolean applied, List<KvResult.OpResult> results, long index, long term,
                                boolean duplicate, String servedBy) {}

    private final NodeRuntime runtime;
    private final NodeProperties props;
    private final ClusterConfig cluster;
    private final PeerClient peers;
    private final RequestMetrics metrics;
    private final OperationLog ops;
    private final JsonMapper json;

    public KvController(NodeRuntime runtime, NodeProperties props, ClusterConfig cluster, PeerClient peers,
                        RequestMetrics metrics, OperationLog ops, JsonMapper json) {
        this.runtime = runtime;
        this.props = props;
        this.cluster = cluster;
        this.peers = peers;
        this.metrics = metrics;
        this.ops = ops;
        this.json = json;
    }

    // =====================================================================================
    // Writes
    // =====================================================================================

    @PutMapping("/kv/{key}")
    public ResponseEntity<?> put(@PathVariable String key, @RequestBody PutRequest body,
                                 @RequestHeader(value = "X-Client-Id", required = false) String clientId,
                                 @RequestHeader(value = "X-Request-Seq", required = false) Long seq,
                                 HttpServletRequest request) {
        validateKey(key);
        if (body == null || body.value() == null) throw ApiException.badRequest("body must be {\"value\": \"...\"}");
        validateValue(body.value());
        long expected = body.expectedVersion() == null ? KvCommand.ANY_VERSION : body.expectedVersion();
        if (expected < KvCommand.ANY_VERSION) throw ApiException.badRequest("expectedVersion must be >= 0");
        KvCommand command = KvCommand.single(clientIdOf(clientId, seq), seqOf(clientId, seq),
                new KvCommand.Op(KvCommand.OpType.PUT, key, body.value(), expected));
        return write("PUT", key, command, clientId, request);
    }

    @DeleteMapping("/kv/{key}")
    public ResponseEntity<?> delete(@PathVariable String key,
                                    @RequestParam(required = false) Long expectedVersion,
                                    @RequestHeader(value = "X-Client-Id", required = false) String clientId,
                                    @RequestHeader(value = "X-Request-Seq", required = false) Long seq,
                                    HttpServletRequest request) {
        validateKey(key);
        long expected = expectedVersion == null ? KvCommand.ANY_VERSION : expectedVersion;
        KvCommand command = KvCommand.single(clientIdOf(clientId, seq), seqOf(clientId, seq),
                new KvCommand.Op(KvCommand.OpType.DELETE, key, null, expected));
        return write("DELETE", key, command, clientId, request);
    }

    @PostMapping("/batch")
    public ResponseEntity<?> batch(@RequestBody BatchRequest body,
                                   @RequestHeader(value = "X-Client-Id", required = false) String clientId,
                                   @RequestHeader(value = "X-Request-Seq", required = false) Long seq,
                                   HttpServletRequest request) {
        if (body == null || body.ops() == null || body.ops().isEmpty() || body.ops().size() > 100) {
            throw ApiException.badRequest("a batch needs between 1 and 100 ops");
        }
        List<KvCommand.Op> commandOps = new ArrayList<>();
        for (BatchOp op : body.ops()) {
            if (op == null || op.op() == null) throw ApiException.badRequest("each op needs an 'op' of PUT or DELETE");
            validateKey(op.key());
            long expected = op.expectedVersion() == null ? KvCommand.ANY_VERSION : op.expectedVersion();
            switch (op.op().toUpperCase(Locale.ROOT)) {
                case "PUT" -> {
                    if (op.value() == null) throw ApiException.badRequest("PUT of '" + op.key() + "' needs a value");
                    validateValue(op.value());
                    commandOps.add(new KvCommand.Op(KvCommand.OpType.PUT, op.key(), op.value(), expected));
                }
                case "DELETE" -> commandOps.add(new KvCommand.Op(KvCommand.OpType.DELETE, op.key(), null, expected));
                default -> throw ApiException.badRequest("unknown op '" + op.op() + "'");
            }
        }
        return write("BATCH", commandOps.size() + " keys",
                new KvCommand(clientIdOf(clientId, seq), seqOf(clientId, seq), commandOps), clientId, request);
    }

    private ResponseEntity<?> write(String type, String key, KvCommand command, String clientId,
                                    HttpServletRequest request) {
        long started = System.nanoTime();
        Outcomes.Proposal outcome = await(runtime.write(command), type, key, clientId, started);
        String self = cluster.self().id();
        switch (outcome.status()) {
            case OK -> {
                KvResult result = KvResult.decode(outcome.result());
                long micros = micros(started);
                metrics.record(RequestMetrics.Kind.WRITE, micros, true);
                ops.add(System.currentTimeMillis(), type, key, null, result.applied() ? "OK" : "CONFLICT", micros,
                        outcome.index(), outcome.term(), 0, true, self, clientId);
                return writeResponse(type, key, command, result, outcome, self);
            }
            case NOT_LEADER -> {
                return forwardOrFail(type, key, outcome.leaderHint(), started, clientId, request);
            }
            case BUSY -> {
                fail(type, key, "BUSY", started, clientId);
                throw new ApiException(HttpStatus.TOO_MANY_REQUESTS, "BUSY", "leader is overloaded; retry later", self);
            }
            default -> { // UNKNOWN
                fail(type, key, "UNKNOWN", started, clientId);
                throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "OUTCOME_UNKNOWN",
                        "leadership changed before the write's fate was known; it may or may not have been applied. "
                                + "Retry with the same X-Client-Id/X-Request-Seq to make the retry safe.", outcome.leaderHint());
            }
        }
    }

    private ResponseEntity<?> writeResponse(String type, String key, KvCommand command, KvResult result,
                                            Outcomes.Proposal outcome, String self) {
        if (type.equals("BATCH")) {
            BatchResponse body = new BatchResponse(result.applied(), result.ops(), outcome.index(), outcome.term(),
                    result.duplicate(), self);
            return ResponseEntity.status(result.applied() ? HttpStatus.OK : HttpStatus.CONFLICT).header("X-QuorumDB-Node", self)
                    .body(body);
        }
        KvResult.OpResult op = result.ops().isEmpty() ? new KvResult.OpResult(false, 0) : result.ops().get(0);
        if (!result.applied()) {
            if (result.ops().isEmpty()) {
                throw new ApiException(HttpStatus.BAD_REQUEST, "REJECTED", "the command was rejected by the state machine");
            }
            throw new ApiException(HttpStatus.CONFLICT, "VERSION_MISMATCH", "expected version "
                    + command.ops().get(0).expectedVersion() + " but the key's version is " + op.version());
        }
        WriteResponse body = new WriteResponse(key, true, op.existed(), op.version(), outcome.index(), outcome.term(),
                result.duplicate(), self);
        HttpStatus status = type.equals("DELETE") && !op.existed() ? HttpStatus.NOT_FOUND
                : type.equals("PUT") && !op.existed() ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).header("X-QuorumDB-Node", self).body(body);
    }

    // =====================================================================================
    // Reads
    // =====================================================================================

    @GetMapping("/kv/{key}")
    public ResponseEntity<?> get(@PathVariable String key, @RequestParam(defaultValue = "linearizable") String consistency,
                                 HttpServletRequest request) {
        validateKey(key);
        ReadMode mode = mode(consistency);
        long started = System.nanoTime();
        NodeRuntime.ReadResult r = await(runtime.read(key, mode), "GET", key, null, started);
        String self = cluster.self().id();
        if (r.status() != Outcomes.Status.OK) {
            if (r.status() == Outcomes.Status.NOT_LEADER && mode != ReadMode.STALE) {
                return forwardOrFail("GET", key, r.leaderHint(), started, null, request);
            }
            fail("GET", key, r.status().name(), started, null);
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "NOT_AVAILABLE", "read could not be served: "
                    + r.status(), r.leaderHint());
        }
        long micros = micros(started);
        metrics.record(RequestMetrics.Kind.READ, micros, true);
        ops.add(System.currentTimeMillis(), "GET", key, mode.name(), r.value() == null ? "NOT_FOUND" : "OK", micros, 0, 0,
                r.readIndex(), false, self, null);
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-QuorumDB-Node", self);
        headers.add("X-QuorumDB-Read-Index", Long.toString(r.readIndex()));
        if (r.value() == null) {
            ApiError err = new ApiError("NOT_FOUND", "key '" + key + "' does not exist", self, null, System.currentTimeMillis());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).headers(headers).body(err);
        }
        return ResponseEntity.ok().headers(headers).body(new ReadResponse(key, r.value().value(), r.value().version(),
                mode.name().toLowerCase(Locale.ROOT), r.readIndex(), self));
    }

    @GetMapping("/kv")
    public ResponseEntity<?> scan(@RequestParam(defaultValue = "") String prefix,
                                  @RequestParam(defaultValue = "100") int limit,
                                  @RequestParam(defaultValue = "linearizable") String consistency,
                                  HttpServletRequest request) {
        if (limit < 1 || limit > 1000) throw ApiException.badRequest("limit must be between 1 and 1000");
        if (prefix.length() > props.maxKeyBytes()) throw ApiException.badRequest("prefix too long");
        ReadMode mode = mode(consistency);
        long started = System.nanoTime();
        NodeRuntime.ScanResult r = await(runtime.scan(prefix, limit, mode), "SCAN", prefix, null, started);
        if (r.status() != Outcomes.Status.OK) {
            if (r.status() == Outcomes.Status.NOT_LEADER && mode != ReadMode.STALE) {
                return forwardOrFail("SCAN", prefix, r.leaderHint(), started, null, request);
            }
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "NOT_AVAILABLE", "scan could not be served", r.leaderHint());
        }
        long micros = micros(started);
        metrics.record(RequestMetrics.Kind.READ, micros, true);
        ops.add(System.currentTimeMillis(), "SCAN", prefix, mode.name(), "OK", micros, 0, 0, r.readIndex(), false,
                cluster.self().id(), null);
        List<ScanResponse.Item> items = r.entries().stream()
                .map(e -> new ScanResponse.Item(e.getKey(), e.getValue().value(), e.getValue().version())).toList();
        return ResponseEntity.ok(new ScanResponse(items, mode.name().toLowerCase(Locale.ROOT), r.readIndex(),
                cluster.self().id()));
    }

    // =====================================================================================
    // Forwarding to the leader
    // =====================================================================================

    private ResponseEntity<?> forwardOrFail(String type, String key, String leaderHint, long started, String clientId,
                                            HttpServletRequest request) {
        String self = cluster.self().id();
        boolean alreadyForwarded = request.getHeader(PeerClient.FORWARDED_HEADER) != null;
        if (leaderHint == null || leaderHint.equals(self) || alreadyForwarded || cluster.member(leaderHint) == null) {
            fail(type, key, "NOT_LEADER", started, clientId);
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "NO_LEADER",
                    leaderHint == null ? "no leader is currently known; the cluster may be electing one or lack a quorum"
                            : "request reached " + self + " which is not the leader", leaderHint);
        }
        try {
            byte[] body = request.getInputStream().readAllBytes();
            Map<String, String> headers = new LinkedHashMap<>();
            Enumeration<String> names = request.getHeaderNames();
            while (names.hasMoreElements()) {
                String n = names.nextElement();
                headers.put(n, request.getHeader(n));
            }
            String path = request.getRequestURI() + (request.getQueryString() == null ? "" : "?" + request.getQueryString());
            PeerClient.Response r = peers.sendSync(leaderHint, request.getMethod(), path,
                    body.length == 0 && cachedBody(request) != null ? cachedBody(request) : body, headers, FORWARD_TIMEOUT);
            long micros = micros(started);
            ops.add(System.currentTimeMillis(), type, key, null, "FORWARDED", micros, 0, 0, 0, r.status() < 300, leaderHint,
                    clientId);
            HttpHeaders out = new HttpHeaders();
            r.headers().forEach((k, v) -> {
                if (k.toLowerCase(Locale.ROOT).startsWith("x-quorumdb") || k.equalsIgnoreCase("content-type")
                        || k.equalsIgnoreCase("retry-after")) out.addAll(k, v);
            });
            out.add("X-QuorumDB-Forwarded-To", leaderHint);
            return ResponseEntity.status(r.status()).headers(out).body(r.body());
        } catch (java.io.IOException e) {
            fail(type, key, "LEADER_UNREACHABLE", started, clientId);
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "LEADER_UNREACHABLE",
                    "could not reach leader " + leaderHint + ": " + e.getMessage(), leaderHint);
        }
    }

    /** Spring has already consumed the body for @RequestBody; re-serialise it for forwarding. */
    private byte[] cachedBody(HttpServletRequest request) {
        Object attr = request.getAttribute(BODY_ATTR);
        return attr instanceof byte[] b ? b : null;
    }

    static final String BODY_ATTR = "quorumdb.rawBody";

    // =====================================================================================
    // Helpers
    // =====================================================================================

    private <T> T await(CompletableFuture<T> future, String type, String key, String clientId, long started) {
        try {
            return future.get(props.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            fail(type, key, "TIMEOUT", started, clientId);
            boolean write = !type.equals("GET") && !type.equals("SCAN");
            throw new ApiException(HttpStatus.GATEWAY_TIMEOUT, write ? "OUTCOME_UNKNOWN" : "TIMEOUT",
                    write ? "no commit within " + props.requestTimeoutMs() + " ms; the write may still be applied later. "
                            + "Retry with the same X-Client-Id/X-Request-Seq to make the retry safe."
                            : "read not served within " + props.requestTimeoutMs() + " ms (no quorum?)");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "INTERRUPTED", "request interrupted");
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    private void fail(String type, String key, String status, long started, String clientId) {
        long micros = micros(started);
        metrics.record(type.equals("GET") || type.equals("SCAN") ? RequestMetrics.Kind.READ : RequestMetrics.Kind.WRITE,
                micros, false);
        ops.add(System.currentTimeMillis(), type, key, null, status, micros, 0, 0, 0, false, cluster.self().id(), clientId);
    }

    private static long micros(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1000;
    }

    private void validateKey(String key) {
        if (key == null || key.isEmpty()) throw ApiException.badRequest("key must not be empty");
        if (key.getBytes(StandardCharsets.UTF_8).length > props.maxKeyBytes()) {
            throw ApiException.badRequest("key exceeds " + props.maxKeyBytes() + " bytes");
        }
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (Character.isISOControl(c) || c == '/') throw ApiException.badRequest("key contains '/' or a control character");
        }
    }

    private void validateValue(String value) {
        if (value.getBytes(StandardCharsets.UTF_8).length > props.maxValueBytes()) {
            throw new ApiException(HttpStatus.PAYLOAD_TOO_LARGE, "VALUE_TOO_LARGE", "value exceeds " + props.maxValueBytes() + " bytes");
        }
    }

    private static ReadMode mode(String consistency) {
        return switch (consistency.toLowerCase(Locale.ROOT)) {
            case "linearizable", "strong" -> ReadMode.LINEARIZABLE;
            case "leader" -> ReadMode.LEADER;
            case "stale", "local" -> ReadMode.STALE;
            default -> throw ApiException.badRequest("consistency must be linearizable, leader or stale");
        };
    }

    private static String clientIdOf(String clientId, Long seq) {
        if (clientId == null && seq == null) return "";
        if (clientId == null || seq == null) throw ApiException.badRequest("X-Client-Id and X-Request-Seq must be sent together");
        if (!CLIENT_ID.matcher(clientId).matches()) throw ApiException.badRequest("X-Client-Id must match " + CLIENT_ID.pattern());
        if (seq < 1) throw ApiException.badRequest("X-Request-Seq must be positive");
        return clientId;
    }

    private static long seqOf(String clientId, Long seq) {
        return seq == null ? 0 : seq;
    }

    JsonMapper json() {
        return json;
    }
}
