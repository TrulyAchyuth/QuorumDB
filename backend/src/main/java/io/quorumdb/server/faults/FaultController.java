package io.quorumdb.server.faults;

import io.quorumdb.server.api.AdminAuth;
import io.quorumdb.server.api.ApiException;
import io.quorumdb.server.config.ClusterConfig;
import io.quorumdb.server.node.NodeRuntime;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Live fault injection ({@code /api/v1/faults}, admin token required) and the node-internal
 * endpoints peers use to coordinate faults and audits ({@code /api/v1/internal/*}, same token).
 */
@RestController
@RequestMapping("/api/v1")
public class FaultController {
    private final LiveFaultService faults;
    private final NodeRuntime runtime;
    private final AdminAuth auth;
    private final ClusterConfig cluster;

    public FaultController(LiveFaultService faults, NodeRuntime runtime, AdminAuth auth, ClusterConfig cluster) {
        this.faults = faults;
        this.runtime = runtime;
        this.auth = auth;
        this.cluster = cluster;
    }

    @PostMapping("/faults")
    public ResponseEntity<LiveFaultService.Injection> inject(@RequestBody LiveFaultRequest request, HttpServletRequest http) {
        auth.require(http);
        LiveFaultService.Injection injection = faults.inject(request);
        return ResponseEntity.status(injection.unreachable().isEmpty() ? HttpStatus.CREATED : HttpStatus.MULTI_STATUS)
                .body(injection);
    }

    @DeleteMapping("/faults")
    public LiveFaultService.Injection healAll(HttpServletRequest http) {
        auth.require(http);
        return faults.healAll();
    }

    /** Faults in force on this node (no token needed: read-only, contains no secrets). */
    @GetMapping("/faults")
    public List<LiveFaultService.Active> active() {
        return faults.active();
    }

    @PostMapping("/snapshot")
    public ResponseEntity<?> snapshot(HttpServletRequest http) {
        auth.require(http);
        runtime.snapshot();
        return ResponseEntity.ok(runtime.storageInfo());
    }

    // ---- internal ----

    @PostMapping("/internal/faults")
    public ResponseEntity<Void> applyDirective(@RequestBody FaultDirective directive, HttpServletRequest http) {
        auth.require(http);
        if (directive == null || directive.action() == null || directive.faultId() == null) {
            throw ApiException.badRequest("invalid directive");
        }
        faults.apply(directive);
        return ResponseEntity.noContent().build();
    }

    /** A window of the durable log as (index, term, checksum), for the live safety audit. */
    @GetMapping("/internal/log")
    public NodeRuntime.LogView log(@RequestParam(defaultValue = "1") long from, @RequestParam(defaultValue = "1000") int limit,
                                   HttpServletRequest http) {
        auth.require(http);
        if (limit < 1 || limit > 10_000) throw ApiException.badRequest("limit must be between 1 and 10000");
        return runtime.logView(Math.max(1, from), limit);
    }

    String self() {
        return cluster.self().id();
    }
}
