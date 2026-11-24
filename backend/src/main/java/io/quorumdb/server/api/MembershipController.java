package io.quorumdb.server.api;

import io.quorumdb.raft.Outcomes;
import io.quorumdb.server.config.ClusterConfig;
import io.quorumdb.server.config.NodeProperties;
import io.quorumdb.server.node.NodeRuntime;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Single-server membership changes. Changes need the admin token and must be sent to the leader; a follower
 * answers 503 NOT_LEADER with the leader's id in {@code leaderHint}.
 */
@RestController
@RequestMapping("/api/v1/members")
public class MembershipController {
    private final NodeRuntime runtime;
    private final AdminAuth auth;
    private final NodeProperties props;

    public MembershipController(NodeRuntime runtime, AdminAuth auth, NodeProperties props) {
        this.runtime = runtime;
        this.auth = auth;
        this.props = props;
    }

    public record NewMember(String id, String raftAddress, String httpUrl) {}

    public record Members(List<String> members, long index) {}

    /** Voting members according to this node's log (each {@code id} or {@code id=raftAddress=httpUrl}). */
    @GetMapping
    public Members list() {
        return new Members(runtime.members(), 0);
    }

    @PostMapping
    public Members add(@RequestBody NewMember body, HttpServletRequest http) {
        auth.require(http);
        if (body == null || body.id() == null || body.raftAddress() == null || body.httpUrl() == null) {
            throw ApiException.badRequest("body must contain id, raftAddress and httpUrl");
        }
        ClusterConfig.Member member;
        try {
            member = ClusterConfig.parseMember(body.id() + "=" + body.raftAddress() + "=" + body.httpUrl());
        } catch (IllegalArgumentException e) {
            throw ApiException.badRequest(e.getMessage());
        }
        return await(runtime.changeMembership(true, ClusterConfig.spec(member)));
    }

    @DeleteMapping("/{id}")
    public Members remove(@PathVariable String id, HttpServletRequest http) {
        auth.require(http);
        return await(runtime.changeMembership(false, id));
    }

    private Members await(CompletableFuture<Outcomes.Proposal> future) {
        Outcomes.Proposal outcome;
        try {
            outcome = future.get(props.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalArgumentException bad) throw ApiException.badRequest(bad.getMessage());
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL", String.valueOf(e.getCause()));
        } catch (TimeoutException e) {
            throw new ApiException(HttpStatus.GATEWAY_TIMEOUT, "OUTCOME_UNKNOWN",
                    "membership change not committed within " + props.requestTimeoutMs() + " ms; it may still take effect");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "OUTCOME_UNKNOWN", "interrupted");
        }
        return switch (outcome.status()) {
            case OK -> new Members(runtime.members(), outcome.index());
            case NOT_LEADER -> throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "NOT_LEADER",
                    "membership changes must be sent to the leader", outcome.leaderHint());
            case BUSY -> throw new ApiException(HttpStatus.TOO_MANY_REQUESTS, "BUSY",
                    "another membership change is still uncommitted, or the leader has not committed an entry of its term yet; retry",
                    outcome.leaderHint());
            case UNKNOWN -> throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "OUTCOME_UNKNOWN",
                    "leadership changed before the change was committed; check GET /api/v1/members", outcome.leaderHint());
        };
    }
}
