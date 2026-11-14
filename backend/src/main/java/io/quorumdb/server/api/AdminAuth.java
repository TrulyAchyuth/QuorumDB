package io.quorumdb.server.api;

import io.quorumdb.server.config.NodeProperties;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Guards endpoints that can disrupt the cluster (live fault injection) or expose internals.
 * Requires {@code Authorization: Bearer <quorumdb.admin-token>}. With no token configured these
 * endpoints are disabled entirely: the secure default.
 */
@Component
public class AdminAuth {
    private final byte[] token;

    public AdminAuth(NodeProperties props) {
        this.token = props.adminToken().getBytes(StandardCharsets.UTF_8);
    }

    public boolean enabled() {
        return token.length > 0;
    }

    public String header() {
        return "Bearer " + new String(token, StandardCharsets.UTF_8);
    }

    public void require(HttpServletRequest request) {
        if (!enabled()) {
            throw new ApiException(HttpStatus.FORBIDDEN, "ADMIN_DISABLED",
                    "fault injection is disabled on this node; start it with quorumdb.admin-token set");
        }
        String header = request.getHeader("Authorization");
        byte[] given = header != null && header.startsWith("Bearer ")
                ? header.substring(7).getBytes(StandardCharsets.UTF_8) : new byte[0];
        if (!MessageDigest.isEqual(given, token)) { // constant-time comparison
            throw new ApiException(HttpStatus.UNAUTHORIZED, "UNAUTHORIZED", "missing or wrong admin token");
        }
    }
}
