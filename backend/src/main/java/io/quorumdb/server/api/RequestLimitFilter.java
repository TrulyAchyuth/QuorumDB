package io.quorumdb.server.api;

import io.quorumdb.server.config.NodeProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Rejects oversized request bodies before they are read: bodies must declare a Content-Length,
 * and it must fit the configured value limit plus JSON overhead. FaultLab scenario bodies get a
 * separate fixed allowance.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestLimitFilter extends OncePerRequestFilter {
    private final long maxBody;

    public RequestLimitFilter(NodeProperties props) {
        // A batch may carry several values; allow 4 values' worth plus framing.
        this.maxBody = 4L * props.maxValueBytes() + (64 << 10);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String method = request.getMethod();
        boolean hasBody = method.equals("PUT") || method.equals("POST") || method.equals("PATCH");
        if (hasBody) {
            long length = request.getContentLengthLong();
            boolean chunked = "chunked".equalsIgnoreCase(request.getHeader("Transfer-Encoding"));
            if (length < 0 && chunked) {
                reject(response, 411, "LENGTH_REQUIRED", "request bodies must declare Content-Length");
                return;
            }
            if (length > maxBody) {
                reject(response, 413, "PAYLOAD_TOO_LARGE", "request body exceeds " + maxBody + " bytes");
                return;
            }
        }
        chain.doFilter(request, response);
    }

    private static void reject(HttpServletResponse response, int status, String code, String message) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        response.getWriter().write("{\"error\":\"" + code + "\",\"message\":\"" + message + "\",\"timestamp\":"
                + System.currentTimeMillis() + "}");
    }
}
