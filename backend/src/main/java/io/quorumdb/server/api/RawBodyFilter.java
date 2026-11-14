package io.quorumdb.server.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Buffers KV request bodies (already bounded by {@link RequestLimitFilter}) so that a follower can
 * forward the exact original bytes to the leader after Spring has parsed them.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class RawBodyFilter extends OncePerRequestFilter {

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String uri = request.getRequestURI();
        return !(uri.startsWith("/api/v1/kv") || uri.startsWith("/api/v1/batch"));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        byte[] body = request.getInputStream().readAllBytes();
        request.setAttribute(KvController.BODY_ATTR, body);
        chain.doFilter(new HttpServletRequestWrapper(request) {
            @Override
            public ServletInputStream getInputStream() {
                ByteArrayInputStream in = new ByteArrayInputStream(body);
                return new ServletInputStream() {
                    @Override public boolean isFinished() { return in.available() == 0; }
                    @Override public boolean isReady() { return true; }
                    @Override public void setReadListener(ReadListener listener) { throw new UnsupportedOperationException(); }
                    @Override public int read() { return in.read(); }
                    @Override public int read(byte[] b, int off, int len) { return in.read(b, off, len); }
                };
            }
        }, response);
    }
}
