package com.flightmonitor.monitor.api;

import com.flightmonitor.monitor.lifecycle.LifecycleSettings;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Local-only browser defenses, not authentication: fixed Host (DNS rebinding), exact Origin,
 * Fetch Metadata and a required non-simple mutation header. No CORS permissions are granted.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public final class LocalApiProtection extends OncePerRequestFilter {
    private static final int BODY_LIMIT = 4_096;
    private final String origin;
    private final String authority;

    public LocalApiProtection(LifecycleSettings settings) {
        origin = settings.publicOrigin();
        authority = URI.create(origin).getRawAuthority();
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        // Never select protected routes using an undecoded URI prefix: encoded characters
        // and matrix parameters can map to the same MVC route with a different raw prefix.
        return (request.getMethod().equals("GET") || request.getMethod().equals("HEAD"))
                && (request.getRequestURI().equals("/actuator/health")
                || request.getRequestURI().equals("/actuator/health/readiness")
                || request.getRequestURI().equals("/actuator/health/liveness"));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        response.setHeader("Cache-Control", "no-store");
        response.setHeader("X-Content-Type-Options", "nosniff");
        if (!authority.equalsIgnoreCase(request.getHeader("Host"))) {
            reject(response, 403, "UNTRUSTED_HOST");
            return;
        }
        String suppliedOrigin = request.getHeader("Origin");
        String fetchSite = request.getHeader("Sec-Fetch-Site");
        if ((suppliedOrigin != null && !origin.equals(suppliedOrigin))
                || (fetchSite != null && !fetchSite.equals("same-origin") && !fetchSite.equals("none"))) {
            reject(response, 403, "CROSS_ORIGIN_DENIED");
            return;
        }
        boolean mutation = request.getMethod().equals("POST") || request.getMethod().equals("DELETE");
        if (mutation && !"1".equals(request.getHeader("X-Flight-Monitor"))) {
            reject(response, 403, "MUTATION_HEADER_REQUIRED");
            return;
        }
        if (mutation) {
            if (request.getContentLengthLong() > BODY_LIMIT) {
                reject(response, 400, "BODY_TOO_LARGE");
                return;
            }
            byte[] body = request.getInputStream().readNBytes(BODY_LIMIT + 1);
            if (body.length > BODY_LIMIT || (request.getMethod().equals("DELETE") && body.length != 0)) {
                reject(response, 400, "INVALID_BODY");
                return;
            }
            chain.doFilter(new BufferedRequest(request, body), response);
        } else {
            chain.doFilter(request, response);
        }
    }

    private static void reject(HttpServletResponse response, int status, String code) throws IOException {
        response.setStatus(status);
        response.setContentType("application/problem+json");
        response.getWriter().write("{\"status\":" + status + ",\"title\":\"" + code + "\",\"code\":\"" + code + "\"}");
    }

    private static final class BufferedRequest extends HttpServletRequestWrapper {
        private final byte[] body;

        private BufferedRequest(HttpServletRequest request, byte[] body) {
            super(request);
            this.body = body;
        }

        @Override
        public ServletInputStream getInputStream() {
            ByteArrayInputStream stream = new ByteArrayInputStream(body);
            return new ServletInputStream() {
                @Override
                public int read() { return stream.read(); }
                @Override
                public boolean isFinished() { return stream.available() == 0; }
                @Override
                public boolean isReady() { return true; }
                @Override
                public void setReadListener(ReadListener listener) {
                    throw new UnsupportedOperationException("Synchronous bounded requests only");
                }
            };
        }

        @Override
        public BufferedReader getReader() {
            return new BufferedReader(new InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
        }
    }
}
