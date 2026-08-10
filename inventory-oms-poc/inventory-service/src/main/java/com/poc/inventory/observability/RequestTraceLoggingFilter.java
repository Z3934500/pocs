package com.poc.inventory.observability;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Logs request timing and the current trace context without logging request bodies. */
@Component
public class RequestTraceLoggingFilter extends OncePerRequestFilter {
    private static final Logger log = LoggerFactory.getLogger(RequestTraceLoggingFilter.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        long started = System.nanoTime();
        try {
            chain.doFilter(request, response);
        } finally {
            log.info("http_request method={} path={} status={} traceId={} spanId={} durationMs={}",
                    request.getMethod(), request.getRequestURI(), response.getStatus(),
                    valueOrDash(MDC.get("traceId")), valueOrDash(MDC.get("spanId")),
                    (System.nanoTime() - started) / 1_000_000.0);
        }
    }

    private static String valueOrDash(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }
}
