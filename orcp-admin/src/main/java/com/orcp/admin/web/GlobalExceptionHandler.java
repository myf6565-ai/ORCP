package com.orcp.admin.web;

import com.orcp.admin.flink.FlinkRestException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Turns raw exceptions into clean JSON error bodies.
 *
 * <p>Status translation:
 * <ul>
 *   <li>{@link IllegalArgumentException} -> 400 (client-side validation).</li>
 *   <li>{@link FlinkRestException} carrying a 4xx upstream code -> 502
 *       (we pass the JobManager's complaint along).</li>
 *   <li>{@link FlinkRestException} with status 0 (I/O / timeout) -> 504.</li>
 *   <li>Everything else -> 500 with no stack trace in the body.</li>
 * </ul>
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException e) {
        return respond(HttpStatus.BAD_REQUEST, "bad_request", e.getMessage(), null);
    }

    @ExceptionHandler(FlinkRestException.class)
    public ResponseEntity<Map<String, Object>> flinkUpstream(FlinkRestException e) {
        log.warn("flink REST failure: {}", e.getMessage());
        HttpStatus status = e.getStatusCode() == 0
                ? HttpStatus.GATEWAY_TIMEOUT
                : HttpStatus.BAD_GATEWAY;
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("operation", e.getOperation());
        extra.put("upstreamStatus", e.getStatusCode());
        return respond(status, "flink_upstream", e.getMessage(), extra);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> generic(Exception e) {
        log.error("unhandled exception: {}", e.getMessage(), e);
        return respond(HttpStatus.INTERNAL_SERVER_ERROR, "internal_error",
                e.getClass().getSimpleName() + ": " + e.getMessage(), null);
    }

    private static ResponseEntity<Map<String, Object>> respond(HttpStatus status,
                                                               String code,
                                                               String message,
                                                               Map<String, Object> extra) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status.value());
        body.put("error", code);
        body.put("message", message);
        if (extra != null) {
            body.putAll(extra);
        }
        return ResponseEntity.status(status).body(body);
    }
}
