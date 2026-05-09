package com.orcp.admin.web;

import com.orcp.admin.service.HealthAggregator;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Aggregated health endpoint called out by DEV_SPEC §8.3.  Deliberately
 * served under {@code /api/health} -- distinct from {@code /actuator/health}
 * which only covers this process.  The {@code /api/health} endpoint is
 * anonymous so monitoring systems can scrape without a credential.
 *
 * <p>Returns HTTP 200 if every subsystem is UP, 503 otherwise, so that
 * liveness/readiness probes, ingress health checks and systemd
 * {@code ExecStartPost} scripts can key on the status line without parsing
 * the body.
 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class HealthController {

    private final HealthAggregator aggregator;

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> report = aggregator.report();
        int status = "UP".equals(report.get("status")) ? 200 : 503;
        return ResponseEntity.status(status).body(report);
    }
}
