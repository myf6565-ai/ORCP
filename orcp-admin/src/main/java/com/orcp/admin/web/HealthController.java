package com.orcp.admin.web;

import com.orcp.admin.service.HealthAggregator;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * DEV_SPEC §8.3 描述的聚合健康检查端点。
 *
 * <p>注意：本端点位于 {@code /api/health}，而非 {@code /actuator/health}——
 * 后者仅反映当前进程自身的健康状态。{@code /api/health} 是匿名开放的，
 * 监控系统无需凭据即可抓取。
 *
 * <p>返回规则：所有子系统 UP 时返回 HTTP 200，否则返回 503，
 * 以便存活/就绪探针、Ingress 健康检查和 systemd {@code ExecStartPost} 脚本
 * 直接通过状态码判断，无需解析响应体。
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
