package com.orcp.admin.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.orcp.admin.config.AdminProperties;
import com.orcp.admin.flink.FlinkRestClient;
import com.orcp.admin.flink.FlinkRestException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 对各子系统执行独立探针，并将结果合并为单一健康报告。
 *
 * <p>每个探针相互独立：某个子系统失败不会影响其他探针的执行。
 * 聚合状态为"UP"当且仅当所有探针均返回 UP。
 * 此行为与 DEV_SPEC §8.3 保持一致。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class HealthAggregator {

    private final AdminProperties properties;
    private final FlinkRestClient flinkRestClient;

    public Map<String, Object> report() {
        long start = System.nanoTime();
        Map<String, Object> components = new LinkedHashMap<>();

        ComponentHealth kafka = probeKafka();
        components.put("kafka", kafka.toMap());
        ComponentHealth flink = probeFlink();
        components.put("flink", flink.toMap());
        ComponentHealth mysql = probeMysql();
        components.put("mysql", mysql.toMap());
        ComponentHealth oceanbase = probeOceanBase();
        components.put("oceanbase", oceanbase.toMap());

        boolean allUp = kafka.up && flink.up && mysql.up && oceanbase.up;

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", allUp ? "UP" : "DOWN");
        out.put("components", components);
        out.put("elapsedMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        return out;
    }

    // ------------------------------------------------------------------
    // Kafka：AdminClient.describeCluster
    // ------------------------------------------------------------------

    private ComponentHealth probeKafka() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.getKafka().getBootstrapServers());
        int timeoutMs = properties.getKafka().getProbeTimeoutMs();
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);

        try (AdminClient ac = AdminClient.create(props)) {
            DescribeClusterResult result = ac.describeCluster();
            int nodeCount = result.nodes().get(timeoutMs, TimeUnit.MILLISECONDS).size();
            String clusterId = result.clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("clusterId", clusterId);
            details.put("nodes", nodeCount);
            return ComponentHealth.up("kafka", details);
        } catch (Exception e) {
            log.warn("Kafka 健康探针失败：{}", e.getMessage());
            return ComponentHealth.down("kafka", e.toString());
        }
    }

    // ------------------------------------------------------------------
    // Flink：REST /overview
    // ------------------------------------------------------------------

    private ComponentHealth probeFlink() {
        try {
            JsonNode overview = flinkRestClient.overview();
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("flinkVersion", overview.path("flink-version").asText(null));
            details.put("slotsTotal", overview.path("slots-total").asInt(0));
            details.put("slotsAvailable", overview.path("slots-available").asInt(0));
            details.put("taskManagers", overview.path("taskmanagers").asInt(0));
            details.put("jobsRunning", overview.path("jobs-running").asInt(0));
            details.put("jobsFailed", overview.path("jobs-failed").asInt(0));
            return ComponentHealth.up("flink", details);
        } catch (FlinkRestException e) {
            log.warn("Flink 健康探针失败：{}", e.getMessage());
            return ComponentHealth.down("flink", e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // MySQL + OceanBase：执行 SELECT 1（带短超时）
    // ------------------------------------------------------------------

    private ComponentHealth probeMysql() {
        AdminProperties.Mysql cfg = properties.getMysql();
        return probeJdbc("mysql", cfg.getUrl(), cfg.getUser(), cfg.getPassword(),
                cfg.getProbeTimeoutSeconds());
    }

    private ComponentHealth probeOceanBase() {
        AdminProperties.Oceanbase cfg = properties.getOceanbase();
        return probeJdbc("oceanbase", cfg.getUrl(), cfg.getUser(), cfg.getPassword(),
                cfg.getProbeTimeoutSeconds());
    }

    private ComponentHealth probeJdbc(String label, String url, String user,
                                      String password, int timeoutSeconds) {
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", password);
        // MySQL 8 和 OceanBase 2.4 均支持通过 Properties 传递超时参数，
        // 比 URL 参数更健壮（对未来 URL 变更更友好）。
        p.setProperty("connectTimeout", String.valueOf(timeoutSeconds * 1000L));
        p.setProperty("socketTimeout", String.valueOf(timeoutSeconds * 1000L));

        long t0 = System.nanoTime();
        try (Connection conn = DriverManager.getConnection(url, p);
             Statement st = conn.createStatement()) {
            st.setQueryTimeout(timeoutSeconds);
            try (java.sql.ResultSet rs = st.executeQuery("SELECT 1")) {
                if (!rs.next()) {
                    return ComponentHealth.down(label, "SELECT 1 未返回任何行");
                }
            }
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("latencyMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0));
            return ComponentHealth.up(label, details);
        } catch (Exception e) {
            log.warn("{} 健康探针失败：{}", label, e.getMessage());
            return ComponentHealth.down(label, e.toString());
        }
    }

    // ------------------------------------------------------------------
    // 内部 DTO
    // ------------------------------------------------------------------

    private static final class ComponentHealth {
        final String name;
        final boolean up;
        final Map<String, Object> details;
        final String error;

        private ComponentHealth(String name, boolean up,
                                Map<String, Object> details, String error) {
            this.name = name;
            this.up = up;
            this.details = details;
            this.error = error;
        }

        static ComponentHealth up(String name, Map<String, Object> details) {
            return new ComponentHealth(name, true, details, null);
        }

        static ComponentHealth down(String name, String error) {
            return new ComponentHealth(name, false, null, error);
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("status", up ? "UP" : "DOWN");
            if (up && details != null && !details.isEmpty()) {
                m.put("details", details);
            }
            if (!up && error != null) {
                m.put("error", error);
            }
            return m;
        }
    }
}
