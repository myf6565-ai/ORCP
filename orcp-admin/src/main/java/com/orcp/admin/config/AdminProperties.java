package com.orcp.admin.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration bundle for every external the admin service touches.
 *
 * <p>Values are bound from {@code application.yml} (defaults) and overridden
 * by Nacos data-id {@code orcp-admin.yaml} at bootstrap time.
 */
@Data
@Component
@ConfigurationProperties(prefix = "orcp")
public class AdminProperties {

    private Flink flink = new Flink();
    private Kafka kafka = new Kafka();
    private Mysql mysql = new Mysql();
    private Oceanbase oceanbase = new Oceanbase();
    private Job job = new Job();

    @Data
    public static class Flink {
        /** REST endpoint for the JobManager, e.g. http://node-1:8081 */
        private String restUrl = "http://node-1:8081";
        /** Bearer timeout, milliseconds, applied to every HTTP call. */
        private int connectTimeoutMs = 3_000;
        private int readTimeoutMs = 30_000;
        /** Long-running call limit (jar upload + job submit). */
        private int writeTimeoutMs = 120_000;
    }

    @Data
    public static class Kafka {
        private String bootstrapServers = "node-1:9092,node-2:9092,node-3:9092";
        private int probeTimeoutMs = 2_000;
    }

    @Data
    public static class Mysql {
        private String url = "jdbc:mysql://node-1:3306/orcp_detail?useSSL=false&serverTimezone=Asia/Shanghai";
        private String user = "orcp_ro";
        private String password = "ChangeMe_ro_1!";
        private int probeTimeoutSeconds = 2;
    }

    @Data
    public static class Oceanbase {
        /** Note: keep URL on the jdbc:mysql:// scheme (see DEV_SPEC §11 + Stage E). */
        private String url = "jdbc:mysql://ob-host:2881/orcp_dw?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai";
        private String user = "orcp_rw@tenant#cluster";
        private String password = "ChangeMe_ob_1!";
        private int probeTimeoutSeconds = 3;
    }

    @Data
    public static class Job {
        /** Default entry class used when the operator omits it. */
        private String defaultEntryClass = "com.orcp.flink.OrcpFlinkJob";
        /** Default parallelism applied to every submit that omits it. */
        private int defaultParallelism = 2;
        /** Where savepoints land.  Passed as target-directory to Flink REST. */
        private String savepointDir = "file:///data/flink/savepoints";
    }
}
