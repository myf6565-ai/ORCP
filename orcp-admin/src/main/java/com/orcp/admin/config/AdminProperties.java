package com.orcp.admin.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 管控服务涉及的所有外部依赖配置集合。
 *
 * <p>各属性从 {@code application.yml}（默认值）读取，
 * 并在启动时由 Nacos data-id {@code orcp-admin.yaml} 覆盖。
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
        /** JobManager REST 端点，例如 http://node-1:8081 */
        private String restUrl = "http://node-1:8081";
        /** 连接超时（毫秒），适用于每次 HTTP 调用。 */
        private int connectTimeoutMs = 3_000;
        private int readTimeoutMs = 30_000;
        /** 长耗时调用（jar 上传 + 作业提交）的超时上限。 */
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
        /** URL 前缀使用 jdbc:mysql://（参见 DEV_SPEC §11 及阶段 E 说明）。 */
        private String url = "jdbc:mysql://ob-host:2881/orcp_dw?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai";
        private String user = "orcp_rw@tenant#cluster";
        private String password = "ChangeMe_ob_1!";
        private int probeTimeoutSeconds = 3;
    }

    @Data
    public static class Job {
        /** 运营省略 entryClass 时使用的默认入口类。 */
        private String defaultEntryClass = "com.orcp.flink.OrcpFlinkJob";
        /** 未指定并行度时的默认值。 */
        private int defaultParallelism = 2;
        /** savepoint 存放目录，作为 target-directory 传递给 Flink REST。 */
        private String savepointDir = "file:///data/flink/savepoints";
    }
}
