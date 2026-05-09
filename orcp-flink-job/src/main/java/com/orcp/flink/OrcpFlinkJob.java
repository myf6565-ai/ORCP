package com.orcp.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Objects;

/**
 * orcp-flink-job 入口类。
 *
 * <p>流水线：Kafka source（{@code orcp.mid.events}）
 * -> MySQL JDBC lookup 维表关联
 * -> 1 分钟滚动窗口聚合
 * -> OceanBase JDBC upsert sink（{@code agg_order_1min}）。
 *
 * <p>所有 DDL 外置于 {@code src/main/resources/sql/}；
 * 本类仅负责配置执行环境并将 SQL 委托给 {@link SqlLoader} 执行。
 *
 * <p>参数优先级（先匹配者生效）：
 * <ol>
 *   <li>命令行 {@code --key value} 参数</li>
 *   <li>jar 包内 {@code job.properties} 中的默认值</li>
 * </ol>
 * 敏感信息（DB 密码、Kafka SASL）应在提交时通过 CLI 注入，切勿打包进 jar。
 */
public final class OrcpFlinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(OrcpFlinkJob.class);

    private static final String JOB_NAME = "orcp-flink-job";
    private static final String JOB_PROPERTIES_RESOURCE = "/job.properties";

    /** SQL 文件按此顺序执行，后面的文件可引用前面文件中定义的表/视图。 */
    private static final String[] SQL_RESOURCES = new String[] {
            "/sql/01_source_kafka.sql",
            "/sql/02_dim_mysql.sql",
            "/sql/03_sink_oceanbase.sql",
            "/sql/10_pipeline.sql",
    };

    private OrcpFlinkJob() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = buildParams(args);
        LOG.info("启动 {}，共 {} 个参数", JOB_NAME, params.getProperties().size());

        StreamExecutionEnvironment env = buildEnvironment(params);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // 将作业名称写入 Pipeline 配置，使 Flink Web UI 显示友好名称。
        tEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, JOB_NAME);

        SqlLoader.loadAndExecute(tEnv, params, SQL_RESOURCES);
        LOG.info("{} 已提交到集群", JOB_NAME);
    }

    // ------------------------------------------------------------------
    // 内部辅助方法
    // ------------------------------------------------------------------

    static ParameterTool buildParams(String[] args) throws Exception {
        ParameterTool fileParams;
        try (InputStream in = OrcpFlinkJob.class.getResourceAsStream(JOB_PROPERTIES_RESOURCE)) {
            Objects.requireNonNull(in, JOB_PROPERTIES_RESOURCE + " 在 classpath 上找不到");
            fileParams = ParameterTool.fromPropertiesFile(in);
        }
        // 命令行参数覆盖 job.properties 中的默认值。
        return fileParams.mergeWith(ParameterTool.fromArgs(args));
    }

    static StreamExecutionEnvironment buildEnvironment(ParameterTool params) {
        final Configuration conf = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 检查点配置：EXACTLY_ONCE，60s 间隔，30s 最短暂停，10min 超时。
        // 参数与 DEV_SPEC §5.5 / §6.3 一致。
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cp = env.getCheckpointConfig();
        cp.setMinPauseBetweenCheckpoints(30_000L);
        cp.setCheckpointTimeout(600_000L);
        cp.setMaxConcurrentCheckpoints(1);
        cp.setTolerableCheckpointFailureNumber(3);
        cp.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 重启策略：最多重试 10 次，每次间隔 30 秒。
        // 超出后 JobManager 标记作业为 FAILED，运维人员从 savepoint 恢复。
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(10, Time.seconds(30)));

        // 默认并行度由 flink-conf.yaml 全局设置为 2；
        // 可在提交时通过 --parallelism <n> 覆盖。
        if (params.has("parallelism")) {
            env.setParallelism(params.getInt("parallelism"));
        }

        LOG.info("Flink 环境：parallelism={}，checkpointing=EXACTLY_ONCE@60s，"
                        + "restartStrategy=fixed-delay(10,30s)",
                env.getParallelism());
        return env;
    }
}
