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
 * orcp-flink-job entry point.
 *
 * <p>Pipeline: Kafka source ({@code orcp.mid.events}) -> JDBC lookup against
 * the MySQL {@code t_customer} dim -> 1-minute tumbling window aggregate ->
 * JDBC upsert into OceanBase {@code agg_order_1min}.
 *
 * <p>All DDL is externalised under {@code src/main/resources/sql/}; this class
 * only wires the execution environment and hands the SQL to {@link SqlLoader}.
 *
 * <p>Parameters are resolved with this precedence (first win):
 * <ol>
 *   <li>Command-line {@code --key value} pairs.</li>
 *   <li>{@code job.properties} bundled in the jar.</li>
 * </ol>
 * Secrets (DB passwords, Kafka SASL) should always be injected at submit time
 * via the CLI, never baked into the jar.
 */
public final class OrcpFlinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(OrcpFlinkJob.class);

    private static final String JOB_NAME = "orcp-flink-job";
    private static final String JOB_PROPERTIES_RESOURCE = "/job.properties";

    /** Classpath resources executed in order; later files depend on earlier ones. */
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
        LOG.info("Starting {} with {} parameter(s)", JOB_NAME, params.getProperties().size());

        StreamExecutionEnvironment env = buildEnvironment(params);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // Make parameters available to functions/operators running on workers.
        tEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, JOB_NAME);

        SqlLoader.loadAndExecute(tEnv, params, SQL_RESOURCES);
        LOG.info("{} submitted to the cluster", JOB_NAME);
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    static ParameterTool buildParams(String[] args) throws Exception {
        ParameterTool fileParams;
        try (InputStream in = OrcpFlinkJob.class.getResourceAsStream(JOB_PROPERTIES_RESOURCE)) {
            Objects.requireNonNull(in, JOB_PROPERTIES_RESOURCE + " not found on classpath");
            fileParams = ParameterTool.fromPropertiesFile(in);
        }
        // Command-line values override the bundled defaults.
        return fileParams.mergeWith(ParameterTool.fromArgs(args));
    }

    static StreamExecutionEnvironment buildEnvironment(ParameterTool params) {
        final Configuration conf = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // Checkpointing: EXACTLY_ONCE, 60s interval, 30s min-pause, 10m timeout.
        // These numbers match DEV_SPEC §5.5 / §6.3.
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cp = env.getCheckpointConfig();
        cp.setMinPauseBetweenCheckpoints(30_000L);
        cp.setCheckpointTimeout(600_000L);
        cp.setMaxConcurrentCheckpoints(1);
        cp.setTolerableCheckpointFailureNumber(3);
        cp.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Restart strategy: 10 retries at 30s spacing.  Beyond that, the
        // JobManager fails the job and the operator (or admin service) has
        // to restart from a savepoint.
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(10, Time.seconds(30)));

        // Parallelism default.  flink-conf.yaml sets this globally to 2; an
        // operator can override on the submit line via --parallelism <n>.
        if (params.has("parallelism")) {
            env.setParallelism(params.getInt("parallelism"));
        }

        LOG.info("Flink environment: parallelism={}, checkpointing=EXACTLY_ONCE@60s, "
                        + "restartStrategy=fixed-delay(10,30s)",
                env.getParallelism());
        return env;
    }
}
