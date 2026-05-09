package com.orcp.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * orcp-flink-job entry point.
 *
 * <p>Skeleton only. The real pipeline (Kafka source + MySQL dim lookup
 * + windowed aggregation + OceanBase sink) is implemented in Stage E,
 * driven by external SQL files under {@code src/main/resources/sql/}.
 *
 * <p>Keeping a compilable shell here lets us exercise the shade build
 * and the JDK 8 version matrix from day one.
 */
public final class OrcpFlinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(OrcpFlinkJob.class);

    private OrcpFlinkJob() {
    }

    public static void main(String[] args) throws Exception {
        LOG.info("orcp-flink-job skeleton started (Stage A placeholder). args={}", java.util.Arrays.toString(args));
        // Stage E will replace this with:
        //   - ParameterTool from job.properties + args
        //   - StreamExecutionEnvironment with checkpointing
        //   - SqlLoader.loadAndExecute(tEnv, p, "/sql/01_source_kafka.sql", ...)
    }
}
