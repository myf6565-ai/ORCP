package com.orcp.flink.ods;

import com.orcp.flink.common.model.BizEvent;
import com.orcp.flink.common.sink.OceanBaseUpsertStatement;
import com.orcp.flink.common.util.JsonParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OdsSyncJob {

    private static final OutputTag<String> DEAD_LETTER = new OutputTag<>("dead-letter") {};

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);

        String brokers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String sourceTopic = System.getenv().getOrDefault("KAFKA_SOURCE_TOPIC", "rt.biz.events");
        String dlqTopic = System.getenv().getOrDefault("KAFKA_DLQ_TOPIC", "rt.biz.events.dlq");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(sourceTopic)
                .setGroupId(System.getenv().getOrDefault("KAFKA_GROUP", "rt-ods-sync"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<BizEvent> validEvents = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .uid("uid-kafka-source")
                .map((MapFunction<String, BizEvent>) JsonParser::parse)
                .uid("uid-json-parser")
                .keyBy(v -> v == null ? "NULL" : v.dedupKey())
                .process(new ValidateAndDedup())
                .uid("uid-validate-dedup");

        KafkaSink<String> deadLetterSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(dlqTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        validEvents.getSideOutput(DEAD_LETTER).sinkTo(deadLetterSink).uid("uid-dlq-sink");

        String jdbcUrl = System.getenv().getOrDefault("OB_JDBC_URL", "jdbc:mysql://localhost:2881/rt_ods");
        String jdbcUser = System.getenv().getOrDefault("OB_USER", "root@test");
        String jdbcPwd = System.getenv().getOrDefault("OB_PASSWORD", "");
        String table = System.getenv().getOrDefault("OB_ODS_TABLE", "ods_event_detail");

        validEvents.addSink(JdbcSink.sink(
                OceanBaseUpsertStatement.odsUpsertSql(table),
                (ps, e) -> {
                    ps.setString(1, e.eventId);
                    ps.setString(2, e.eventTime);
                    ps.setString(3, e.bizKey);
                    ps.setInt(4, e.schemaVersion == null ? 1 : e.schemaVersion);
                    ps.setString(5, e.traceId);
                    ps.setDouble(6, e.amount == null ? 0D : e.amount);
                    ps.setString(7, e.version);
                },
                JdbcExecutionOptions.builder().withBatchSize(200).withBatchIntervalMs(2000).withMaxRetries(3).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(jdbcUrl).withDriverName("com.mysql.cj.jdbc.Driver").withUsername(jdbcUser).withPassword(jdbcPwd).build()
        )).uid("uid-ods-upsert-sink").name("ods-upsert-sink");

        env.execute("rt-flink-ods-sync-job");
    }

    public static class ValidateAndDedup extends KeyedProcessFunction<String, BizEvent, BizEvent> {
        private transient MapState<String, Boolean> dedupState;

        @Override
        public void open(Configuration parameters) {
            dedupState = getRuntimeContext().getMapState(new MapStateDescriptor<>("dedup", String.class, Boolean.class));
        }

        @Override
        public void processElement(BizEvent value, Context ctx, Collector<BizEvent> out) throws Exception {
            if (value == null || !value.isValid()) {
                ctx.output(DEAD_LETTER, "INVALID_EVENT");
                return;
            }
            String dedupKey = value.dedupKey();
            if (!dedupState.contains(dedupKey)) {
                dedupState.put(dedupKey, true);
                out.collect(value);
            }
        }
    }
}
