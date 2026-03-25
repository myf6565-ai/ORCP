package com.orcp.jobs.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orcp.common.model.AggregationResult;
import com.orcp.common.model.StandardEvent;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class RuleAggregationJob {
    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrapServers = params.get("bootstrapServers", "localhost:9092");
        String sourceTopic = params.get("sourceTopic", "standard-events");
        String sinkTopic = params.get("sinkTopic", "result-events");
        String groupId = params.get("groupId", "rule-aggregation-job");
        long threshold = params.getLong("threshold", 3L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(sinkTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        Map<String, Long> state = new HashMap<>();
        DataStream<String> result = env.fromSource(source, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "standard-source")
                .map(raw -> aggregate(raw, state, threshold))
                .filter(v -> v != null);

        result.sinkTo(sink).name("result-sink");
        env.execute("rule-aggregation-job");
    }

    static String aggregate(String raw, Map<String, Long> state, long threshold) {
        try {
            StandardEvent event = MAPPER.readValue(raw, StandardEvent.class);
            long current = state.getOrDefault(event.bizKey(), 0L) + 1;
            state.put(event.bizKey(), current);
            AggregationResult result = new AggregationResult(
                    event.bizKey(),
                    current >= threshold,
                    current,
                    event.eventTime(),
                    Instant.now()
            );
            return MAPPER.writeValueAsString(result);
        } catch (Exception e) {
            return null;
        }
    }
}
