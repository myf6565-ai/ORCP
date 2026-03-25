package com.orcp.jobs.cleaning;

import com.fasterxml.jackson.databind.ObjectMapper;
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

public class EventCleaningJob {
    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrapServers = params.get("bootstrapServers", "localhost:9092");
        String sourceTopic = params.get("sourceTopic", "raw-events");
        String sinkTopic = params.get("sinkTopic", "standard-events");
        String groupId = params.get("groupId", "event-cleaning-job");
        int parallelism = params.getInt("parallelism", 1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

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

        DataStream<String> cleaned = env.fromSource(source, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "raw-source")
                .map(EventCleaningJob::clean)
                .filter(v -> v != null);

        cleaned.sinkTo(sink).name("standard-sink");
        env.execute("event-cleaning-job");
    }

    static String clean(String raw) {
        try {
            StandardEvent event = MAPPER.readValue(raw, StandardEvent.class);
            if (event.bizKey() == null || event.bizKey().isBlank()) {
                return null;
            }
            StandardEvent cleaned = new StandardEvent(
                    event.eventId(),
                    event.bizKey().trim(),
                    event.eventType() == null ? "UNKNOWN" : event.eventType().trim().toUpperCase(),
                    event.eventTime() == null ? Instant.now() : event.eventTime(),
                    event.ingestTime() == null ? Instant.now() : event.ingestTime(),
                    event.source() == null ? "event-cleaning-job" : event.source(),
                    event.payload()
            );
            return MAPPER.writeValueAsString(cleaned);
        } catch (Exception e) {
            return null;
        }
    }
}
