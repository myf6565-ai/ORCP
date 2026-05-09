package com.orcp.ingest.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orcp.common.dto.SourceEvent;
import com.orcp.ingest.exception.IngestException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 将标准化事件发布到 {@code orcp.mid.events}，供 Flink 作业消费。
 *
 * <p>mid-topic 的 wire schema 与源 DTO 刻意不同：
 * 阶段 E 的 Flink SQL source DDL 期望 snake_case 字段名和带时区的 ISO-8601 时间戳
 * （TIMESTAMP_LTZ(3)）。在此处显式做转换，可避免将 Java DTO 与流式 wire 格式耦合。
 *
 * <p>采用同步发送（阻塞等待 broker ack），确保 listener 只在 mid 消息持久化后
 * 才提交源 offset。此处失败会向上冒泡，触发源消息重投——
 * 去重层使重投成为空操作，端到端安全。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ForwardService {

    /** ISO-8601 UTC 毫秒格式，与 Flink TIMESTAMP_LTZ(3) 解析格式对齐。 */
    private static final DateTimeFormatter ISO_MILLIS_UTC = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(ZoneId.of("UTC"));

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${orcp.kafka.mid-topic:orcp.mid.events}")
    private String midTopic;

    @Value("${orcp.kafka.send-timeout-seconds:10}")
    private long sendTimeoutSeconds;

    /**
     * 将事件发送到 mid topic 并阻塞直到 broker 确认。
     *
     * <p>分区键为 {@code bizKey}，确保同一订单的所有事件落在同一分区，
     * 保证下游消费者的按键有序性。
     */
    public void forward(SourceEvent event) {
        final String key = event.getBizKey();
        final String payload = serialiseForMidTopic(event);

        try {
            SendResult<String, String> result = kafkaTemplate
                    .send(midTopic, key, payload)
                    .get(sendTimeoutSeconds, TimeUnit.SECONDS);
            if (log.isDebugEnabled()) {
                log.debug("已转发 eventId={} -> {}-{}@{}",
                        event.getEventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IngestException("发送到 " + midTopic + " 时被中断", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IngestException(
                    "转发 eventId=" + event.getEventId() + " 到 " + midTopic + " 失败", e);
        }
    }

    /**
     * 构建 mid topic 消费端（阶段 E Flink Kafka source DDL）期望的 wire payload。
     *
     * <p>契约（必须与 {@code orcp-flink-job/src/main/resources/sql/01_source_kafka.sql} 保持一致）：
     * <pre>
     * {
     *   "event_id":    "...",
     *   "biz_type":    "ORDER",
     *   "biz_key":     "order-0000000001",
     *   "customer_id": 42,
     *   "amount":      "128.3200",
     *   "event_time":  "2026-05-09T10:00:00.000Z"
     * }
     * </pre>
     */
    String serialiseForMidTopic(SourceEvent event) {
        Map<String, Object> wire = new LinkedHashMap<>();
        wire.put("event_id", event.getEventId());
        wire.put("biz_type", event.getBizType());
        wire.put("biz_key", event.getBizKey());
        wire.put("customer_id", event.getCustomerId());
        // amount 以字符串形式传输，保留 DECIMAL(18,4) 精度，避免 JSON number 精度损失。
        wire.put("amount",
                event.getAmount() != null ? event.getAmount().toPlainString() : null);
        wire.put("event_time", formatEventTime(event));
        try {
            return objectMapper.writeValueAsString(wire);
        } catch (Exception e) {
            throw new IngestException("序列化 eventId=" + event.getEventId() + " 失败", e);
        }
    }

    /**
     * 将 eventTime（Asia/Shanghai 本地时间）转换为 ISO-8601 UTC 字符串，
     * 例如 2026-05-09T02:00:00.000Z。
     */
    private static String formatEventTime(SourceEvent event) {
        if (event.getEventTime() == null) {
            return ISO_MILLIS_UTC.format(Instant.now());
        }
        Instant instant = event.getEventTime()
                .atZone(ZoneId.of("Asia/Shanghai"))
                .toInstant();
        return ISO_MILLIS_UTC.format(instant);
    }
}
