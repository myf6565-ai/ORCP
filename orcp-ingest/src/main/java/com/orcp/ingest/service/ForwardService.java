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
 * Publishes normalised events to {@code orcp.mid.events} for Flink to
 * consume.
 *
 * <p>The mid-topic schema is intentionally NOT the same as the source DTO:
 * Stage E's Flink SQL source DDL expects snake_case column names and an
 * ISO-8601 timestamp with timezone (TIMESTAMP_LTZ(3)).  Keeping the
 * translation here is explicit and avoids coupling the Java DTO to the
 * streaming wire format.
 *
 * <p>We publish synchronously (block on broker ack) so the listener can
 * only acknowledge the source offset after the mid record is durably
 * stored.  A failure here bubbles up and triggers source redelivery --
 * safe because dedup makes the replay a no-op.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ForwardService {

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
     * Send the event to the mid topic and block until the broker acks it.
     *
     * <p>The partition key is {@code bizKey} so all events for one order
     * land on the same partition, which preserves per-key ordering for
     * downstream consumers.
     */
    public void forward(SourceEvent event) {
        final String key = event.getBizKey();
        final String payload = serialiseForMidTopic(event);

        try {
            SendResult<String, String> result = kafkaTemplate
                    .send(midTopic, key, payload)
                    .get(sendTimeoutSeconds, TimeUnit.SECONDS);
            if (log.isDebugEnabled()) {
                log.debug("forwarded eventId={} -> {}-{}@{}",
                        event.getEventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IngestException("Interrupted while sending to " + midTopic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IngestException(
                    "Failed to forward eventId=" + event.getEventId() + " to " + midTopic, e);
        }
    }

    /**
     * Build the wire payload consumed by Stage E's Flink Kafka source DDL.
     *
     * <p>Contract (must stay in lockstep with {@code orcp-flink-job/src/main/resources/sql/01_source_kafka.sql}):
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
        // Keep the amount as a string so downstream parsers preserve the
        // exact DECIMAL(18,4) scale without JSON-number precision loss.
        wire.put("amount",
                event.getAmount() != null ? event.getAmount().toPlainString() : null);
        wire.put("event_time", formatEventTime(event));
        try {
            return objectMapper.writeValueAsString(wire);
        } catch (Exception e) {
            throw new IngestException("Cannot serialise eventId=" + event.getEventId(), e);
        }
    }

    /** Renders eventTime (Asia/Shanghai local) as ISO-8601 UTC, e.g. 2026-05-09T02:00:00.000Z. */
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
