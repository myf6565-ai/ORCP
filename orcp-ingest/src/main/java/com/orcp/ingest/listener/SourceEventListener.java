package com.orcp.ingest.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orcp.common.dto.SourceEvent;
import com.orcp.ingest.exception.IngestException;
import com.orcp.ingest.service.DedupService;
import com.orcp.ingest.service.DetailWriteService;
import com.orcp.ingest.service.ForwardService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Consumes the external source topic, persists to MySQL, forwards to the
 * mid topic, then acknowledges the Kafka offset.
 *
 * <p>Processing order matters for correctness (DEV_SPEC §6.2):
 * <ol>
 *   <li>Parse + reject malformed JSON (never block the whole topic on one bad message).</li>
 *   <li>Caffeine lookup -- if cached hit, short-circuit and ack without touching the DB.</li>
 *   <li>DB transaction: claim eventId in {@code t_dedup} + insert {@code t_order}.</li>
 *   <li>Sync forward to the mid topic.  If this throws, we do NOT ack; Kafka will redeliver.</li>
 *   <li>{@code markSeen} and {@code ack}.</li>
 * </ol>
 *
 * <p>{@link #handle} only ever throws {@link IngestException} for transient
 * infrastructure problems that warrant redelivery.  Permanent errors (JSON
 * parse failure) are logged, acked, and dropped -- a single poison record
 * must not halt the pipeline.
 */
@Slf4j
@Component
public class SourceEventListener {

    private final ObjectMapper objectMapper;
    private final DedupService dedupService;
    private final DetailWriteService detailWriteService;
    private final ForwardService forwardService;

    private final Counter receivedCounter;
    private final Counter duplicateCounter;
    private final Counter parseErrorCounter;
    private final Counter persistedCounter;
    private final Timer processingTimer;

    public SourceEventListener(ObjectMapper objectMapper,
                               DedupService dedupService,
                               DetailWriteService detailWriteService,
                               ForwardService forwardService,
                               MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.dedupService = dedupService;
        this.detailWriteService = detailWriteService;
        this.forwardService = forwardService;

        this.receivedCounter   = Counter.builder("orcp.ingest.received").register(meterRegistry);
        this.duplicateCounter  = Counter.builder("orcp.ingest.duplicate").register(meterRegistry);
        this.parseErrorCounter = Counter.builder("orcp.ingest.parseError").register(meterRegistry);
        this.persistedCounter  = Counter.builder("orcp.ingest.persisted").register(meterRegistry);
        this.processingTimer   = Timer.builder("orcp.ingest.processingTime")
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    @PostConstruct
    void logStartup() {
        log.info("SourceEventListener ready");
    }

    @KafkaListener(
            topics = "${orcp.kafka.src-topic}",
            groupId = "${spring.kafka.consumer.group-id:orcp-ingest}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handle(ConsumerRecord<String, String> record, Acknowledgment ack) {
        receivedCounter.increment();
        final long t0 = System.nanoTime();

        SourceEvent event;
        try {
            event = objectMapper.readValue(record.value(), SourceEvent.class);
        } catch (Exception e) {
            // Poison record: log, ack, drop.  We deliberately do NOT rethrow
            // -- if we did, Kafka would redeliver forever.
            parseErrorCounter.increment();
            log.warn("dropping malformed record at {}-{}@{}: {}",
                    record.topic(), record.partition(), record.offset(),
                    abbreviate(record.value()));
            ack.acknowledge();
            return;
        }

        if (event.getEventId() == null || event.getEventId().isEmpty()) {
            parseErrorCounter.increment();
            log.warn("dropping record without eventId at {}-{}@{}",
                    record.topic(), record.partition(), record.offset());
            ack.acknowledge();
            return;
        }

        // Fast-path dedup: if Caffeine has seen it recently, skip everything.
        if (dedupService.isCachedHit(event.getEventId())) {
            duplicateCounter.increment();
            log.debug("caffeine dedup hit: {}", event.getEventId());
            ack.acknowledge();
            recordTime(t0);
            return;
        }

        try {
            boolean claimed = detailWriteService.tryClaimAndPersist(event);
            if (!claimed) {
                duplicateCounter.increment();
                dedupService.markSeen(event.getEventId()); // cache the negative result too
                ack.acknowledge();
                recordTime(t0);
                return;
            }

            forwardService.forward(event);

            dedupService.markSeen(event.getEventId());
            persistedCounter.increment();
            ack.acknowledge();
        } catch (IngestException e) {
            // Transient: don't ack, let the container redeliver after a
            // back-off.  Dedup makes the replay idempotent.
            log.error("retriable failure for eventId={}: {}", event.getEventId(), e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            log.error("unexpected failure for eventId={}: {}", event.getEventId(), e.getMessage(), e);
            throw new IngestException("unexpected failure", e);
        } finally {
            recordTime(t0);
        }
    }

    private void recordTime(long startNanos) {
        processingTimer.record(System.nanoTime() - startNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
    }

    private static String abbreviate(String s) {
        if (s == null) return "<null>";
        return s.length() <= 256 ? s : s.substring(0, 256) + "...(" + s.length() + " chars)";
    }
}
