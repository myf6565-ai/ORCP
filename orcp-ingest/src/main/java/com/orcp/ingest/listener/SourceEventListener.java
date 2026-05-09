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
 * 消费外部源 topic，持久化到 MySQL，转发到 mid topic，最后提交 Kafka offset。
 *
 * <p>处理顺序（保证正确性）：
 * <ol>
 *   <li>解析并拒绝畸形 JSON（绝不因单条坏消息阻塞整个 topic）。</li>
 *   <li>Caffeine 查询——命中则短路，不访问 DB 直接 ack。</li>
 *   <li>DB 事务：在 {@code t_dedup} 声明 eventId 所有权 + 写入 {@code t_order}。</li>
 *   <li>同步转发到 mid topic。如果失败则 <strong>不 ack</strong>；Kafka 会重投消息。</li>
 *   <li>{@code markSeen} 更新 Caffeine 缓存，然后 {@code ack}。</li>
 * </ol>
 *
 * <p>{@link #handle} 只会抛出 {@link IngestException} 表示需要重投的瞬时错误。
 * 永久性错误（JSON 解析失败）只记录日志后 ack 并丢弃——
 * 单条毒药消息不得阻塞流水线。
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
        log.info("SourceEventListener 已就绪");
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
            // 毒药消息：记录日志、ack、丢弃。不重新抛出——否则 Kafka 会永久重投。
            parseErrorCounter.increment();
            log.warn("丢弃畸形消息 {}-{}@{}：{}",
                    record.topic(), record.partition(), record.offset(),
                    abbreviate(record.value()));
            ack.acknowledge();
            return;
        }

        if (event.getEventId() == null || event.getEventId().isEmpty()) {
            parseErrorCounter.increment();
            log.warn("丢弃缺少 eventId 的消息 {}-{}@{}",
                    record.topic(), record.partition(), record.offset());
            ack.acknowledge();
            return;
        }

        // 快速路径去重：Caffeine 近期已见过则跳过所有处理。
        if (dedupService.isCachedHit(event.getEventId())) {
            duplicateCounter.increment();
            log.debug("Caffeine 去重命中：{}", event.getEventId());
            ack.acknowledge();
            recordTime(t0);
            return;
        }

        try {
            boolean claimed = detailWriteService.tryClaimAndPersist(event);
            if (!claimed) {
                duplicateCounter.increment();
                dedupService.markSeen(event.getEventId()); // 缓存负向结果，减少后续 DB 访问
                ack.acknowledge();
                recordTime(t0);
                return;
            }

            forwardService.forward(event);

            dedupService.markSeen(event.getEventId());
            persistedCounter.increment();
            ack.acknowledge();
        } catch (IngestException e) {
            // 瞬时故障：不 ack，让容器在退避后重投。去重使重投幂等。
            log.error("eventId={} 处理失败（可重试）：{}", event.getEventId(), e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            log.error("eventId={} 发生非预期异常：{}", event.getEventId(), e.getMessage(), e);
            throw new IngestException("非预期异常", e);
        } finally {
            recordTime(t0);
        }
    }

    private void recordTime(long startNanos) {
        processingTimer.record(System.nanoTime() - startNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
    }

    private static String abbreviate(String s) {
        if (s == null) return "<null>";
        return s.length() <= 256 ? s : s.substring(0, 256) + "...（共 " + s.length() + " 字符）";
    }
}
