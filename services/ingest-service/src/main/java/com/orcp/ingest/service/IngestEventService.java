package com.orcp.ingest.service;

import com.orcp.common.kafka.KafkaTopics;
import com.orcp.common.kafka.producer.EventProducer;
import com.orcp.common.model.EventRequest;
import com.orcp.common.model.StandardEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class IngestEventService {
    private static final Logger log = LoggerFactory.getLogger(IngestEventService.class);

    private final EventProducer eventProducer;
    private final KafkaTopics kafkaTopics;

    public IngestEventService(EventProducer eventProducer, KafkaTopics kafkaTopics) {
        this.eventProducer = eventProducer;
        this.kafkaTopics = kafkaTopics;
    }

    public void ingest(EventRequest request) {
        StandardEvent standardEvent = new StandardEvent(
                request.eventId(),
                request.bizKey(),
                request.eventType(),
                request.eventTime(),
                Instant.now(),
                "ingest-service",
                request.payload()
        );

        eventProducer.send(kafkaTopics.rawEvents(), request.bizKey(), standardEvent);
        log.info("接入事件成功 eventId={}, bizKey={}", request.eventId(), request.bizKey());
    }
}
