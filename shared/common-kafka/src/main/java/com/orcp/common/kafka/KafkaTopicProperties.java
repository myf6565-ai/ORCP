package com.orcp.common.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "orcp.kafka.topics")
public record KafkaTopicProperties(
        String rawEvents,
        String standardEvents,
        String resultEvents,
        String deadLetterEvents
) {
}
