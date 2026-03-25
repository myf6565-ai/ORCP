package com.orcp.common.kafka;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(KafkaTopicProperties.class)
public class KafkaCommonAutoConfiguration {

    @Bean
    public KafkaTopics kafkaTopics(KafkaTopicProperties properties) {
        return KafkaTopics.from(properties);
    }
}
