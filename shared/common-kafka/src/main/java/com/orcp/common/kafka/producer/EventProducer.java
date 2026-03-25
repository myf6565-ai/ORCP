package com.orcp.common.kafka.producer;

public interface EventProducer {
    void send(String topic, String key, Object payload);
}
