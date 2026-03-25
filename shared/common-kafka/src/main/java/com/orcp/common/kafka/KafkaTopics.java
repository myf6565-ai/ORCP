package com.orcp.common.kafka;

public record KafkaTopics(String rawEvents, String standardEvents, String resultEvents, String deadLetterEvents) {
    public static KafkaTopics from(KafkaTopicProperties props) {
        return new KafkaTopics(props.rawEvents(), props.standardEvents(), props.resultEvents(), props.deadLetterEvents());
    }
}
