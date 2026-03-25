package com.orcp.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record StandardEvent(
        String eventId,
        String bizKey,
        String eventType,
        Instant eventTime,
        Instant ingestTime,
        String source,
        Map<String, Object> payload
) {
}
