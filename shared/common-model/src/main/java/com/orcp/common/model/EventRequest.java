package com.orcp.common.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.Map;

public record EventRequest(
        @NotBlank String eventId,
        @NotBlank String bizKey,
        @NotBlank String eventType,
        @NotNull Instant eventTime,
        Map<String, Object> payload,
        String idempotencyKey
) {
}
