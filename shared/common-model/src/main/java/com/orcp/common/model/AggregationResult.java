package com.orcp.common.model;

import java.time.Instant;

public record AggregationResult(
        String bizKey,
        boolean ruleMatched,
        long count,
        Instant lastEventTime,
        Instant updatedAt
) {
}
