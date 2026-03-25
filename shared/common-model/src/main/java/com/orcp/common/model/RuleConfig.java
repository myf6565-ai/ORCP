package com.orcp.common.model;

public record RuleConfig(
        String ruleId,
        String eventType,
        long threshold,
        String description
) {
}
