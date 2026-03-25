package com.orcp.jobs.aggregation;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class RuleAggregationJobTest {

    @Test
    void shouldCountAndMatchRule() {
        String event = """
                {"eventId":"e1","bizKey":"u1","eventType":"ORDER_CREATED","eventTime":"2026-03-25T10:00:00Z","ingestTime":"2026-03-25T10:00:01Z","source":"cleaning","payload":{"a":1}}
                """;
        HashMap<String, Long> state = new HashMap<>();
        RuleAggregationJob.aggregate(event, state, 2L);
        String result = RuleAggregationJob.aggregate(event, state, 2L);
        assertNotNull(result);
        assertTrue(result.contains("\"ruleMatched\":true"));
    }
}
