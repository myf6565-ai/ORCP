package com.orcp.jobs.cleaning;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EventCleaningJobTest {

    @Test
    void shouldNormalizeEventType() {
        String raw = """
                {"eventId":"e1","bizKey":"u1","eventType":"order_created","eventTime":"2026-03-25T10:00:00Z","ingestTime":"2026-03-25T10:00:01Z","source":"ingest-service","payload":{"a":1}}
                """;
        String cleaned = EventCleaningJob.clean(raw);
        assertNotNull(cleaned);
        assertTrue(cleaned.contains("ORDER_CREATED"));
    }
}
