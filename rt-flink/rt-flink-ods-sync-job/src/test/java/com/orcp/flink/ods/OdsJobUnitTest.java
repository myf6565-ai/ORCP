package com.orcp.flink.ods;

import com.orcp.flink.common.model.BizEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OdsJobUnitTest {

    @Test
    void validEventShouldPass() {
        BizEvent event = new BizEvent();
        event.eventId = "evt-1";
        event.bizKey = "bk-1";
        event.eventTime = "2026-01-01T00:00:00Z";
        Assertions.assertTrue(event.isValid());
    }
}
