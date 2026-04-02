package com.orcp.flink.common;

import com.orcp.flink.common.model.BizEvent;
import com.orcp.flink.common.util.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JsonParserTest {

    @Test
    void parseSuccess() {
        BizEvent e = JsonParser.parse("{\"eventId\":\"e1\",\"eventTime\":\"2026-01-01T00:00:00Z\",\"bizKey\":\"b1\",\"schemaVersion\":1,\"traceId\":\"t1\"}");
        Assertions.assertNotNull(e);
        Assertions.assertTrue(e.isValid());
    }
}
