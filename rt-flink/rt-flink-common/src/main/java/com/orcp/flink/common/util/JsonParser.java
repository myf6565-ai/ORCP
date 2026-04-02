package com.orcp.flink.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orcp.flink.common.model.BizEvent;

public final class JsonParser {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonParser() {
    }

    public static BizEvent parse(String json) {
        try {
            return MAPPER.readValue(json, BizEvent.class);
        } catch (Exception e) {
            return null;
        }
    }
}
