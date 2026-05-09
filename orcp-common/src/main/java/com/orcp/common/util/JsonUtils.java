package com.orcp.common.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Single project-wide {@link ObjectMapper} with:
 * <ul>
 *   <li>JSR-310 date/time types ({@code LocalDateTime}, {@code Instant}, ...)</li>
 *   <li>Human-readable timestamps (no epoch millis)</li>
 *   <li>Lenient deserialisation against unknown fields</li>
 *   <li>Null fields suppressed on serialisation</li>
 * </ul>
 */
public final class JsonUtils {

    private static final ObjectMapper MAPPER = buildMapper();

    private JsonUtils() {
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize object to JSON", e);
        }
    }

    public static <T> T fromJson(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize JSON to " + type.getName(), e);
        }
    }

    private static ObjectMapper buildMapper() {
        ObjectMapper m = new ObjectMapper();
        m.registerModule(new JavaTimeModule());
        m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        m.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        m.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return m;
    }
}
