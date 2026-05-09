package com.orcp.common.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * 项目统一的 {@link ObjectMapper} 单例，具备以下特性：
 * <ul>
 *   <li>支持 JSR-310 日期/时间类型（{@code LocalDateTime}、{@code Instant} 等）</li>
 *   <li>时间戳以人类可读格式输出（非 epoch 毫秒）</li>
 *   <li>反序列化时宽松处理未知字段</li>
 *   <li>序列化时抑制 null 字段</li>
 * </ul>
 */
public final class JsonUtils {

    private static final ObjectMapper MAPPER = buildMapper();

    private JsonUtils() {
    }

    /** 返回共享的 {@link ObjectMapper} 实例。 */
    public static ObjectMapper mapper() {
        return MAPPER;
    }

    /** 将对象序列化为 JSON 字符串，失败时抛出 {@link IllegalStateException}。 */
    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            throw new IllegalStateException("序列化对象为 JSON 失败", e);
        }
    }

    /** 将 JSON 字符串反序列化为目标类型，失败时抛出 {@link IllegalStateException}。 */
    public static <T> T fromJson(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalStateException("将 JSON 反序列化为 " + type.getName() + " 失败", e);
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
