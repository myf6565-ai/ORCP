package com.orcp.admin.web;

import com.orcp.admin.flink.FlinkRestException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 将原始异常转换为统一的 JSON 错误响应体。
 *
 * <p>状态码映射规则：
 * <ul>
 *   <li>{@link IllegalArgumentException} -> 400（客户端参数错误）。</li>
 *   <li>{@link FlinkRestException} 携带 4xx 上游状态码 -> 502（上游拒绝请求）。</li>
 *   <li>{@link FlinkRestException} 状态码为 0（I/O/超时）-> 504（网关超时）。</li>
 *   <li>其他所有异常 -> 500，响应体中不暴露堆栈信息。</li>
 * </ul>
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException e) {
        return respond(HttpStatus.BAD_REQUEST, "bad_request", e.getMessage(), null);
    }

    @ExceptionHandler(FlinkRestException.class)
    public ResponseEntity<Map<String, Object>> flinkUpstream(FlinkRestException e) {
        log.warn("Flink REST 调用失败：{}", e.getMessage());
        HttpStatus status = e.getStatusCode() == 0
                ? HttpStatus.GATEWAY_TIMEOUT
                : HttpStatus.BAD_GATEWAY;
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("operation", e.getOperation());
        extra.put("upstreamStatus", e.getStatusCode());
        return respond(status, "flink_upstream", e.getMessage(), extra);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> generic(Exception e) {
        log.error("未处理的异常：{}", e.getMessage(), e);
        return respond(HttpStatus.INTERNAL_SERVER_ERROR, "internal_error",
                e.getClass().getSimpleName() + ": " + e.getMessage(), null);
    }

    private static ResponseEntity<Map<String, Object>> respond(HttpStatus status,
                                                               String code,
                                                               String message,
                                                               Map<String, Object> extra) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status.value());
        body.put("error", code);
        body.put("message", message);
        if (extra != null) {
            body.putAll(extra);
        }
        return ResponseEntity.status(status).body(body);
    }
}
