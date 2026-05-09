package com.orcp.ingest.exception;

/**
 * 摄入处理过程中（解析 / 持久化 / 转发）发生的非受检异常。
 *
 * <p>从 Kafka listener 内部抛出此异常，会触发 Spring-Kafka 的重试 + DLT 路径
 * （Stage F 中可按需配置）。当前阶段仅让异常向上冒泡，容器会重投消息——
 * 去重机制保证这是安全的幂等重放。
 */
public class IngestException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IngestException(String message) {
        super(message);
    }

    public IngestException(String message, Throwable cause) {
        super(message, cause);
    }
}
