package com.orcp.admin.flink;

/**
 * 当 JobManager 返回非 2xx 状态码或请求在 I/O 层失败时，由
 * {@link FlinkRestClient} 抛出。
 * 携带 HTTP 状态码，供 Web 控制器翻译为 502（上游拒绝）或 504（I/O/超时）。
 */
public class FlinkRestException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String operation;
    private final int statusCode;

    public FlinkRestException(String operation, int statusCode, String message) {
        super(operation + " -> " + statusCode + ": " + message);
        this.operation = operation;
        this.statusCode = statusCode;
    }

    public String getOperation() {
        return operation;
    }

    /** {@code 0} 表示 I/O 层失败（连接拒绝、超时等）。 */
    public int getStatusCode() {
        return statusCode;
    }
}
