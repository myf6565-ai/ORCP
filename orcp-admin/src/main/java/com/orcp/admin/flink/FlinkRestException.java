package com.orcp.admin.flink;

/**
 * Raised by {@link FlinkRestClient} whenever the JobManager returns a
 * non-2xx status or the request fails at the I/O layer.  Carries the HTTP
 * status code so the web controller can translate to 502 (upstream rejected)
 * or 504 (I/O / timeout) as appropriate.
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

    /** {@code 0} indicates an I/O layer failure (connect refused, timeout, etc.). */
    public int getStatusCode() {
        return statusCode;
    }
}
