package com.orcp.ingest.exception;

/**
 * Unchecked failure during ingest processing (parse / persist / forward).
 *
 * <p>Throwing this from inside the Kafka listener triggers Spring-Kafka's
 * retry + DLT path (wherever Stage F decides to wire it).  For Stage D we
 * simply let it bubble up, which causes the container to redeliver the
 * message -- safe because dedup is idempotent.
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
