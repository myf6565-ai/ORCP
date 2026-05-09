package com.orcp.admin.web.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Returned from savepoint / stop-with-savepoint triggers.  The operator
 * polls {@code GET /api/jobs/{jobId}/savepoints/{requestId}} for the
 * eventual location.
 */
@Data
@AllArgsConstructor
public class SavepointResponse {
    private final String requestId;
}
