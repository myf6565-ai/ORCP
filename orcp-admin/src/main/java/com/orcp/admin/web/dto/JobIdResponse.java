package com.orcp.admin.web.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Simple {@code {"jobId": "..."}} body returned from submit/run endpoints.
 */
@Data
@AllArgsConstructor
public class JobIdResponse {
    private final String jobId;
}
