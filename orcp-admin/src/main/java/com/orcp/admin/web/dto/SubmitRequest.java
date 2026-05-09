package com.orcp.admin.web.dto;

import lombok.Data;

/**
 * Request body for {@code POST /api/jobs/run}: run an already-uploaded jar.
 *
 * <p>For a combined upload+run call in a single request, see
 * {@code POST /api/jobs/submit} in {@link com.orcp.admin.web.JobController}.
 */
@Data
public class SubmitRequest {

    /** Flink jar-id returned by a previous upload. Required. */
    private String jarId;

    /** Entry class. If omitted, the default from application config applies. */
    private String entryClass;

    /** Parallelism override. If null or 0, the default from config applies. */
    private Integer parallelism;

    /** Program args passed verbatim to the job's {@code main}. */
    private String programArgs;

    /** Optional savepoint to resume from. */
    private String savepointPath;
}
