package com.orcp.admin.web.dto;

import lombok.Data;

/**
 * {@code POST /api/jobs/run} 的请求体：运行一个已上传的 jar。
 *
 * <p>若需要上传与运行合并在同一请求中，请使用
 * {@link com.orcp.admin.web.JobController} 中的 {@code POST /api/jobs/submit}。
 */
@Data
public class SubmitRequest {

    /** 通过上一步上传获得的 Flink jar-id。必填。 */
    private String jarId;

    /** 入口类。如省略，则使用配置中的默认值。 */
    private String entryClass;

    /** 并行度覆盖值。null 或 0 时使用配置中的默认值。 */
    private Integer parallelism;

    /** 原样传递给作业 {@code main} 方法的程序参数。 */
    private String programArgs;

    /** 可选的 savepoint 恢复路径。 */
    private String savepointPath;
}
