package com.orcp.admin.web.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * savepoint 触发 / stop-with-savepoint 请求的响应体。
 * 运维人员通过 {@code GET /api/jobs/{jobId}/savepoints/{requestId}}
 * 轮询直到获得最终 savepoint 路径。
 */
@Data
@AllArgsConstructor
public class SavepointResponse {
    private final String requestId;
}
