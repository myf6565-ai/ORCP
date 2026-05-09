package com.orcp.admin.web.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * submit / run 端点返回的简单响应体，格式为 {@code {"jobId": "..."}}。
 */
@Data
@AllArgsConstructor
public class JobIdResponse {
    private final String jobId;
}
