package com.orcp.admin.web.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/** jar 上传成功后返回的响应体，包含供后续 /run 调用使用的 jar-id。 */
@Data
@AllArgsConstructor
public class UploadResponse {
    private final String jarId;
}
