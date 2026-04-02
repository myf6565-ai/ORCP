package com.orcp.admin.dto;

import jakarta.validation.constraints.NotBlank;

public record ReplayRequest(
        @NotBlank String topic,
        @NotBlank String fromOffset,
        @NotBlank String toOffset
) {
}
