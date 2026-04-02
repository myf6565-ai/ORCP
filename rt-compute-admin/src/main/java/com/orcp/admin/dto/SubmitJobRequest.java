package com.orcp.admin.dto;

import jakarta.validation.constraints.NotBlank;

public record SubmitJobRequest(
        @NotBlank String jobJar,
        @NotBlank String jobName,
        String[] args
) {
}
