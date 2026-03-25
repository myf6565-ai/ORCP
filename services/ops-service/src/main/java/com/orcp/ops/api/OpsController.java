package com.orcp.ops.api;

import com.orcp.common.web.ApiResponse;
import com.orcp.ops.model.JobStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/ops")
public class OpsController {

    @GetMapping("/jobs")
    public ApiResponse<List<JobStatus>> jobs() {
        return ApiResponse.success(List.of(
                new JobStatus("event-cleaning-job", "RUNNING", "清洗作业运行中"),
                new JobStatus("rule-aggregation-job", "RUNNING", "聚合作业运行中")
        ));
    }

    @GetMapping("/audit")
    public ApiResponse<Map<String, String>> audit() {
        return ApiResponse.success(Map.of("message", "审计能力 MVP 占位"));
    }
}
