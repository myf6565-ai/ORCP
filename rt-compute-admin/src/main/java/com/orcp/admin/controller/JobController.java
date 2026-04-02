package com.orcp.admin.controller;

import com.orcp.admin.dto.ReplayRequest;
import com.orcp.admin.dto.SubmitJobRequest;
import com.orcp.admin.service.FlinkControlService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class JobController {

    private final FlinkControlService flinkControlService;

    public JobController(FlinkControlService flinkControlService) {
        this.flinkControlService = flinkControlService;
    }

    @PostMapping("/jobs/submit")
    public Map<String, String> submit(@RequestBody @Valid SubmitJobRequest req) {
        return flinkControlService.submit(req);
    }

    @PostMapping("/jobs/{jobId}/stop")
    public Map<String, String> stop(@PathVariable String jobId) {
        return flinkControlService.stop(jobId);
    }

    @PostMapping("/jobs/{jobId}/savepoint")
    public Map<String, String> savepoint(@PathVariable String jobId) {
        return flinkControlService.savepoint(jobId);
    }

    @GetMapping("/jobs/{jobId}")
    public Map<String, String> status(@PathVariable String jobId) {
        return flinkControlService.status(jobId);
    }

    @PostMapping("/replay")
    public Map<String, String> replay(@RequestBody @Valid ReplayRequest req) {
        return flinkControlService.replay(req.topic(), req.fromOffset(), req.toOffset());
    }
}
