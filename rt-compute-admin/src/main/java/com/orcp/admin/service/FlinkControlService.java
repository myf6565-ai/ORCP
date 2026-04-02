package com.orcp.admin.service;

import com.orcp.admin.dto.SubmitJobRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class FlinkControlService {

    @Value("${flink.rest.url:http://localhost:8081}")
    private String flinkRestUrl;

    private final Map<String, String> jobState = new ConcurrentHashMap<>();

    public Map<String, String> submit(SubmitJobRequest request) {
        String jobId = UUID.randomUUID().toString();
        jobState.put(jobId, "RUNNING");
        return Map.of("jobId", jobId, "state", "RUNNING", "flinkRest", flinkRestUrl, "jobName", request.jobName());
    }

    public Map<String, String> stop(String jobId) {
        jobState.put(jobId, "STOPPED");
        return Map.of("jobId", jobId, "state", "STOPPED");
    }

    public Map<String, String> savepoint(String jobId) {
        String path = "file:///tmp/savepoints/" + jobId;
        return Map.of("jobId", jobId, "savepointPath", path);
    }

    public Map<String, String> status(String jobId) {
        return Map.of("jobId", jobId, "state", jobState.getOrDefault(jobId, "UNKNOWN"));
    }

    public Map<String, String> replay(String topic, String from, String to) {
        return Map.of("topic", topic, "fromOffset", from, "toOffset", to, "result", "REPLAY_ACCEPTED");
    }
}
