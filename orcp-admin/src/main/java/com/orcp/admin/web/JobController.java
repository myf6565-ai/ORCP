package com.orcp.admin.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.orcp.admin.config.AdminProperties;
import com.orcp.admin.flink.FlinkRestClient;
import com.orcp.admin.web.dto.JobIdResponse;
import com.orcp.admin.web.dto.SavepointResponse;
import com.orcp.admin.web.dto.SubmitRequest;
import com.orcp.admin.web.dto.UploadResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.constraints.NotBlank;
import java.io.IOException;

/**
 * Operator-facing REST surface for the Flink control plane.
 *
 * <p>All endpoints require Basic auth (see {@link com.orcp.admin.config.SecurityConfig}).
 *
 * <p>Two submit flavours:
 * <ul>
 *   <li>{@code POST /api/jobs/submit} -- multipart upload + run in one call.
 *       Mirrors {@code scripts/submit_job.sh}; use for ad-hoc ops.</li>
 *   <li>{@code POST /api/jobs/upload} + {@code POST /api/jobs/run} -- two
 *       steps when you want to run the same jar with different params.</li>
 * </ul>
 */
@Slf4j
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final FlinkRestClient flink;
    private final AdminProperties properties;

    // ------------------------------------------------------------------
    // Upload + run
    // ------------------------------------------------------------------

    @PostMapping(path = "/upload", consumes = "multipart/form-data")
    public UploadResponse upload(@RequestPart("jar") MultipartFile jar) throws IOException {
        requireNonEmpty(jar);
        String jarId = flink.uploadJar(jar);
        return new UploadResponse(jarId);
    }

    @PostMapping("/run")
    public JobIdResponse run(@RequestBody SubmitRequest req) {
        validateSubmit(req, true);
        String jobId = flink.runJar(
                req.getJarId(),
                firstNonEmpty(req.getEntryClass(), properties.getJob().getDefaultEntryClass()),
                req.getParallelism() != null && req.getParallelism() > 0
                        ? req.getParallelism() : properties.getJob().getDefaultParallelism(),
                req.getProgramArgs(),
                req.getSavepointPath());
        return new JobIdResponse(jobId);
    }

    /**
     * Single-shot convenience: upload, then immediately run.  The upload is
     * multipart and the run parameters are query / form fields -- we avoid a
     * JSON + multipart mix because that requires separate @RequestPart
     * per field, which obscures the shape.
     */
    @PostMapping(path = "/submit", consumes = "multipart/form-data")
    public JobIdResponse submit(
            @RequestPart("jar") MultipartFile jar,
            @RequestParam(value = "entryClass", required = false) String entryClass,
            @RequestParam(value = "parallelism", required = false) Integer parallelism,
            @RequestParam(value = "programArgs", required = false) String programArgs,
            @RequestParam(value = "savepointPath", required = false) String savepointPath)
            throws IOException {
        requireNonEmpty(jar);
        String jarId = flink.uploadJar(jar);
        String jobId = flink.runJar(
                jarId,
                firstNonEmpty(entryClass, properties.getJob().getDefaultEntryClass()),
                parallelism != null && parallelism > 0
                        ? parallelism : properties.getJob().getDefaultParallelism(),
                programArgs,
                savepointPath);
        return new JobIdResponse(jobId);
    }

    // ------------------------------------------------------------------
    // Savepoint / cancel / status
    // ------------------------------------------------------------------

    @PostMapping("/{jobId}/savepoint")
    public SavepointResponse savepoint(@PathVariable("jobId") @NotBlank String jobId) {
        String requestId = flink.triggerSavepoint(jobId, properties.getJob().getSavepointDir());
        log.info("savepoint triggered for job {} -> request {}", jobId, requestId);
        return new SavepointResponse(requestId);
    }

    @PostMapping("/{jobId}/cancel")
    public SavepointResponse cancelWithSavepoint(
            @PathVariable("jobId") @NotBlank String jobId,
            @RequestParam(value = "drain", defaultValue = "false") boolean drain) {
        String requestId = flink.stopWithSavepoint(
                jobId, properties.getJob().getSavepointDir(), drain);
        log.info("stop-with-savepoint for job {} -> request {}", jobId, requestId);
        return new SavepointResponse(requestId);
    }

    @PostMapping("/{jobId}/cancel-hard")
    public ResponseEntity<Void> hardCancel(@PathVariable("jobId") @NotBlank String jobId) {
        flink.cancel(jobId);
        log.warn("hard-cancel requested for job {} (no savepoint)", jobId);
        return ResponseEntity.accepted().build();
    }

    @GetMapping("/{jobId}/savepoints/{requestId}")
    public JsonNode savepointStatus(@PathVariable("jobId") @NotBlank String jobId,
                                    @PathVariable("requestId") @NotBlank String requestId) {
        return flink.savepointStatus(jobId, requestId);
    }

    @GetMapping("/{jobId}")
    public JsonNode job(@PathVariable("jobId") @NotBlank String jobId) {
        return flink.job(jobId);
    }

    @GetMapping
    public JsonNode listJobs() {
        return flink.jobsOverview();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static void validateSubmit(SubmitRequest r, boolean requireJarId) {
        if (r == null) {
            throw new IllegalArgumentException("request body is required");
        }
        if (requireJarId && (r.getJarId() == null || r.getJarId().isEmpty())) {
            throw new IllegalArgumentException("jarId is required; upload first or use /submit");
        }
    }

    private static void requireNonEmpty(MultipartFile jar) {
        if (jar == null || jar.isEmpty()) {
            throw new IllegalArgumentException("jar part is required and must not be empty");
        }
    }

    private static String firstNonEmpty(String a, String b) {
        return (a != null && !a.isEmpty()) ? a : b;
    }
}
