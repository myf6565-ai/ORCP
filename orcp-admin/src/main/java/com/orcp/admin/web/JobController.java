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
 * 面向运维人员的 Flink 控制面 REST 接口。
 *
 * <p>所有端点均需要 Basic 认证（参见 {@link com.orcp.admin.config.SecurityConfig}）。
 *
 * <p>两种提交方式：
 * <ul>
 *   <li>{@code POST /api/jobs/submit} -- multipart 上传 + 立即运行（与 scripts/submit_job.sh 等价，适合临时操作）。</li>
 *   <li>{@code POST /api/jobs/upload} + {@code POST /api/jobs/run} -- 两步式，适合使用相同 jar 以不同参数多次提交。</li>
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
    // 上传 + 运行
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
     * 一步式便捷接口：上传后立即运行。
     * 运行参数通过 query/form 字段传递（避免混合 JSON + multipart 两种请求体格式）。
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
    // Savepoint / 取消 / 状态查询
    // ------------------------------------------------------------------

    @PostMapping("/{jobId}/savepoint")
    public SavepointResponse savepoint(@PathVariable("jobId") @NotBlank String jobId) {
        String requestId = flink.triggerSavepoint(jobId, properties.getJob().getSavepointDir());
        log.info("已为作业 {} 触发 savepoint -> request {}", jobId, requestId);
        return new SavepointResponse(requestId);
    }

    @PostMapping("/{jobId}/cancel")
    public SavepointResponse cancelWithSavepoint(
            @PathVariable("jobId") @NotBlank String jobId,
            @RequestParam(value = "drain", defaultValue = "false") boolean drain) {
        String requestId = flink.stopWithSavepoint(
                jobId, properties.getJob().getSavepointDir(), drain);
        log.info("已为作业 {} 发起 stop-with-savepoint -> request {}", jobId, requestId);
        return new SavepointResponse(requestId);
    }

    @PostMapping("/{jobId}/cancel-hard")
    public ResponseEntity<Void> hardCancel(@PathVariable("jobId") @NotBlank String jobId) {
        flink.cancel(jobId);
        log.warn("对作业 {} 执行硬取消（无 savepoint）", jobId);
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
    // 内部辅助方法
    // ------------------------------------------------------------------

    private static void validateSubmit(SubmitRequest r, boolean requireJarId) {
        if (r == null) {
            throw new IllegalArgumentException("请求体不能为空");
        }
        if (requireJarId && (r.getJarId() == null || r.getJarId().isEmpty())) {
            throw new IllegalArgumentException("jarId 是必填项；请先调用 /upload 或直接使用 /submit");
        }
    }

    private static void requireNonEmpty(MultipartFile jar) {
        if (jar == null || jar.isEmpty()) {
            throw new IllegalArgumentException("jar 部分是必需的且不能为空");
        }
    }

    private static String firstNonEmpty(String a, String b) {
        return (a != null && !a.isEmpty()) ? a : b;
    }
}
