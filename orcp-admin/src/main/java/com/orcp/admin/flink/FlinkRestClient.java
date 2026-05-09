package com.orcp.admin.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orcp.admin.config.AdminProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Map;

/**
 * Flink JobManager REST API 的轻量类型化封装。
 * 仅封装管控服务实际需要的接口；新增调用应经过审慎设计。
 *
 * <p>失败模型：所有非 2xx 响应均抛出 {@link FlinkRestException}，
 * 携带上游状态码和响应体，供控制器翻译为 502/504 而无需检查 OkHttp 内部状态。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FlinkRestClient {

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private static final MediaType OCTET_STREAM = MediaType.get("application/java-archive");

    private final AdminProperties properties;
    private final ObjectMapper objectMapper;
    private OkHttpClient http;

    @PostConstruct
    void init() {
        AdminProperties.Flink cfg = properties.getFlink();
        this.http = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofMillis(cfg.getConnectTimeoutMs()))
                .readTimeout(Duration.ofMillis(cfg.getReadTimeoutMs()))
                .writeTimeout(Duration.ofMillis(cfg.getWriteTimeoutMs()))
                .retryOnConnectionFailure(true)
                .build();
        log.info("FlinkRestClient 目标：{}", cfg.getRestUrl());
    }

    // ------------------------------------------------------------------
    // 健康 / 概览
    // ------------------------------------------------------------------

    /** @return 解析后的 {@code /overview} 响应，失败时抛出异常。 */
    public JsonNode overview() {
        return getJson("/overview");
    }

    public JsonNode jobsOverview() {
        return getJson("/jobs/overview");
    }

    public JsonNode taskManagers() {
        return getJson("/taskmanagers");
    }

    public JsonNode job(String jobId) {
        return getJson("/jobs/" + jobId);
    }

    // ------------------------------------------------------------------
    // Jar 生命周期
    // ------------------------------------------------------------------

    /**
     * 上传 jar 并返回其 Flink jar-id（响应 {@code filename} 字段的最后一段路径）。
     * 非 2xx 响应时拒绝请求。
     */
    public String uploadJar(MultipartFile jar) throws IOException {
        File tmp = File.createTempFile("orcp-upload-", ".jar");
        try {
            copyToFile(jar.getInputStream(), tmp);
            RequestBody body = new MultipartBody.Builder()
                    .setType(MultipartBody.FORM)
                    .addFormDataPart(
                            "jarfile",
                            jar.getOriginalFilename() != null ? jar.getOriginalFilename() : "orcp-flink-job.jar",
                            RequestBody.create(tmp, OCTET_STREAM))
                    .build();
            JsonNode resp = postMultipart("/jars/upload", body);
            String filename = resp.path("filename").asText("");
            // filename 是绝对路径；jar-id 是最后一段。
            int slash = filename.lastIndexOf('/');
            String jarId = slash >= 0 ? filename.substring(slash + 1) : filename;
            if (jarId.isEmpty()) {
                throw new FlinkRestException("upload", 200,
                        "上传响应中无 filename：" + resp.toString());
            }
            log.info("jar 已上传（{} 字节），Flink jar-id={}", jar.getSize(), jarId);
            return jarId;
        } finally {
            //noinspection ResultOfMethodCallIgnored
            tmp.delete();
        }
    }

    /**
     * 运行已上传的 jar，返回 Flink {@code jobid}。
     *
     * <p>{@code programArgs} 是一个字符串（Flink 使用自身解析器分割），
     * {@code savepointPath} 可为 null（全新启动）。
     */
    public String runJar(String jarId,
                         String entryClass,
                         int parallelism,
                         String programArgs,
                         String savepointPath) {
        Map<String, Object> body = new java.util.LinkedHashMap<>();
        if (entryClass != null && !entryClass.isEmpty()) {
            body.put("entryClass", entryClass);
        }
        body.put("parallelism", parallelism);
        if (programArgs != null && !programArgs.isEmpty()) {
            body.put("programArgs", programArgs);
        }
        body.put("allowNonRestoredState", false);
        if (savepointPath != null && !savepointPath.isEmpty()) {
            body.put("savepointPath", savepointPath);
        }
        JsonNode resp = postJson("/jars/" + jarId + "/run", body);
        String jobId = resp.path("jobid").asText("");
        if (jobId.isEmpty()) {
            throw new FlinkRestException("run", 200, "运行响应中无 jobid：" + resp);
        }
        log.info("已提交作业 jar-id={} -> job-id={}", jarId, jobId);
        return jobId;
    }

    // ------------------------------------------------------------------
    // Savepoint / 停止 / 取消
    // ------------------------------------------------------------------

    /** 触发 savepoint，返回 {@code request-id}。 */
    public String triggerSavepoint(String jobId, String targetDirectory) {
        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("target-directory", targetDirectory);
        body.put("cancel-job", false);
        JsonNode resp = postJson("/jobs/" + jobId + "/savepoints", body);
        return requireRequestId(resp, "savepoint");
    }

    /** 带 savepoint 优雅停止作业，返回 request-id 供轮询。 */
    public String stopWithSavepoint(String jobId, String targetDirectory, boolean drain) {
        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("targetDirectory", targetDirectory);
        body.put("drain", drain);
        JsonNode resp = postJson("/jobs/" + jobId + "/stop", body);
        return requireRequestId(resp, "stop");
    }

    public JsonNode savepointStatus(String jobId, String requestId) {
        return getJson("/jobs/" + jobId + "/savepoints/" + requestId);
    }

    /** 硬取消（不触发 savepoint），仅用于故障恢复场景。 */
    public void cancel(String jobId) {
        // Flink REST 对 PATCH ?mode=cancel 返回 202 空响应体。
        execute(new Request.Builder()
                .url(resolve("/jobs/" + jobId + "?mode=cancel"))
                .patch(RequestBody.create(new byte[0], null))
                .build());
    }

    // ------------------------------------------------------------------
    // HTTP 辅助方法
    // ------------------------------------------------------------------

    private JsonNode getJson(String path) {
        return executeJson(new Request.Builder().url(resolve(path)).get().build());
    }

    private JsonNode postJson(String path, Object body) {
        try {
            RequestBody rb = RequestBody.create(objectMapper.writeValueAsBytes(body), JSON);
            return executeJson(new Request.Builder().url(resolve(path)).post(rb).build());
        } catch (IOException e) {
            throw new FlinkRestException("POST " + path, 0,
                    "序列化请求体失败：" + e.getMessage());
        }
    }

    private JsonNode postMultipart(String path, RequestBody body) {
        return executeJson(new Request.Builder().url(resolve(path)).post(body).build());
    }

    private JsonNode executeJson(Request req) {
        try (Response resp = execute(req)) {
            ResponseBody body = resp.body();
            String text = body != null ? body.string() : "";
            if (text.isEmpty()) {
                return objectMapper.createObjectNode();
            }
            return objectMapper.readTree(text);
        } catch (IOException e) {
            throw new FlinkRestException(req.method() + " " + req.url().encodedPath(), 0,
                    "I/O 错误：" + e.getMessage());
        }
    }

    private Response execute(Request req) {
        try {
            Response resp = http.newCall(req).execute();
            if (!resp.isSuccessful()) {
                String body = "";
                try (ResponseBody rb = resp.body()) {
                    if (rb != null) body = rb.string();
                }
                int code = resp.code();
                resp.close();
                throw new FlinkRestException(
                        req.method() + " " + req.url().encodedPath(), code, body);
            }
            return resp;
        } catch (IOException e) {
            throw new FlinkRestException(
                    req.method() + " " + req.url().encodedPath(), 0,
                    "I/O 错误：" + e.getMessage());
        }
    }

    private String resolve(String path) {
        String base = properties.getFlink().getRestUrl();
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + (path.startsWith("/") ? path : "/" + path);
    }

    private static String requireRequestId(JsonNode resp, String what) {
        String id = resp.path("request-id").asText("");
        if (id.isEmpty()) {
            throw new FlinkRestException(what, 200, "响应中无 request-id：" + resp);
        }
        return id;
    }

    private static void copyToFile(InputStream in, File dest) throws IOException {
        try (InputStream src = in;
             FileOutputStream out = new FileOutputStream(dest)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = src.read(buf)) > 0) {
                out.write(buf, 0, n);
            }
        } finally {
            //noinspection ResultOfMethodCallIgnored
            dest.setReadable(true, true);
        }
        if (!Files.exists(dest.toPath())) {
            throw new IOException("临时上传文件已消失：" + dest);
        }
    }
}
