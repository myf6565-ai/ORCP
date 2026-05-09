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
 * Thin, typed wrapper around the Flink JobManager REST API.  Contains only
 * the calls the admin service actually uses; adding more should be
 * deliberate.
 *
 * <p>Failure model: every non-2xx response raises {@link FlinkRestException}
 * carrying the upstream status + body, so the controller can translate it
 * into a 502/504 without inspecting OkHttp internals.
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
        log.info("FlinkRestClient targeting {}", cfg.getRestUrl());
    }

    // ------------------------------------------------------------------
    // Health / info
    // ------------------------------------------------------------------

    /** @return parsed {@code /overview} payload, or throws on any failure. */
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
    // Jar lifecycle
    // ------------------------------------------------------------------

    /**
     * Uploads a jar and returns its Flink jar-id (the last path segment in
     * the {@code filename} field of the response).  Rejects on any non-2xx.
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
            // filename is returned as an absolute path; the jar-id is just the last segment.
            int slash = filename.lastIndexOf('/');
            String jarId = slash >= 0 ? filename.substring(slash + 1) : filename;
            if (jarId.isEmpty()) {
                throw new FlinkRestException("upload", 200,
                        "no filename in upload response: " + resp.toString());
            }
            log.info("Uploaded jar ({} bytes) to Flink as jar-id={}", jar.getSize(), jarId);
            return jarId;
        } finally {
            //noinspection ResultOfMethodCallIgnored
            tmp.delete();
        }
    }

    /**
     * Runs a previously-uploaded jar.  Returns the Flink {@code jobid}.
     *
     * <p>{@code programArgs} is a single string (Flink parses it with its own
     * shell-like splitter).  {@code savepointPath} is optional.
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
            throw new FlinkRestException("run", 200, "no jobid in run response: " + resp);
        }
        log.info("Submitted job jar-id={} -> job-id={}", jarId, jobId);
        return jobId;
    }

    // ------------------------------------------------------------------
    // Savepoint / stop / cancel
    // ------------------------------------------------------------------

    /** Triggers a savepoint.  Returns the savepoint {@code request-id}. */
    public String triggerSavepoint(String jobId, String targetDirectory) {
        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("target-directory", targetDirectory);
        body.put("cancel-job", false);
        JsonNode resp = postJson("/jobs/" + jobId + "/savepoints", body);
        return requireRequestId(resp, "savepoint");
    }

    /** Stops a job with a final savepoint.  Returns the request-id to poll. */
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

    /** Hard cancel without a savepoint.  Use only for recovery. */
    public void cancel(String jobId) {
        // Flink REST returns 202 Accepted with an empty body for PATCH ?mode=cancel.
        execute(new Request.Builder()
                .url(resolve("/jobs/" + jobId + "?mode=cancel"))
                .patch(RequestBody.create(new byte[0], null))
                .build());
    }

    // ------------------------------------------------------------------
    // HTTP helpers
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
                    "Failed to serialise request body: " + e.getMessage());
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
                    "I/O error: " + e.getMessage());
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
                    "I/O error: " + e.getMessage());
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
            throw new FlinkRestException(what, 200, "no request-id in response: " + resp);
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
            // Ensure the tmp file is readable by our user only.
            //noinspection ResultOfMethodCallIgnored
            dest.setReadable(true, true);
        }
        // Belt-and-braces: the OkHttp multipart wrapper opens the file on its own.
        if (!Files.exists(dest.toPath())) {
            throw new IOException("tmp upload file vanished: " + dest);
        }
    }
}
