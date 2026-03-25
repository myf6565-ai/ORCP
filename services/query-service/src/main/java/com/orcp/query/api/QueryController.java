package com.orcp.query.api;

import com.orcp.common.model.AggregationResult;
import com.orcp.common.web.ApiResponse;
import com.orcp.query.store.ResultStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@RequestMapping("/api/results")
public class QueryController {

    private final ResultStore resultStore;

    public QueryController(ResultStore resultStore) {
        this.resultStore = resultStore;
        resultStore.put(new AggregationResult("demo-key", true, 3, Instant.parse("2026-03-25T10:01:20Z"), Instant.now()));
    }

    @GetMapping("/{key}")
    public ApiResponse<AggregationResult> getByKey(@PathVariable String key) {
        return resultStore.get(key)
                .map(ApiResponse::success)
                .orElseGet(() -> ApiResponse.error("未找到结果: " + key));
    }
}
