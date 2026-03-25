package com.orcp.query.store;

import com.orcp.common.model.AggregationResult;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ResultStore {
    private final Map<String, AggregationResult> store = new ConcurrentHashMap<>();

    public void put(AggregationResult result) {
        store.put(result.bizKey(), result);
    }

    public Optional<AggregationResult> get(String key) {
        return Optional.ofNullable(store.get(key));
    }
}
