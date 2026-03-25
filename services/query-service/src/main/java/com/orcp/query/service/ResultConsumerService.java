package com.orcp.query.service;

import com.orcp.common.model.AggregationResult;
import com.orcp.query.store.ResultStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ResultConsumerService {
    private static final Logger log = LoggerFactory.getLogger(ResultConsumerService.class);

    private final ResultStore resultStore;

    public ResultConsumerService(ResultStore resultStore) {
        this.resultStore = resultStore;
    }

    @KafkaListener(topics = "${orcp.kafka.topics.result-events}", groupId = "${orcp.kafka.consumer.group-id:query-service}")
    public void consume(AggregationResult result) {
        resultStore.put(result);
        log.info("写入查询缓存 bizKey={}, count={}", result.bizKey(), result.count());
    }
}
