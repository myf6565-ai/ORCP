package com.orcp.config.service;

import com.orcp.common.model.RuleConfig;
import com.orcp.config.model.TopicMapping;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class ConfigStoreService {
    private final CopyOnWriteArrayList<RuleConfig> rules = new CopyOnWriteArrayList<>(List.of(
            new RuleConfig("rule-order-3", "ORDER_CREATED", 3, "同一 bizKey 订单事件达到 3 次告警")
    ));

    private volatile TopicMapping topicMapping = new TopicMapping("raw-events", "standard-events", "result-events");

    public List<RuleConfig> listRules() {
        return rules;
    }

    public void addRule(RuleConfig ruleConfig) {
        rules.add(ruleConfig);
    }

    public TopicMapping getTopicMapping() {
        return topicMapping;
    }

    public void updateTopicMapping(TopicMapping mapping) {
        this.topicMapping = mapping;
    }
}
