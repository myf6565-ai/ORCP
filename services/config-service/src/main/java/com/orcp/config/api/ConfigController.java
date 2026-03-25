package com.orcp.config.api;

import com.orcp.common.model.RuleConfig;
import com.orcp.common.web.ApiResponse;
import com.orcp.config.model.TopicMapping;
import com.orcp.config.service.ConfigStoreService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/config")
public class ConfigController {
    private final ConfigStoreService configStoreService;

    public ConfigController(ConfigStoreService configStoreService) {
        this.configStoreService = configStoreService;
    }

    @GetMapping("/rules")
    public ApiResponse<List<RuleConfig>> rules() {
        return ApiResponse.success(configStoreService.listRules());
    }

    @PostMapping("/rules")
    public ApiResponse<Void> createRule(@RequestBody RuleConfig ruleConfig) {
        configStoreService.addRule(ruleConfig);
        return ApiResponse.success(null);
    }

    @GetMapping("/topics")
    public ApiResponse<TopicMapping> topics() {
        return ApiResponse.success(configStoreService.getTopicMapping());
    }

    @PutMapping("/topics")
    public ApiResponse<Void> updateTopics(@RequestBody TopicMapping mapping) {
        configStoreService.updateTopicMapping(mapping);
        return ApiResponse.success(null);
    }
}
