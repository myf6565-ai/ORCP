package com.orcp.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orcp.common.util.JsonUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * orcp-admin 服务入口。
 *
 * <p>功能简介：通过 REST 接口提交/取消/savepoint Flink 作业，
 * 并提供覆盖 Kafka、Flink、MySQL、OceanBase 四个外部依赖的聚合健康检查端点。
 *
 * <p>排除 {@link DataSourceAutoConfiguration}：本服务不持有主 DataSource，
 * MySQL 和 OceanBase 的健康探针使用按需建立的 {@code DriverManager} 短连接。
 * 这样可避免引入 HikariCP、Flyway 和 Spring Boot JDBC 自动配置，保持依赖集整洁。
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableDiscoveryClient
public class AdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdminApplication.class, args);
    }

    /** 复用 orcp-common 提供的项目统一 JSON 配置。 */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return JsonUtils.mapper();
    }
}
