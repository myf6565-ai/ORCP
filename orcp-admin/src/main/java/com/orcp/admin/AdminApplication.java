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
 * orcp-admin entry point.
 *
 * <p>A deliberately small control-plane: submit/cancel/savepoint/restore a
 * Flink job via REST, plus an aggregated health endpoint covering the four
 * externals the data plane depends on (Kafka, Flink, MySQL, OceanBase).
 *
 * <p>We exclude {@link DataSourceAutoConfiguration} because the service
 * holds no primary DataSource -- the MySQL and OceanBase probes open short
 * lived connections on demand via {@code DriverManager}.  This keeps the
 * classpath predictable: no HikariCP, no Flyway, no surprise bean wiring.
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableDiscoveryClient
public class AdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdminApplication.class, args);
    }

    /** Reuse the shared JSON contract from orcp-common. */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return JsonUtils.mapper();
    }
}
