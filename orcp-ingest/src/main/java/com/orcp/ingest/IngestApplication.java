package com.orcp.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orcp.common.util.JsonUtils;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * orcp-ingest entry point.
 *
 * <p>Consumes external Kafka topics, persists detail rows into the local
 * MySQL {@code orcp_detail} schema and forwards normalised events to
 * the internal {@code orcp.mid.events} topic for the Flink job.
 *
 * <p>{@link MapperScan} keeps the MyBatis-Plus mapper package explicit so
 * developers can see at a glance where DAOs live.
 */
@SpringBootApplication
@EnableDiscoveryClient
@MapperScan("com.orcp.ingest.mapper")
public class IngestApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestApplication.class, args);
    }

    /**
     * Reuse the project-wide Jackson configuration from orcp-common so that
     * both serialisation (forward payload) and deserialisation (incoming
     * record) go through the same JSR-310 / NON_NULL / lenient settings.
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return JsonUtils.mapper();
    }
}

