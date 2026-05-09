package com.orcp.ingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * orcp-ingest entry point.
 *
 * <p>Consumes external Kafka topics, persists detail rows into the local
 * MySQL {@code orcp_detail} schema and forwards normalised events to
 * the internal {@code orcp.mid.events} topic for the Flink job.
 *
 * <p>Business logic is added in Stage D.
 */
@SpringBootApplication
@EnableDiscoveryClient
public class IngestApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestApplication.class, args);
    }
}
