package com.orcp.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * orcp-admin entry point.
 *
 * <p>Minimal control plane for the Flink cluster: submit/cancel/
 * savepoint jobs and expose an aggregated health endpoint. REST
 * endpoints are added in Stage F.
 */
@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients
public class AdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdminApplication.class, args);
    }
}
