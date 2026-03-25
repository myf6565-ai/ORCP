package com.orcp.ingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.orcp")
public class IngestServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(IngestServiceApplication.class, args);
    }
}
