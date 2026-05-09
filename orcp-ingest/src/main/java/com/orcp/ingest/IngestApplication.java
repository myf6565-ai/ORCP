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
 * orcp-ingest 服务入口。
 *
 * <p>消费外部 Kafka topic，将明细行写入本地 MySQL {@code orcp_detail} 库，
 * 并将标准化后的事件转发到内部 {@code orcp.mid.events} topic 供 Flink 作业消费。
 *
 * <p>{@link MapperScan} 显式声明 MyBatis-Plus Mapper 扫描包，
 * 方便开发者一眼看到 DAO 层的位置。
 */
@SpringBootApplication
@EnableDiscoveryClient
@MapperScan("com.orcp.ingest.mapper")
public class IngestApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestApplication.class, args);
    }

    /**
     * 复用 orcp-common 提供的项目统一 Jackson 配置，
     * 确保序列化（转发 payload）与反序列化（incoming record）
     * 使用相同的 JSR-310 / NON_NULL / 宽松模式。
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return JsonUtils.mapper();
    }
}
