package com.orcp.ingest.config;

import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import org.apache.ibatis.reflection.MetaObject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.LocalDateTime;

/**
 * MyBatis-Plus hooks.
 *
 * <p>Auto-fills {@code updated_at} on update; {@code created_at} is always
 * set explicitly by the ingest pipeline (we use the source {@code eventTime}
 * when present), so we do NOT fill it here.
 */
@Configuration
@EnableTransactionManagement
public class MybatisConfig {

    @Bean
    public MetaObjectHandler metaObjectHandler() {
        return new MetaObjectHandler() {
            @Override
            public void insertFill(MetaObject metaObject) {
                // no-op; createdAt is supplied by the application
            }

            @Override
            public void updateFill(MetaObject metaObject) {
                this.strictUpdateFill(metaObject, "updatedAt",
                        LocalDateTime.class, LocalDateTime.now());
            }
        };
    }
}
