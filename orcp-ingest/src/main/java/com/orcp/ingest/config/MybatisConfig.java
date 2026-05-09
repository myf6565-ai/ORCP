package com.orcp.ingest.config;

import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import org.apache.ibatis.reflection.MetaObject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.LocalDateTime;

/**
 * MyBatis-Plus 钩子配置。
 *
 * <p>仅在 UPDATE 时自动填充 {@code updated_at}；
 * {@code created_at} 由流水线显式设置（使用源事件的 {@code eventTime}），
 * 因此 <strong>不在此处填充</strong>。
 */
@Configuration
@EnableTransactionManagement
public class MybatisConfig {

    @Bean
    public MetaObjectHandler metaObjectHandler() {
        return new MetaObjectHandler() {
            @Override
            public void insertFill(MetaObject metaObject) {
                // 无操作：createdAt 由业务代码显式提供
            }

            @Override
            public void updateFill(MetaObject metaObject) {
                this.strictUpdateFill(metaObject, "updatedAt",
                        LocalDateTime.class, LocalDateTime.now());
            }
        };
    }
}
