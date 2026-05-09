package com.orcp.ingest.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.orcp.ingest.entity.DedupEntity;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;

/**
 * {@code orcp_detail.t_dedup} 的 MyBatis-Plus Mapper。
 *
 * <p>手写 {@link #insertIgnore(String, LocalDateTime)} 而非使用默认的 {@code insert}，
 * 原因：主键已存在时 {@code INSERT IGNORE} 静默跳过并返回 0，
 * 这正是原子"声明 eventId 所有权"原语所需的行为，无需捕获重复键异常。
 */
@Mapper
public interface DedupMapper extends BaseMapper<DedupEntity> {

    /**
     * @return 1 表示本次调用成为该 eventId 的所有者（应执行后续转发）；
     *         0 表示其他线程/进程已先行写入（重复，直接跳过）。
     */
    @Insert("INSERT IGNORE INTO t_dedup (event_id, created_at) VALUES (#{eventId}, #{createdAt})")
    int insertIgnore(@Param("eventId") String eventId,
                     @Param("createdAt") LocalDateTime createdAt);
}
