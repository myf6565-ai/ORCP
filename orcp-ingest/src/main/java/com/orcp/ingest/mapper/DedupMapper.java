package com.orcp.ingest.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.orcp.ingest.entity.DedupEntity;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;

/**
 * MyBatis-Plus mapper for {@code orcp_detail.t_dedup}.
 *
 * <p>We hand-write {@link #insertIgnore(String, LocalDateTime)} instead of
 * relying on the default {@code insert}, because {@code INSERT IGNORE}
 * silently skips the row when the primary key already exists and returns 0
 * affected rows.  That is exactly the atomic "claim the eventId" primitive
 * we want for dedup, with no try/catch on a duplicate-key exception.
 */
@Mapper
public interface DedupMapper extends BaseMapper<DedupEntity> {

    /**
     * @return 1 if this eventId was inserted (caller is the owner), 0 if
     *         another thread/process already owns it (duplicate).
     */
    @Insert("INSERT IGNORE INTO t_dedup (event_id, created_at) VALUES (#{eventId}, #{createdAt})")
    int insertIgnore(@Param("eventId") String eventId,
                     @Param("createdAt") LocalDateTime createdAt);
}
