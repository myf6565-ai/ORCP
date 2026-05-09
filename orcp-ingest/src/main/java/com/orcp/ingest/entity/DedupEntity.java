package com.orcp.ingest.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Row in {@code orcp_detail.t_dedup}.  Mirrors the DDL from Stage C.
 *
 * <p>The primary key is the business {@code eventId}, not an auto-increment
 * surrogate, so that {@code INSERT IGNORE} gives us race-free
 * "reserve-first-writer-wins" semantics for dedup.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("t_dedup")
public class DedupEntity {

    @TableId(value = "event_id", type = IdType.INPUT)
    private String eventId;

    private LocalDateTime createdAt;
}
