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
 * {@code orcp_detail.t_dedup} 表的实体对象，与阶段 C DDL 对应。
 *
 * <p>主键为业务 {@code eventId}（非自增代理键），
 * 配合 {@code INSERT IGNORE} 可实现原子的"首写者获胜"去重语义。
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
