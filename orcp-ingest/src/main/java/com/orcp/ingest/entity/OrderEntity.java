package com.orcp.ingest.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Row in {@code orcp_detail.t_order}.  Column names match the Stage C DDL.
 *
 * <p>{@code orderId} is derived from the source bizKey so the fact table
 * keeps its natural business-id primary key instead of an auto-increment.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("t_order")
public class OrderEntity {

    @TableId(value = "order_id", type = IdType.INPUT)
    private Long orderId;

    @TableField("customer_id")
    private Long customerId;

    @TableField("biz_type")
    private String bizType;

    private BigDecimal amount;

    private String status;

    @TableField("created_at")
    private LocalDateTime createdAt;

    @TableField(value = "updated_at", fill = com.baomidou.mybatisplus.annotation.FieldFill.UPDATE)
    private LocalDateTime updatedAt;
}
