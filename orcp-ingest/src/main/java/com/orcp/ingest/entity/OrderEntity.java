package com.orcp.ingest.entity;

import com.baomidou.mybatisplus.annotation.FieldFill;
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
 * {@code orcp_detail.t_order} 表的实体对象，字段名与阶段 C DDL 保持一致。
 *
 * <p>{@code orderId} 来自源事件的 bizKey，使用业务主键而非自增代理键。
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

    @TableField(value = "updated_at", fill = FieldFill.UPDATE)
    private LocalDateTime updatedAt;
}
