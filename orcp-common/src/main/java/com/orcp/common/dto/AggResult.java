package com.orcp.common.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Flink 作业写入 OceanBase 的聚合结果。
 * 字段布局与 {@code agg_order_1min} 表保持一致。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AggResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 滚动窗口左边界时间戳。 */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime windowStart;

    /** 业务类型（ORDER / REFUND）。 */
    private String bizType;

    private Long customerId;

    /** 窗口内订单数。 */
    private Long orderCnt;

    /** 窗口内金额合计。 */
    private BigDecimal amountSum;
}
