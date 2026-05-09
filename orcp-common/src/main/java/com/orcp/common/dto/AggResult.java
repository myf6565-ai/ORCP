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
 * Aggregated result written by the Flink job into OceanBase.
 * Mirrors the {@code agg_order_1min} table layout.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AggResult implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime windowStart;

    private String bizType;

    private Long customerId;

    private Long orderCnt;

    private BigDecimal amountSum;
}
