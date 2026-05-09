package com.orcp.common.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 上游事件的统一消息契约，写入 {@code orcp.src.*} 并在
 * 去重/持久化后转发到 {@code orcp.mid.events}。
 *
 * <p>Schema 刻意保持扁平结构，以简化 Flink SQL 解析；
 * 更复杂的 payload 应通过专用 topic 传输，而非嵌套在此对象中。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SourceEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 全局唯一事件 ID，用于去重。 */
    private String eventId;

    /** 业务类型，参见 {@code OrcpConstants.BIZ_TYPE_*}。 */
    private String bizType;

    /** 业务键（如订单 ID），用于分区路由。 */
    private String bizKey;

    /** 客户 ID，供下游聚合使用（非规范化字段）。 */
    private Long customerId;

    /** 金额（如适用）。 */
    private BigDecimal amount;

    /** 事件发生时间（Asia/Shanghai），非摄入时间。 */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime eventTime;

    /** 分布式追踪 ID（不透明字符串）。 */
    private String traceId;
}
