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
 * Canonical upstream event contract produced to {@code orcp.src.*} and
 * forwarded (after dedup/persist) to {@code orcp.mid.events}.
 *
 * <p>The schema is intentionally flat to keep Flink SQL parsing simple;
 * richer payloads should flow through dedicated topics instead of
 * nesting complex structures here.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SourceEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Globally unique event id, used for dedup. */
    private String eventId;

    /** Business type, see {@code OrcpConstants.BIZ_TYPE_*}. */
    private String bizType;

    /** Business key (e.g. order id) for partition routing. */
    private String bizKey;

    /** Denormalised customer id used by downstream aggregations. */
    private Long customerId;

    /** Monetary amount if applicable. */
    private BigDecimal amount;

    /** Event time in Asia/Shanghai; NOT ingest time. */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime eventTime;

    /** Distributed trace id (opaque string). */
    private String traceId;
}
