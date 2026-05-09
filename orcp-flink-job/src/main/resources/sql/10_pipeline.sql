-- =============================================================================
-- 端到端流水线：source -> 维表 lookup -> 1 分钟滚动窗口 -> sink。
--
-- 关于执行顺序的重要说明：
--   窗口 TVF（TUMBLE、HOP 等）会将输入时间属性降级为普通 TIMESTAMP_LTZ，
--   导致其输出列不再是时间属性。这意味着：
--     * FOR SYSTEM_TIME AS OF proc_time 的时态/lookup JOIN 必须在 TUMBLE 之前执行，
--       此时 proc_time 仍是时间属性；
--     * TUMBLE 之后只剩 window_start / window_end 时间戳，这正是 sink 主键所需的。
--   因此本文件先用 CREATE TEMPORARY VIEW 构建增强后的流（含维表 JOIN），
--   再对该 VIEW 执行窗口聚合 INSERT。
--
-- 使用 LEFT JOIN 而非 INNER JOIN：
--   维表缺失行时不丢弃事件——聚合仍计数，仅维表字段为 null。
--   保留 JOIN（即使当前未投影 level/region）意味着阶段 F+ 可在
--   Grafana 下钻时增加维度字段，而无需重新部署流水线。
-- =============================================================================

-- 第一步：维表 lookup JOIN，必须在 TUMBLE 之前完成。
CREATE TEMPORARY VIEW enriched_events AS
SELECT
    e.event_id,
    e.biz_type,
    e.customer_id,
    e.amount,
    e.event_time,
    c.level     AS customer_level,
    c.region    AS customer_region
FROM src_events AS e
LEFT JOIN dim_customer FOR SYSTEM_TIME AS OF e.proc_time AS c
    ON e.customer_id = c.customer_id;

-- 第二步：窗口聚合 + 写入 OceanBase。
INSERT INTO sink_agg_1min
SELECT
    window_start,
    biz_type,
    customer_id,
    COUNT(*)      AS order_cnt,
    SUM(amount)   AS amount_sum
FROM TABLE(
    TUMBLE(TABLE enriched_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, biz_type, customer_id;
