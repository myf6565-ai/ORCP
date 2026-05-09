-- =============================================================================
-- End-to-end pipeline: source -> dim lookup -> 1-min tumbling window -> sink.
--
-- Shape note for anyone new to Flink SQL: windowing TVFs (TUMBLE, HOP, ...)
-- demote the time attribute of their input to a regular TIMESTAMP_LTZ in
-- their output.  That means:
--   * a temporal/lookup JOIN on FOR SYSTEM_TIME AS OF MUST happen BEFORE
--     the TUMBLE, while proc_time / event_time are still time attributes;
--   * after the TUMBLE we only have window_start / window_end timestamps,
--     which is exactly what the sink's primary key needs.
-- That's why this file creates a TEMPORARY VIEW first (enriched stream with
-- the dim column joined in) and then runs the windowed INSERT against it.
--
-- Left join so a missing customer row does NOT drop the event -- the
-- aggregate is still counted; only the dim enrichment is null.  For the
-- current agg we don't actually project any dim column, but the join is
-- preserved so Grafana drilldowns (Stage F+) can add level/region without
-- another pipeline deploy.
-- =============================================================================

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
