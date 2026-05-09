-- =============================================================================
-- OceanBase 3.2.3 sink: agg_order_1min in the orcp_dw schema
-- (docs/SQL/oceanbase_dw_schema.sql).
--
-- Why 'jdbc:mysql://' and not 'jdbc:oceanbase://'?
--   * flink-connector-jdbc 3.1.2-1.17 selects its SQL dialect from the URL
--     prefix; only the built-in 'mysql', 'postgres', and 'derby' prefixes
--     are recognised.  'jdbc:oceanbase://' would throw a ValidationException
--     at CREATE TABLE time with no dialect matched.
--   * OceanBase Connector/J 2.4.14 advertises itself as 'com.oceanbase.jdbc.Driver'
--     but also accepts 'jdbc:mysql://' URLs unchanged.
--   So: we pin the driver explicitly AND keep the URL on the 'mysql' scheme.
--   DEV_SPEC §11 covers this combination.
--
-- The sink operates in UPSERT mode because the source is a keyed
-- aggregating stream (window + group by).  With the PRIMARY KEY declared
-- below, the JDBC connector generates INSERT ... ON DUPLICATE KEY UPDATE
-- for each flushed batch -- which OB 3.2.3 supports on non-partitioned
-- tables (the table in §7.2 has no explicit partition clause).
--
-- Buffering: flush every 1000 rows OR every 2s, whichever comes first.
-- Retries: 3 JDBC attempts.
-- =============================================================================

CREATE TABLE sink_agg_1min (
    window_start TIMESTAMP(3) NOT NULL,
    biz_type     STRING NOT NULL,
    customer_id  BIGINT NOT NULL,
    order_cnt    BIGINT NOT NULL,
    amount_sum   DECIMAL(18, 4) NOT NULL,
    PRIMARY KEY (window_start, biz_type, customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = '${ob.url}',
    'driver' = 'com.oceanbase.jdbc.Driver',
    'table-name' = 'agg_order_1min',
    'username' = '${ob.user}',
    'password' = '${ob.password}',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '2s',
    'sink.max-retries' = '3',
    'sink.parallelism' = '2'
);
