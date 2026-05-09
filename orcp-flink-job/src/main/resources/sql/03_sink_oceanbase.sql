-- =============================================================================
-- OceanBase 3.2.3 sink：orcp_dw 库的 agg_order_1min 表。
-- 对应 docs/SQL/oceanbase_dw_schema.sql。
--
-- 为何 URL 前缀使用 'jdbc:mysql://' 而非 'jdbc:oceanbase://'？
--   * flink-connector-jdbc 3.1.2-1.17 根据 URL scheme 选择 SQL 方言；
--     只认识 'mysql'、'postgres'、'derby' 三种前缀。
--     'jdbc:oceanbase://' 会在 CREATE TABLE 时抛出 ValidationException（无法匹配方言）。
--   * OceanBase Connector/J 2.4.14 以 'com.oceanbase.jdbc.Driver' 注册，
--     但同时接受 'jdbc:mysql://' URL，两者完全兼容。
--   因此：显式指定 driver 并保持 URL 使用 mysql scheme 即可。
--   参见 DEV_SPEC §11 及 Stage E PR 说明。
--
-- sink 以 UPSERT 模式运行（上游为有键聚合流）。
-- 声明 PRIMARY KEY 后，JDBC connector 为每批写入生成
-- INSERT ... ON DUPLICATE KEY UPDATE —— OB 3.2.3 在非分区表上支持此语法。
--
-- 缓冲：每 1000 行或每 2 秒刷新一次（以先到者为准）。
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
