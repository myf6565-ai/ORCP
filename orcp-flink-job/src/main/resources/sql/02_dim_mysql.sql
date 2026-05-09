-- =============================================================================
-- MySQL 维表：t_customer，通过 Flink JDBC lookup 方式访问。
-- 对应 docs/SQL/mysql_detail_schema.sql 中的 orcp_detail.t_customer。
--
-- PARTIAL 缓存：最多 10 万行，写入后 10 分钟过期。
-- lookup miss 时按需填充缓存，不做全量预加载。
--
-- 重试：最多 3 次 JDBC 重试。结合 Flink 的 fixed-delay 重启策略
--（最多 10 次，每次 30 秒），可扛过短时 MySQL 抖动。
-- =============================================================================

CREATE TABLE dim_customer (
    customer_id BIGINT NOT NULL,
    name        STRING,
    level       STRING,
    region      STRING,
    updated_at  TIMESTAMP(3),
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = '${mysql.url}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'table-name' = 't_customer',
    'username' = '${mysql.user}',
    'password' = '${mysql.password}',
    'lookup.cache' = 'PARTIAL',
    'lookup.partial-cache.max-rows' = '100000',
    'lookup.partial-cache.expire-after-write' = '10 min',
    'lookup.max-retries' = '3'
);
