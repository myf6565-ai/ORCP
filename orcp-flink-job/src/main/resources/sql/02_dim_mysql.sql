-- =============================================================================
-- MySQL dimension: t_customer, accessed through the Flink JDBC lookup table
-- source.  See orcp_detail.t_customer in docs/SQL/mysql_detail_schema.sql.
--
-- Partial cache: up to 100k rows, evicted 10 min after write.  Lookup misses
-- populate the cache on demand; there is no full-table preload.
--
-- Retries: 3 JDBC attempts before the lookup fails the pipeline.  Combined
-- with the Flink restart strategy (fixed-delay, 10 attempts, 30s delay), a
-- transient MySQL blip rides out without manual intervention.
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
