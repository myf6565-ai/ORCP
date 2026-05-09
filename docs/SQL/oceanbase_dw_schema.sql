-- =============================================================================
-- ORCP data warehouse schema (OceanBase 3.2.3, MySQL mode)
-- DEV_SPEC §7.2
--
-- Intentionally restricted to the MySQL 5.7 compatible subset:
--   * DEFAULT CHARSET=utf8mb4, NO utf8mb4_0900_ai_ci
--   * no CHECK constraints
--   * no INVISIBLE indexes, no FUNCTIONAL indexes
--   * no JSON_TABLE / IGNORE NULLS
--
-- The tenant user (e.g. orcp_rw@tenant#cluster) is assumed to already exist;
-- this file does NOT issue CREATE USER / GRANT -- that is handled by the OB
-- DBA outside the pipeline.
--
-- Idempotent: re-running this file is safe.
-- Connection string example:
--   jdbc:oceanbase://<host>:<port>/orcp_dw
--     ?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai
--     &rewriteBatchedStatements=true
-- =============================================================================

CREATE DATABASE IF NOT EXISTS `orcp_dw`
    DEFAULT CHARSET=utf8mb4
    COLLATE utf8mb4_general_ci;

USE `orcp_dw`;

-- -----------------------------------------------------------------------------
-- agg_order_1min  --  1-minute tumbling-window aggregates, primary result
-- table.  Fed by Flink SQL INSERT INTO (§6.3, 10_pipeline.sql).  Writes are
-- routed through the JDBC connector's upsert path (keyed by the primary key
-- below), so no ON DUPLICATE KEY UPDATE is required on the OB side.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `agg_order_1min` (
    `window_start`  DATETIME        NOT NULL                       COMMENT 'tumble window left edge, Asia/Shanghai',
    `biz_type`      VARCHAR(32)     NOT NULL                       COMMENT 'ORDER / REFUND',
    `customer_id`   BIGINT          NOT NULL,
    `order_cnt`     BIGINT          NOT NULL DEFAULT 0,
    `amount_sum`    DECIMAL(18,4)   NOT NULL DEFAULT 0,
    `update_time`   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`window_start`, `biz_type`, `customer_id`)
) DEFAULT CHARSET=utf8mb4
  COMMENT='1-minute window aggregates, upserted by orcp-flink-job';

-- -----------------------------------------------------------------------------
-- agg_order_daily -- daily rollup; currently populated by an optional
-- scheduled job, not on the critical path.  Kept here so the dashboard layer
-- has a stable target.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `agg_order_daily` (
    `stat_date`     DATE            NOT NULL,
    `biz_type`      VARCHAR(32)     NOT NULL,
    `order_cnt`     BIGINT          NOT NULL DEFAULT 0,
    `amount_sum`    DECIMAL(18,4)   NOT NULL DEFAULT 0,
    `update_time`   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`stat_date`, `biz_type`)
) DEFAULT CHARSET=utf8mb4
  COMMENT='daily rollup aggregates';

-- -----------------------------------------------------------------------------
-- Sanity queries (run manually after Stage E is live):
--
--   SELECT COUNT(*) FROM agg_order_1min;
--   SELECT window_start, biz_type, SUM(order_cnt), SUM(amount_sum)
--     FROM agg_order_1min
--    WHERE window_start >= DATE_SUB(NOW(), INTERVAL 10 MINUTE)
--    GROUP BY window_start, biz_type
--    ORDER BY window_start DESC
--    LIMIT 20;
-- -----------------------------------------------------------------------------
