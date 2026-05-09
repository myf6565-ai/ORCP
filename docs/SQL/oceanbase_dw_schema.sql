-- =============================================================================
-- ORCP 数据仓库建表脚本（OceanBase 3.2.3，MySQL 模式）
-- DEV_SPEC §7.2
--
-- 刻意限定在 MySQL 5.7 兼容子集内：
--   * DEFAULT CHARSET=utf8mb4，不使用 utf8mb4_0900_ai_ci
--   * 不使用 CHECK 约束
--   * 不使用 INVISIBLE 索引、FUNCTIONAL INDEX
--   * 不使用 JSON_TABLE / IGNORE NULLS
--
-- 租户用户（如 orcp_rw@tenant#cluster）假定已存在；
-- 本文件 **不** 执行 CREATE USER / GRANT——由 OB DBA 在管控平台外部处理。
--
-- 幂等性：重复执行本文件是安全的。
-- 连接串示例：
--   jdbc:oceanbase://<host>:<port>/orcp_dw
--     ?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai
--     &rewriteBatchedStatements=true
-- =============================================================================

CREATE DATABASE IF NOT EXISTS `orcp_dw`
    DEFAULT CHARSET=utf8mb4
    COLLATE utf8mb4_general_ci;

USE `orcp_dw`;

-- -----------------------------------------------------------------------------
-- agg_order_1min —— 1 分钟滚动窗口聚合，主要结果表。
-- 由 Flink SQL INSERT INTO（§6.3 10_pipeline.sql）写入。
-- 写入通过 JDBC connector 的 upsert 路径（按主键）完成，
-- OB 端无需 ON DUPLICATE KEY UPDATE。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `agg_order_1min` (
    `window_start`  DATETIME        NOT NULL                       COMMENT '滚动窗口左边界（Asia/Shanghai）',
    `biz_type`      VARCHAR(32)     NOT NULL                       COMMENT 'ORDER / REFUND',
    `customer_id`   BIGINT          NOT NULL,
    `order_cnt`     BIGINT          NOT NULL DEFAULT 0             COMMENT '窗口内订单数',
    `amount_sum`    DECIMAL(18,4)   NOT NULL DEFAULT 0             COMMENT '窗口内金额合计',
    `update_time`   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                    ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`window_start`, `biz_type`, `customer_id`)
) DEFAULT CHARSET=utf8mb4
  COMMENT='1 分钟窗口聚合，由 orcp-flink-job upsert 写入';

-- -----------------------------------------------------------------------------
-- agg_order_daily —— 日级汇总表，当前由可选的定时任务填充，
-- 不在核心路径上。保留此表为后续看板层提供稳定的查询目标。
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
  COMMENT='日级汇总聚合';

-- -----------------------------------------------------------------------------
-- 验证查询（阶段 E 上线后手动执行）：
--
--   SELECT COUNT(*) FROM agg_order_1min;
--   SELECT window_start, biz_type, SUM(order_cnt), SUM(amount_sum)
--     FROM agg_order_1min
--    WHERE window_start >= DATE_SUB(NOW(), INTERVAL 10 MINUTE)
--    GROUP BY window_start, biz_type
--    ORDER BY window_start DESC
--    LIMIT 20;
-- -----------------------------------------------------------------------------
