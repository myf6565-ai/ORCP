-- =============================================================================
-- ORCP 本地明细库建表脚本（MySQL 8.0）
-- DEV_SPEC §7.1
--
-- 幂等性：重复执行本文件是安全的。
-- 建议执行方式：
--   mysql -h<host> -uorcp_rw -p < mysql_detail_schema.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 数据库
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `orcp_detail`
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_general_ci;

USE `orcp_detail`;

-- -----------------------------------------------------------------------------
-- 应用账号
--
-- 下方密码仅为占位符，操作人员在首次安装后 **必须** 立即修改
-- （或通过 Nacos 注入真实值）。使用 mysql_native_password 以确保旧版驱动兼容性，
-- Spring Boot 2.7 + 8.0.28 驱动同时支持 caching_sha2，保持此配置以便基线可预测。
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS 'orcp_rw'@'%' IDENTIFIED WITH mysql_native_password BY 'ChangeMe_rw_1!';
CREATE USER IF NOT EXISTS 'orcp_ro'@'%' IDENTIFIED WITH mysql_native_password BY 'ChangeMe_ro_1!';

GRANT SELECT, INSERT, UPDATE, DELETE ON `orcp_detail`.* TO 'orcp_rw'@'%';
GRANT SELECT                         ON `orcp_detail`.* TO 'orcp_ro'@'%';
FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
-- t_customer —— 供 Flink JDBC lookup 使用的客户维表（参见 §6.3 02_dim_mysql.sql）
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `t_customer` (
    `customer_id` BIGINT       NOT NULL             COMMENT '业务 ID，非自增主键',
    `name`        VARCHAR(128) NOT NULL DEFAULT ''  COMMENT '客户展示名称',
    `level`       VARCHAR(16)  NOT NULL DEFAULT 'NORMAL' COMMENT '客户等级：NORMAL / VIP / SVIP',
    `region`      VARCHAR(32)  NOT NULL DEFAULT 'CN-UNKNOWN' COMMENT '国家 + 区域代码',
    `created_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`customer_id`),
    KEY `idx_region_level` (`region`, `level`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户维表（Flink lookup 来源）';

-- -----------------------------------------------------------------------------
-- t_order —— 事实表，由 orcp-ingest（阶段 D）写入
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `t_order` (
    `order_id`    BIGINT         NOT NULL,
    `customer_id` BIGINT         NOT NULL,
    `biz_type`    VARCHAR(32)    NOT NULL COMMENT '业务类型：ORDER / REFUND',
    `amount`      DECIMAL(18,4)  NOT NULL DEFAULT 0,
    `status`      VARCHAR(16)    NOT NULL DEFAULT 'NEW',
    `created_at`  DATETIME       NOT NULL,
    `updated_at`  DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                     ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`order_id`),
    KEY `idx_customer`              (`customer_id`),
    KEY `idx_biztype_createdat`     (`biz_type`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单事实表（摄入目标）';

-- -----------------------------------------------------------------------------
-- t_order_item —— 为多行订单预留，阶段 D 仅写入订单头部；
-- 保留此表以避免未来迁移。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `t_order_item` (
    `id`          BIGINT        NOT NULL AUTO_INCREMENT,
    `order_id`    BIGINT        NOT NULL,
    `sku_id`      VARCHAR(64)   NOT NULL DEFAULT '',
    `qty`         INT           NOT NULL DEFAULT 1,
    `price`       DECIMAL(18,4) NOT NULL DEFAULT 0,
    `created_at`  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `idx_order_id` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单明细行';

-- -----------------------------------------------------------------------------
-- t_dedup —— 消息去重账本（第二层，Caffeine 为第一层）。
-- TTL 7 天，由定时任务清理（参见 OPS_RUNBOOK.md §4.1）。
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `t_dedup` (
    `event_id`    VARCHAR(64)  NOT NULL,
    `created_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`event_id`),
    KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='事件去重账本（7 天保留期）';

-- =============================================================================
-- 种子数据
-- =============================================================================
-- 50 名客户，跨三个区域和三个等级，使阶段 E 的 JOIN 具备真实的基数分布。
-- 发压脚本 scripts/gen_events.py 默认 customerId 范围为 1..50，
-- 确保每条事件的 customerId 都能找到对应的维表行。
-- INSERT IGNORE 保证重复执行时不报错。
-- -----------------------------------------------------------------------------
INSERT IGNORE INTO `t_customer` (`customer_id`, `name`, `level`, `region`) VALUES
    (1 , 'customer-0001', 'NORMAL', 'CN-EAST'),
    (2 , 'customer-0002', 'VIP'   , 'CN-EAST'),
    (3 , 'customer-0003', 'SVIP'  , 'CN-EAST'),
    (4 , 'customer-0004', 'NORMAL', 'CN-NORTH'),
    (5 , 'customer-0005', 'VIP'   , 'CN-NORTH'),
    (6 , 'customer-0006', 'SVIP'  , 'CN-NORTH'),
    (7 , 'customer-0007', 'NORMAL', 'CN-SOUTH'),
    (8 , 'customer-0008', 'VIP'   , 'CN-SOUTH'),
    (9 , 'customer-0009', 'SVIP'  , 'CN-SOUTH'),
    (10, 'customer-0010', 'NORMAL', 'CN-EAST'),
    (11, 'customer-0011', 'VIP'   , 'CN-EAST'),
    (12, 'customer-0012', 'NORMAL', 'CN-NORTH'),
    (13, 'customer-0013', 'VIP'   , 'CN-NORTH'),
    (14, 'customer-0014', 'NORMAL', 'CN-SOUTH'),
    (15, 'customer-0015', 'VIP'   , 'CN-SOUTH'),
    (16, 'customer-0016', 'NORMAL', 'CN-EAST'),
    (17, 'customer-0017', 'NORMAL', 'CN-NORTH'),
    (18, 'customer-0018', 'NORMAL', 'CN-SOUTH'),
    (19, 'customer-0019', 'VIP'   , 'CN-EAST'),
    (20, 'customer-0020', 'VIP'   , 'CN-NORTH'),
    (21, 'customer-0021', 'VIP'   , 'CN-SOUTH'),
    (22, 'customer-0022', 'SVIP'  , 'CN-EAST'),
    (23, 'customer-0023', 'SVIP'  , 'CN-NORTH'),
    (24, 'customer-0024', 'SVIP'  , 'CN-SOUTH'),
    (25, 'customer-0025', 'NORMAL', 'CN-EAST'),
    (26, 'customer-0026', 'NORMAL', 'CN-NORTH'),
    (27, 'customer-0027', 'NORMAL', 'CN-SOUTH'),
    (28, 'customer-0028', 'VIP'   , 'CN-EAST'),
    (29, 'customer-0029', 'VIP'   , 'CN-NORTH'),
    (30, 'customer-0030', 'VIP'   , 'CN-SOUTH'),
    (31, 'customer-0031', 'NORMAL', 'CN-EAST'),
    (32, 'customer-0032', 'NORMAL', 'CN-NORTH'),
    (33, 'customer-0033', 'NORMAL', 'CN-SOUTH'),
    (34, 'customer-0034', 'VIP'   , 'CN-EAST'),
    (35, 'customer-0035', 'VIP'   , 'CN-NORTH'),
    (36, 'customer-0036', 'VIP'   , 'CN-SOUTH'),
    (37, 'customer-0037', 'SVIP'  , 'CN-EAST'),
    (38, 'customer-0038', 'SVIP'  , 'CN-NORTH'),
    (39, 'customer-0039', 'SVIP'  , 'CN-SOUTH'),
    (40, 'customer-0040', 'NORMAL', 'CN-EAST'),
    (41, 'customer-0041', 'NORMAL', 'CN-NORTH'),
    (42, 'customer-0042', 'NORMAL', 'CN-SOUTH'),
    (43, 'customer-0043', 'VIP'   , 'CN-EAST'),
    (44, 'customer-0044', 'VIP'   , 'CN-NORTH'),
    (45, 'customer-0045', 'VIP'   , 'CN-SOUTH'),
    (46, 'customer-0046', 'NORMAL', 'CN-EAST'),
    (47, 'customer-0047', 'NORMAL', 'CN-NORTH'),
    (48, 'customer-0048', 'NORMAL', 'CN-SOUTH'),
    (49, 'customer-0049', 'VIP'   , 'CN-EAST'),
    (50, 'customer-0050', 'SVIP'  , 'CN-NORTH');

-- -----------------------------------------------------------------------------
-- 验证查询（手动执行确认安装结果）：
--
--   SELECT COUNT(*)                 FROM t_customer;     -- 期望 50
--   SELECT level, COUNT(*)          FROM t_customer GROUP BY level;
--   SELECT TABLE_NAME, TABLE_ROWS   FROM information_schema.TABLES
--      WHERE TABLE_SCHEMA = 'orcp_detail';
-- -----------------------------------------------------------------------------
