-- =============================================================================
-- ORCP local detail schema (MySQL 8.0)
-- DEV_SPEC §7.1
--
-- Idempotent: re-running this file is safe.
-- Intended load order:
--   mysql -h<host> -uorcp_rw -p <mysql_detail_schema.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Database
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS `orcp_detail`
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_general_ci;

USE `orcp_detail`;

-- -----------------------------------------------------------------------------
-- Application accounts
--
-- Passwords shown here are placeholders; the operator MUST rotate them after
-- first install (or inject real values via Nacos).  mysql_native_password is
-- used so older drivers still connect cleanly -- Spring Boot 2.7 + the
-- 8.0.28 connector can handle caching_sha2 too, but we keep the baseline
-- predictable.
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS 'orcp_rw'@'%' IDENTIFIED WITH mysql_native_password BY 'ChangeMe_rw_1!';
CREATE USER IF NOT EXISTS 'orcp_ro'@'%' IDENTIFIED WITH mysql_native_password BY 'ChangeMe_ro_1!';

GRANT SELECT, INSERT, UPDATE, DELETE ON `orcp_detail`.* TO 'orcp_rw'@'%';
GRANT SELECT                         ON `orcp_detail`.* TO 'orcp_ro'@'%';
FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
-- t_customer -- used by the Flink JDBC lookup (§6.3, 02_dim_mysql.sql)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `t_customer` (
    `customer_id` BIGINT       NOT NULL             COMMENT 'business id, not auto_increment',
    `name`        VARCHAR(128) NOT NULL DEFAULT ''  COMMENT 'customer display name',
    `level`       VARCHAR(16)  NOT NULL DEFAULT 'NORMAL' COMMENT 'NORMAL / VIP / SVIP',
    `region`      VARCHAR(32)  NOT NULL DEFAULT 'CN-UNKNOWN' COMMENT 'ISO country + region code',
    `created_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`customer_id`),
    KEY `idx_region_level` (`region`, `level`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='customer dimension (Flink lookup source)';

-- -----------------------------------------------------------------------------
-- t_order -- raw event table; populated by orcp-ingest (Stage D)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `t_order` (
    `order_id`    BIGINT         NOT NULL,
    `customer_id` BIGINT         NOT NULL,
    `biz_type`    VARCHAR(32)    NOT NULL COMMENT 'ORDER / REFUND',
    `amount`      DECIMAL(18,4)  NOT NULL DEFAULT 0,
    `status`      VARCHAR(16)    NOT NULL DEFAULT 'NEW',
    `created_at`  DATETIME       NOT NULL,
    `updated_at`  DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                     ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`order_id`),
    KEY `idx_customer`              (`customer_id`),
    KEY `idx_biztype_createdat`     (`biz_type`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='order fact table (ingest target)';

-- -----------------------------------------------------------------------------
-- t_order_item -- reserved for multi-line orders; Stage D only writes the
-- header, but keeping the table avoids a migration later.
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='order line items';

-- -----------------------------------------------------------------------------
-- t_dedup -- second tier of the ingest dedup (Caffeine LRU is first tier).
-- TTL 7 days; a nightly job (not part of this spec) deletes rows where
-- created_at < NOW() - INTERVAL 7 DAY.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `t_dedup` (
    `event_id`    VARCHAR(64)  NOT NULL,
    `created_at`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`event_id`),
    KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='event_id dedup ledger (7-day retention)';

-- =============================================================================
-- Seed data
-- =============================================================================
-- 50 customers covering three regions and three tier levels so Stage E's
-- JOIN has realistic cardinality.  INSERT IGNORE keeps re-runs safe.
-- The load generator scripts/gen_events.py defaults to customer ids 1..50
-- so every event's customerId finds a matching row here.
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
-- Sanity queries (run manually to verify the install).
--
--   SELECT COUNT(*)                 FROM t_customer;     -- expect 50
--   SELECT level, COUNT(*)          FROM t_customer GROUP BY level;
--   SELECT TABLE_NAME, TABLE_ROWS   FROM information_schema.TABLES
--      WHERE TABLE_SCHEMA = 'orcp_detail';
-- -----------------------------------------------------------------------------
