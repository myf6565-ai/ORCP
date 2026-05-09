-- =============================================================================
-- Kafka source：消费 orcp-ingest 写入的内部 topic。
--
-- 字段契约必须与以下文件保持同步：
--   orcp-ingest/src/main/java/com/orcp/ingest/service/ForwardService.java
-- ForwardService 生成的 wire 格式为：
--   {
--     "event_id":    "evt-0000000001",
--     "biz_type":    "ORDER",
--     "biz_key":     "order-0000000001",
--     "customer_id": 42,
--     "amount":      "128.3200",          // JSON 字符串 -> DECIMAL(18,4)
--     "event_time":  "2026-05-09T10:00:00.000Z"   // ISO-8601 UTC
--   }
--
-- Watermark：5 秒乱序容忍度，足以覆盖集群内合理的网络延迟。
-- 如有跨机房或摄入层有缓冲的场景，可适当增大。
-- =============================================================================

CREATE TABLE src_events (
    event_id    STRING NOT NULL,
    biz_type    STRING NOT NULL,
    biz_key     STRING NOT NULL,
    customer_id BIGINT,
    amount      DECIMAL(18, 4),
    event_time  TIMESTAMP_LTZ(3),
    proc_time   AS PROCTIME(),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = '${kafka.mid.topic}',
    'properties.bootstrap.servers' = '${kafka.bootstrap}',
    'properties.group.id' = '${kafka.group.id}',
    'properties.isolation.level' = 'read_committed',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
