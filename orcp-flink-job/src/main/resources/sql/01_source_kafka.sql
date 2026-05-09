-- =============================================================================
-- Kafka source: consumes the internal topic produced by orcp-ingest.
--
-- Column contract MUST stay in lockstep with
--   orcp-ingest/src/main/java/com/orcp/ingest/service/ForwardService.java
-- which emits:
--   {
--     "event_id":    "evt-0000000001",
--     "biz_type":    "ORDER",
--     "biz_key":     "order-0000000001",
--     "customer_id": 42,
--     "amount":      "128.3200",          // JSON string -> DECIMAL(18,4)
--     "event_time":  "2026-05-09T10:00:00.000Z"   // ISO-8601 UTC
--   }
--
-- Watermark: 5s out-of-order tolerance is enough for a realistic in-cluster
-- path (ingest -> kafka -> flink).  Increase if cross-region or if the ingest
-- path ever buffers for longer.
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
