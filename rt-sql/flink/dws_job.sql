CREATE TABLE ods_source (
  event_id STRING,
  event_time TIMESTAMP(3),
  biz_key STRING,
  schema_version INT,
  trace_id STRING,
  amount DECIMAL(18,2),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = '${kafka.clean.topic}',
  'properties.bootstrap.servers' = '${kafka.bootstrap}',
  'properties.group.id' = 'rt-dws-sql',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE TABLE dim_biz_lookup (
  biz_key STRING,
  biz_type STRING,
  biz_owner STRING,
  PRIMARY KEY (biz_key) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = '${ob.jdbc.url}',
  'table-name' = 'dim_biz',
  'username' = '${ob.username}',
  'password' = '${ob.password}'
);

CREATE TABLE dws_sink (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  biz_type STRING,
  total_amount DECIMAL(18,2),
  event_cnt BIGINT,
  PRIMARY KEY (window_start, window_end, biz_type) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = '${ob.dws.jdbc.url}',
  'table-name' = 'dws_biz_tumble_1m',
  'username' = '${ob.username}',
  'password' = '${ob.password}'
);

INSERT INTO dws_sink
SELECT
  window_start,
  window_end,
  b.biz_type,
  SUM(o.amount) AS total_amount,
  COUNT(*) AS event_cnt
FROM TABLE(
  TUMBLE(TABLE ods_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
) o
LEFT JOIN dim_biz_lookup FOR SYSTEM_TIME AS OF o.event_time AS b
ON o.biz_key = b.biz_key
GROUP BY window_start, window_end, b.biz_type;
