CREATE DATABASE IF NOT EXISTS rt_admin;
CREATE DATABASE IF NOT EXISTS rt_ods;
CREATE DATABASE IF NOT EXISTS rt_dws;

USE rt_admin;
CREATE TABLE IF NOT EXISTS job_instance (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  job_name VARCHAR(128) NOT NULL,
  flink_job_id VARCHAR(128) NOT NULL,
  state VARCHAR(32) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

USE rt_ods;
CREATE TABLE IF NOT EXISTS ods_event_detail (
  event_id VARCHAR(64) PRIMARY KEY,
  event_time VARCHAR(64) NOT NULL,
  biz_key VARCHAR(128) NOT NULL,
  schema_version INT NOT NULL,
  trace_id VARCHAR(128) NOT NULL,
  amount DECIMAL(18,2) DEFAULT 0,
  version VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS dim_biz (
  biz_key VARCHAR(128) PRIMARY KEY,
  biz_type VARCHAR(64) NOT NULL,
  biz_owner VARCHAR(64) NOT NULL
);

USE rt_dws;
CREATE TABLE IF NOT EXISTS dws_biz_tumble_1m (
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  biz_type VARCHAR(64) NOT NULL,
  total_amount DECIMAL(18,2) NOT NULL,
  event_cnt BIGINT NOT NULL,
  PRIMARY KEY (window_start, window_end, biz_type)
);
