# Architecture

## Control Plane
- rt-gateway: API gateway
- rt-compute-admin: job submit/stop/savepoint/status/replay endpoint

## Data Plane
- rt-flink-ods-sync-job: Kafka -> clean/validate/dedup -> OceanBase ODS + DLQ
- rt-flink-dws-sql-job: Flink SQL temporal join + tumble window agg -> OceanBase DWS

## Constraints
- Flink JM/TM run as standalone cluster, not Spring Boot services.
- OceanBase MySQL mode used as admin/ODS/DWS unified storage.
