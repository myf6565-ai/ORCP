# Development Notes

## Build
mvn clean package

## Logging fields
trace_id, job_name, topic, partition, offset

## Retry/DLQ/Replay
- Retry: Flink JDBC sink maxRetries=3
- DLQ: invalid records go to rt.biz.events.dlq
- Replay: call POST /replay with offsets range
