# Topic 设计

## 1. Topic 列表

| Topic | 说明 | 生产者 | 消费者 |
|---|---|---|---|
| raw-events | 原始接入事件 | ingest-service | event-cleaning-job |
| standard-events | 清洗标准化事件 | event-cleaning-job | rule-aggregation-job |
| result-events | 聚合结果事件 | rule-aggregation-job | query-service |
| dead-letter-events | 死信事件（预留） | 各模块（可选） | 运维排障链路 |

## 2. 消息结构示例

### raw-events / standard-events

```json
{
  "eventId": "evt-10001",
  "bizKey": "user-001",
  "eventType": "ORDER_CREATED",
  "eventTime": "2026-03-25T10:00:00Z",
  "ingestTime": "2026-03-25T10:00:01Z",
  "source": "ingest-service",
  "payload": {
    "amount": 120.5,
    "currency": "CNY"
  }
}
```

### result-events

```json
{
  "bizKey": "user-001",
  "ruleMatched": true,
  "count": 3,
  "lastEventTime": "2026-03-25T10:01:20Z",
  "updatedAt": "2026-03-25T10:01:21Z"
}
```
