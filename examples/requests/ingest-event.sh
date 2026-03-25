curl -X POST http://localhost:8081/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "evt-10001",
    "bizKey": "user-001",
    "eventType": "ORDER_CREATED",
    "eventTime": "2026-03-25T10:00:00Z",
    "payload": {"amount": 120.5, "currency": "CNY"}
  }'
