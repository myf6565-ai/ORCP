# ORCP Acceptance Report

Evidence log for [DEV_SPEC §8.8](./DEV_SPEC.md#88-). Copy this file to
`docs/ACCEPTANCE_REPORT-<yyyy-mm-dd>.md` for each acceptance run, then
fill in. The sections mirror the five gates in the spec and match the
five checks in `scripts/acceptance_test.sh`.

| Field       | Value                        |
|-------------|------------------------------|
| Date        | _YYYY-MM-DD_                 |
| Environment | _staging / pre-prod / prod_  |
| Git SHA     | _`git rev-parse HEAD`_       |
| Operator    | _name_                       |
| Run command | _e.g. `scripts/acceptance_test.sh --all`_ |

---

## Gate 1 — Happy path

> 1000 source events produced → visible in `orcp_detail.t_order` → visible
> in OceanBase `orcp_dw.agg_order_1min` within 90 seconds.

- [ ] `gen_events.py` reported `done. total sent=1000 … failed=0`
- [ ] `SELECT COUNT(*) FROM orcp_detail.t_order` grew by ≥ 1000
- [ ] `SELECT COUNT(*) FROM orcp_dw.agg_order_1min` grew > 0 within 90s
- [ ] P95 ingest processing latency remained < 200 ms

```
# paste the summary line from acceptance_test.sh here
```

**Result:** PASS / FAIL / N-A

---

## Gate 2 — TaskManager self-heal

> Kill a TaskManager; within 120 s the job returns to RUNNING with no
> records lost and no duplicates (checkpointed EXACTLY_ONCE + JDBC upsert
> on primary key).

- [ ] Record pre-kill counts: `t_order=_____`, `agg_order_1min=_____`
- [ ] `sudo systemctl restart flink-taskmanager` on `TM_HOST`
- [ ] Flink overview shows `state=RUNNING` within 120 s
- [ ] Post-kill counts: `t_order=_____` (unchanged), `agg_order_1min=_____` (unchanged or updated upserts only)
- [ ] No duplicate rows in `agg_order_1min` (check via
      `SELECT COUNT(*), COUNT(DISTINCT (window_start, biz_type, customer_id)) FROM agg_order_1min`)

**Result:** PASS / FAIL / N-A

---

## Gate 3 — Ingest restart has no replay effect

> Bounce orcp-ingest and re-send the same seeded batch; `t_order` must
> not grow (t_dedup + Caffeine suppress the replay).

- [ ] Record pre-restart: `t_order=_____`
- [ ] `sudo systemctl restart orcp-ingest` on `INGEST_HOST`
- [ ] Re-sent seeded batch with `--seed 7 --count 50`
- [ ] Post-restart: `t_order=_____` (must equal pre-restart)
- [ ] `grep 'already present in t_dedup' /var/log/orcp/orcp-ingest.log | wc -l` > 0

**Result:** PASS / FAIL / N-A

---

## Gate 4 — Observability

> Prometheus scraping works, Grafana dashboards light up, `/api/health`
> reports all subsystems UP.

- [ ] `GET /api/health` returns 200 with `status=UP` for every component
- [ ] `GET /jobs/overview` (Flink) shows at least one `RUNNING` job
- [ ] Grafana `orcp-overview` dashboard shows non-zero values on every panel
- [ ] Prometheus `/targets` lists all scrape jobs as `up=1`:
      `flink`, `orcp-spring`, (optionally `kafka-exporter`)

**Result:** PASS / FAIL / N-A

---

## Gate 5 — Alerting

> Stop the JobManager. One alert fires through the configured webhook
> within 3 minutes. Restart the JobManager and confirm the alert resolves.

- [ ] `sudo systemctl stop flink-jobmanager` on `JM_HOST`
- [ ] Within 180 s, `GET ${ALERTMANAGER_URL}/api/v2/alerts?active=true`
      contains an entry with `alertname=OrcpFlinkJobDown` (or
      `OrcpSpringServiceDown`)
- [ ] Webhook receiver (dingtalk/feishu/slack) received the message
- [ ] After JM restart, alert transitions to `resolved` within 5 minutes

**Result:** PASS / FAIL / N-A

---

## Summary

| Gate | 1 happy | 2 self-heal | 3 no-dup | 4 observability | 5 alerts |
|------|---------|-------------|----------|-----------------|----------|
| Result | P / F | P / F | P / F | P / F | P / F |

**Overall:** RELEASE / BLOCK

### Follow-ups

Record any defects, flaky behaviour, or improvements to file as GitHub
issues:

1. _..._
2. _..._
