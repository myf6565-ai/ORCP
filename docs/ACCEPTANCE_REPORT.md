# ORCP 验收报告

本报告是 [DEV_SPEC §8.8](./DEV_SPEC.md#88-) 的证据记录。每次验收运行前，请将本文件复制为
`docs/ACCEPTANCE_REPORT-<yyyy-mm-dd>.md`，然后逐项填写。各节与规格书的五个验收门对应，
同时与 `scripts/acceptance_test.sh` 的五项检查一一对齐。

| 字段 | 值 |
|------|----|
| 日期 | _YYYY-MM-DD_ |
| 环境 | _staging / pre-prod / prod_ |
| Git SHA | _`git rev-parse HEAD`_ |
| 操作人 | _姓名_ |
| 执行命令 | _例：`scripts/acceptance_test.sh --all`_ |

---

## 验收门 1 — 正常路径

> 生产 1000 条源事件 → 在 `orcp_detail.t_order` 中可见 → 90 秒内在
> OceanBase `orcp_dw.agg_order_1min` 中可见。

- [ ] `gen_events.py` 输出 `done. total sent=1000 … failed=0`
- [ ] `SELECT COUNT(*) FROM orcp_detail.t_order` 增长 ≥ 1000
- [ ] `SELECT COUNT(*) FROM orcp_dw.agg_order_1min` 在 90 秒内增长 > 0
- [ ] 摄入 P95 处理延迟 < 200 ms

```
# 将 acceptance_test.sh 的汇总行粘贴于此
```

**结果：** PASS / FAIL / N-A

---

## 验收门 2 — TaskManager 自愈

> 杀死一个 TaskManager；120 秒内作业恢复为 RUNNING，无数据丢失、无重复
> （EXACTLY_ONCE 检查点 + JDBC upsert 主键保证）。

- [ ] 记录操作前计数：`t_order=_____`，`agg_order_1min=_____`
- [ ] 在 `TM_HOST` 上执行 `sudo systemctl restart flink-taskmanager`
- [ ] Flink 总览 120 秒内显示 `state=RUNNING`
- [ ] 操作后计数：`t_order=_____`（不变），`agg_order_1min=_____`（不变或仅有 upsert 更新）
- [ ] `agg_order_1min` 无重复行（验证：`SELECT COUNT(*), COUNT(DISTINCT (window_start, biz_type, customer_id)) FROM agg_order_1min`）

**结果：** PASS / FAIL / N-A

---

## 验收门 3 — 摄入服务重启后无重放效果

> 重启 orcp-ingest 后重新发送相同种子批次；`t_order` 不应增长
> （t_dedup + Caffeine 抑制重放）。

- [ ] 记录重启前：`t_order=_____`
- [ ] 在 `INGEST_HOST` 上执行 `sudo systemctl restart orcp-ingest`
- [ ] 以 `--seed 7 --count 50` 重新发送种子批次
- [ ] 重启后：`t_order=_____`（必须等于重启前的值）
- [ ] `grep 'already present in t_dedup' /var/log/orcp/orcp-ingest.log | wc -l` > 0

**结果：** PASS / FAIL / N-A

---

## 验收门 4 — 可观测性

> Prometheus 抓取正常，Grafana 看板有数据，`/api/health` 显示所有子系统 UP。

- [ ] `GET /api/health` 返回 200 且每个组件 `status=UP`
- [ ] `GET /jobs/overview`（Flink）显示至少一个 `RUNNING` 作业
- [ ] Grafana `orcp-overview` 看板每个面板均有非零数据
- [ ] Prometheus `/targets` 所有抓取任务显示 `up=1`：`flink`、`orcp-spring`（可选 `kafka-exporter`）

**结果：** PASS / FAIL / N-A

---

## 验收门 5 — 告警

> 停止 JobManager；3 分钟内通过配置的 Webhook 收到一条告警。
> 重启 JobManager 后确认告警已解除。

- [ ] 在 `JM_HOST` 上执行 `sudo systemctl stop flink-jobmanager`
- [ ] 180 秒内，`GET ${ALERTMANAGER_URL}/api/v2/alerts?active=true` 包含
      `alertname=OrcpFlinkJobDown`（或 `OrcpSpringServiceDown`）条目
- [ ] Webhook 接收端（钉钉 / 飞书 / Slack）收到消息
- [ ] JM 重启后，5 分钟内告警切换为 `resolved`

**结果：** PASS / FAIL / N-A

---

## 汇总

| 门 | 1 正常路径 | 2 自愈 | 3 无重放 | 4 可观测性 | 5 告警 |
|----|-----------|--------|---------|-----------|--------|
| 结果 | P / F | P / F | P / F | P / F | P / F |

**总体结论：** 发布 / 阻塞

### 待跟进事项

将发现的缺陷、不稳定行为或改进建议记录为 GitHub Issue：

1. _..._
2. _..._
