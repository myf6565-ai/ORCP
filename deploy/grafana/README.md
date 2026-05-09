# Grafana 看板

## orcp-overview-dashboard.json

端到端概览看板，覆盖 ORCP 流水线自身的应用层指标。
共 7 个面板，双列布局：

1. 摄入吞吐：每秒 received / persisted / duplicate 数量。
2. 摄入解析错误：每分钟解析失败次数。
3. Flink 作业状态（Stat 面板）：当前 uptime，按作业名称分组。
4. Flink 重启次数（最近 5 分钟）。
5. 最近检查点耗时。
6. JVM 堆内存用量（按 Spring 实例分组）。
7. 摄入处理时间 P95。

### 导入方式

1. Grafana UI：Dashboards → Import → 粘贴 `orcp-overview-dashboard.json` 内容。
2. 选择提示中出现的 Prometheus 数据源。
3. 保存。

### 刻意省略的内容

- 算子级背压：请使用上游 Flink 看板（Grafana ID 14911）。本看板定位于
  **应用层信号**的单一视图，不替代 Flink 内部监控。
- OceanBase 侧指标：OB 有自己的 Prometheus 端点，应放在独立的 OB 看板中，
  而不是混入应用视图。
- 告警覆盖层：Alertmanager 的 `:9093/#/alerts` 是告警状态的权威来源；
  Grafana 面板反映原始指标值，而非告警状态。
