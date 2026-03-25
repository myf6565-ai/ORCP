# MVP 范围说明

## 1. 当前范围（已实现）

- 多模块 Maven Monorepo 骨架。
- 5 个 Spring Boot 服务最小可运行实现。
- 2 个 Flink 作业（清洗 + 聚合）可打包运行。
- Kafka Topic 统一配置与发送抽象。
- Docker Compose、K8s 清单骨架、脚本。
- 基础单元测试与接口测试样例。

## 2. 暂不实现

- 多租户权限模型。
- 复杂规则编辑 UI。
- 大规模调度编排系统。
- 服务网格与复杂云原生附加组件。

## 3. 后续演进建议

- 引入规则配置持久化（MySQL/Redis）。
- 引入 Flink State 后端与 checkpoint 存储。
- 完善死信链路与重放能力。
- 增加 Prometheus + Grafana 可观测性。
