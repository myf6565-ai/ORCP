# 流计算系统 MVP

基于 **Spring Boot + Kafka + Flink** 的微服务部署模式流计算系统最小可用平台（MVP）。

本项目目标是构建一套**可编译、可启动、可验证、可部署**的流计算平台基础工程，用于后续逐步演进到生产级实时数据处理系统。

---

## 1. 项目简介

本项目采用以下技术栈：

- **Spring Boot**：承载接入服务、配置服务、查询服务、运维服务等微服务能力
- **Kafka**：作为事件总线，负责消息接入、解耦、缓冲与流转
- **Flink**：作为核心流计算引擎，负责实时清洗、聚合、规则匹配与结果输出
- **Docker Compose**：用于本地最小环境快速启动
- **Kubernetes**：用于生产环境部署与扩展

本项目当前实现的是 **MVP（最小可用平台）**，优先完成核心数据闭环，而不是一次性建设成完整的大平台。

---

## 2. MVP 目标

当前版本聚焦以下最小能力：

- 事件接入
- Kafka 消息流转
- Flink 实时清洗
- Flink 规则匹配/聚合
- 结果查询
- 本地开发环境
- Kubernetes 部署骨架
- 基础测试与文档

系统最小数据闭环如下：

```text
外部请求
  -> ingest-service
  -> Kafka raw-events
  -> event-cleaning-job
  -> Kafka standard-events
  -> rule-aggregation-job
  -> result sink / result-events
  -> query-service 查询结果
```

---

## 3. 项目目录结构

```text
/
  README.md
  pom.xml
  /docs
    architecture.md
    mvp-scope.md
    topic-design.md
    assumptions.md
  /services
    /gateway-service
    /ingest-service
    /config-service
    /query-service
    /ops-service
  /jobs
    /event-cleaning-job
    /rule-aggregation-job
  /shared
    /common-model
    /common-kafka
    /common-web
  /infra
    /docker-compose
    /k8s
    /scripts
  /examples
    /requests
    /events
```

---

## 4. 模块说明

### 4.1 services

#### gateway-service
统一入口服务，提供网关或路由转发能力，并预留统一鉴权能力。

#### ingest-service
事件接入服务，接收外部 HTTP 请求，对事件做基础校验、标准化处理，并写入 Kafka 原始 Topic。

#### config-service
配置与规则服务，管理 Topic 映射、规则配置、作业元数据和系统基础配置。

#### query-service
查询服务，对外提供实时计算结果的查询接口。

#### ops-service
运维服务，提供健康检查、作业状态查询、回放任务骨架、审计接口等能力。

### 4.2 jobs

#### event-cleaning-job
Flink 实时清洗作业，从 `raw-events` Topic 消费，进行字段清洗和标准化，输出到 `standard-events`。

#### rule-aggregation-job
Flink 实时规则或聚合作业，从 `standard-events` Topic 消费，进行规则命中、窗口统计或简单聚合，并输出结果。

### 4.3 shared

#### common-model
公共数据模型、事件定义、DTO、VO 等。

#### common-kafka
Kafka 公共配置、Producer/Consumer 封装、Topic 常量管理等。

#### common-web
通用 Web 层能力，如统一响应、异常处理、拦截器、工具类等。

### 4.4 infra

#### docker-compose
本地依赖环境与最小运行环境定义。

#### k8s
Kubernetes 部署清单，包括 Spring Boot 服务与 Flink 作业示例。

#### scripts
本地启动、Topic 初始化、构建与部署辅助脚本。

### 4.5 examples

#### requests
接口调用示例。

#### events
示例事件数据。

---

## 5. 技术栈说明

| 类别 | 技术 |
|---|---|
| 后端框架 | Spring Boot |
| 消息总线 | Kafka |
| 流计算引擎 | Flink |
| 构建工具 | Maven |
| 本地环境 | Docker Compose |
| 容器编排 | Kubernetes |
| 文档接口 | OpenAPI / Swagger |
| 健康检查 | Spring Boot Actuator |

---

## 6. 核心数据流说明

### 6.1 事件接入
外部系统通过 HTTP 调用 `ingest-service` 的接入接口，发送业务事件。

### 6.2 原始事件入 Kafka
`ingest-service` 将基础校验后的事件写入 Kafka 原始 Topic，例如：

- `raw-events`

### 6.3 实时清洗
`event-cleaning-job` 从 `raw-events` 读取消息，执行数据标准化、补齐默认字段、过滤非法数据，并输出到：

- `standard-events`

### 6.4 规则匹配/聚合
`rule-aggregation-job` 从 `standard-events` 读取消息，进行规则命中、计数聚合或窗口统计，并将结果输出到：

- `result-events`
- 或结果存储（如数据库/缓存）

### 6.5 查询结果
`query-service` 对外提供结果查询接口，返回指定业务键的实时计算结果。

---

## 7. Topic 设计

当前 MVP 默认使用以下 Topic：

| Topic 名称 | 用途 |
|---|---|
| `raw-events` | 原始接入事件 |
| `standard-events` | 清洗标准化后的事件 |
| `result-events` | 计算结果事件 |
| `dead-letter-events` | 死信事件（可选） |

详细设计请参考：

- `docs/topic-design.md`

---

## 8. 环境要求

建议环境：

- JDK 17 或以上
- Maven 3.8+
- Docker
- Docker Compose
- Kubernetes 1.25+（生产部署时）
- 可用的 Kafka / Flink 运行环境

---

## 9. 本地开发启动步骤

### 9.1 克隆代码

```bash
git clone <your-repo-url>
cd <your-repo-name>
```

### 9.2 构建项目

```bash
mvn clean install
```

### 9.3 启动本地依赖环境

```bash
cd infra/docker-compose
docker compose up -d
```

### 9.4 初始化 Kafka Topic

```bash
cd ../scripts
sh init-topics.sh
```

### 9.5 启动 Spring Boot 服务

按需分别启动以下服务：

```bash
mvn -pl services/ingest-service spring-boot:run
mvn -pl services/query-service spring-boot:run
mvn -pl services/config-service spring-boot:run
```

如已实现网关与运维服务，也可启动：

```bash
mvn -pl services/gateway-service spring-boot:run
mvn -pl services/ops-service spring-boot:run
```

### 9.6 启动 Flink 作业

根据实际实现方式启动：

```bash
mvn -pl jobs/event-cleaning-job clean package
mvn -pl jobs/rule-aggregation-job clean package
```

提交作业到本地或目标 Flink 集群。

---

## 10. Docker Compose 启动说明

本地最小依赖环境位于：

- `infra/docker-compose/docker-compose.yml`

启动命令：

```bash
docker compose -f infra/docker-compose/docker-compose.yml up -d
```

停止命令：

```bash
docker compose -f infra/docker-compose/docker-compose.yml down
```

查看日志：

```bash
docker compose -f infra/docker-compose/docker-compose.yml logs -f
```

---

## 11. 配置说明

项目采用外部化配置方式。

建议至少区分以下配置文件：

- `application.yml`
- `application-dev.yml`
- `application-prod.yml`（如有）
- Kubernetes ConfigMap / Secret
- 环境变量注入

配置内容通常包括：

- Kafka 地址
- Topic 名称
- Consumer Group
- Flink 并行度
- Checkpoint / Savepoint 配置
- 结果存储配置
- 服务端口
- 日志级别

敏感信息禁止硬编码在代码中。

---

## 12. 接口示例

### 12.1 接入事件

接口：

```http
POST /api/events
Content-Type: application/json
```

请求示例：

```json
{
  "eventId": "evt-10001",
  "bizKey": "user-001",
  "eventType": "ORDER_CREATED",
  "eventTime": "2026-03-25T10:00:00Z",
  "payload": {
    "amount": 120.50,
    "currency": "CNY"
  }
}
```

示例命令：

```bash
curl -X POST http://localhost:8081/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "evt-10001",
    "bizKey": "user-001",
    "eventType": "ORDER_CREATED",
    "eventTime": "2026-03-25T10:00:00Z",
    "payload": {
      "amount": 120.50,
      "currency": "CNY"
    }
  }'
```

### 12.2 查询结果

接口：

```http
GET /api/results/{key}
```

示例命令：

```bash
curl http://localhost:8083/api/results/user-001
```

响应示例：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "bizKey": "user-001",
    "ruleMatched": true,
    "count": 3,
    "lastEventTime": "2026-03-25T10:01:20Z"
  }
}
```

---

## 13. 健康检查与可观测性

所有 Spring Boot 服务必须暴露 Actuator 健康检查接口，例如：

```http
GET /actuator/health
```

建议同时暴露：

- `/actuator/info`
- `/actuator/metrics`

后续可对接：

- Prometheus
- Grafana
- 日志平台
- Trace 系统

---

## 14. 测试说明

执行全部测试：

```bash
mvn test
```

执行指定模块测试示例：

```bash
mvn -pl services/ingest-service test
mvn -pl jobs/event-cleaning-job test
```

建议至少覆盖：

- Spring Boot 接口基础测试
- Kafka 消息发送测试
- Flink 作业最小单元测试
- 关键工具类测试

如某些集成测试依赖 Kafka/Flink 环境，请先启动本地依赖环境。

---

## 15. Kubernetes 部署说明

Kubernetes 部署文件位于：

- `infra/k8s/`

建议包含：

- Spring Boot 服务 Deployment
- Service
- ConfigMap
- Secret
- Flink Job 或 FlinkDeployment 示例
- 命名空间与资源配额示例

示例部署命令：

```bash
kubectl apply -f infra/k8s/
```

如按模块拆分部署，可分别执行：

```bash
kubectl apply -f infra/k8s/services/
kubectl apply -f infra/k8s/jobs/
```

---

## 16. 文档索引

详细文档请参考：

- `docs/architecture.md`
- `docs/mvp-scope.md`
- `docs/topic-design.md`
- `docs/assumptions.md`

---

## 17. 当前版本边界

### 当前已实现
- [在此填写]
- [在此填写]
- [在此填写]

### 当前未实现
- [在此填写]
- [在此填写]
- [在此填写]

### 已知风险
- [在此填写]
- [在此填写]

### 下一阶段建议
- 引入规则中心持久化
- 增加回放补偿能力
- 增加死信处理链路
- 完善监控与告警
- 增强端到端测试与压测能力

---

## 18. 参与开发说明

开发时请遵循仓库根目录下的：

- `AGENTS.md`

该文件定义了本仓库的开发约束、实现范围、交付标准和文档要求。

---

## 19. License

根据项目实际情况填写，例如：

```text
版权所有 © 本项目团队
```

或：

```text
MIT License
```
