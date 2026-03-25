# AGENTS.md

## 目标

在本仓库中实现一套“最小可用平台（MVP）”级别、可向生产演进的流计算系统，技术栈为：

- Spring Boot
- Kafka
- Flink
- Docker Compose
- Kubernetes

目标是交付**可编译、可启动、可验证、可部署**的项目，而不是只输出设计说明。

---

## 总体工作原则

1. 优先满足 MVP，不做与当前目标无关的扩展。
2. 所有实现必须服务于以下最小闭环：

   外部请求 -> ingest-service -> Kafka raw topic -> Flink cleaning job -> Kafka standard topic -> Flink aggregation job -> result sink -> query-service 查询结果

3. 除非仓库已有明确约束，否则默认采用 **Java + Maven + Monorepo**。
4. 遇到不确定项时，优先采用**合理默认值继续推进**，并把假设记录到 `docs/assumptions.md`。
5. 不要反复停下来提问；除非存在真正阻塞开发的问题，否则应继续实现。
6. 优先复用仓库已有代码和结构，不要无意义重写。
7. 不要为了“看起来完整”而引入重型组件或过度设计。

---

## 范围约束

### 当前必须实现
- Spring Boot 微服务骨架与关键接口
- Kafka 消息生产与消费基础能力
- 2 个可运行的 Flink 作业
- 本地开发环境
- Kubernetes 部署清单骨架
- 基础测试
- 清晰的中文文档

### 当前不要实现
- 复杂前端界面
- 多租户平台
- 大而全的权限系统
- 复杂规则编辑器 UI
- 大规模调度编排系统
- 非必要的服务网格或复杂云原生组件

---

## 推荐目录结构

如果当前仓库没有成熟结构，优先采用以下组织方式：

```text
/
  README.md
  pom.xml
  /docs
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

## 服务与模块要求

### Spring Boot 服务

#### gateway-service
- 作为统一入口或网关占位层
- 负责基础路由转发或预留统一鉴权能力
- 必须暴露健康检查

#### ingest-service
- 提供事件接入接口
- 至少实现：
  - `POST /api/events`
- 接收 JSON 事件后进行基础校验、标准化封装
- 写入 Kafka `raw-events` topic
- 幂等键能力可先预留结构，不要求完整实现

#### config-service
- 提供规则、Topic 映射、作业元数据、系统配置的基础 REST API
- MVP 可先以内存或轻量存储方式实现
- 要有清晰的数据模型定义

#### query-service
- 提供结果查询接口
- 至少实现：
  - `GET /api/results/{key}`
- 返回 Flink 计算结果或其落地结果

#### ops-service
- 提供作业状态、回放任务、健康检查、审计记录的轻量接口或骨架
- MVP 阶段允许只实现最小接口与数据结构

---

## Flink 作业要求

### event-cleaning-job
- 从 Kafka `raw-events` 消费
- 完成字段清洗、标准化、基础补齐
- 输出到 Kafka `standard-events`

### rule-aggregation-job
- 从 Kafka `standard-events` 消费
- 实现最小可运行规则命中、计数聚合、窗口统计或简单状态计算
- 输出到结果 Topic 或结果存储

### Flink 编码要求
- 必须清晰划分 source / process / sink
- topic、groupId、并行度等必须可配置
- checkpoint、savepoint、state backend 相关参数必须预留配置入口
- 不允许写成只适合 demo 的硬编码脚本式代码

---

## Kafka 相关要求

1. 所有 Kafka 连接参数必须外部化配置。
2. topic 名称不得硬编码散落在业务代码中，应集中管理。
3. Spring Boot 侧 Kafka 操作必须有清晰抽象，不要把 producer/consumer 逻辑直接散落在 controller 或 service 里。
4. 至少定义以下 Topic：
   - `raw-events`
   - `standard-events`
   - `result-events`
   - 可选：`dead-letter-events`
5. 必须提供 topic 初始化脚本或说明。

---

## 配置要求

1. 所有环境相关配置必须外部化。
2. 至少区分：
   - 本地开发配置
   - Docker/容器配置
   - Kubernetes 配置
3. 敏感信息必须通过环境变量、Secret 或示例占位处理，不得硬编码真实密钥。
4. 必须提供：
   - `application.yml`
   - `application-dev.yml`
   - 环境变量样例
   - ConfigMap / Secret 示例

---

## 基础设施要求

### Docker Compose
必须提供本地开发环境，至少包含：
- Kafka
- 必要依赖组件
- 可选轻量数据库或 MinIO（仅在确有必要时引入）

### Kubernetes
必须提供：
- Spring Boot 服务的 Deployment、Service、ConfigMap、Secret 示例
- Flink 作业部署清单
- 部署说明文档

### 脚本
必须提供：
- 本地启动脚本
- topic 初始化脚本
- 构建脚本
- 部署辅助脚本

---

## 代码质量要求

1. 代码风格统一，包名、模块名、配置名保持一致。
2. 公共模型、公共 Kafka 能力、公共 Web 能力要下沉到 shared 模块，避免重复实现。
3. 不要生成无实际用途的空壳类。
4. 关键类必须有清晰命名，不使用含糊命名。
5. 异常处理要统一，不要在接口层直接抛出杂乱异常。
6. 日志要有基本结构化意识，至少保证关键流程可追踪。
7. 所有 README 和文档必须使用中文，必要术语可保留英文。

---

## 可观测性要求

所有 Spring Boot 服务必须具备：
- Actuator 健康检查
- 基础指标暴露
- 基础日志配置

所有关键模块应具备：
- 失败日志
- 启动日志
- 核心数据流转日志

若时间允许，可补充：
- Prometheus 指标集成占位
- Trace 相关依赖预留

---

## 测试要求

至少提供以下测试：

1. Spring Boot 服务基础单元测试
2. Kafka 交互的基础测试或示例测试
3. Flink 作业最小测试
4. 至少一个端到端演示流程说明

要求：
- 测试要能说明关键路径是可工作的
- 不要求过度追求覆盖率，但不能完全没有测试
- 若某类集成测试因环境限制无法完整运行，需要在文档中明确说明如何运行

---

## 文档要求

必须生成并维护以下文档：

### 根目录
- `README.md`
  - 项目简介
  - 模块说明
  - 技术栈说明
  - 本地启动步骤
  - 构建步骤
  - 测试步骤
  - docker-compose 启动步骤
  - k8s 部署说明
  - 演示请求示例

### docs 目录
- `docs/architecture.md`
  - 总体架构图
  - 服务职责
  - 数据流
  - 部署拓扑
- `docs/mvp-scope.md`
  - MVP 范围
  - 暂不实现内容
  - 后续演进建议
- `docs/topic-design.md`
  - Topic 设计
  - 消息结构示例
  - 生产/消费关系
- `docs/assumptions.md`
  - 合理默认假设
  - 未决项说明

架构图可以使用 Mermaid。

---

## 实施方式要求

执行任务时遵循以下流程：

1. 先检查当前仓库结构和已有文件。
2. 先输出一个简洁实施计划。
3. 按阶段实施，每个阶段都要说明：
   - 改了什么
   - 为什么这样设计
   - 还有什么未完成
4. 每个阶段结束时列出已修改文件。
5. 完成后必须自行验证，而不是只说“理论可行”。

---

## 完成标准

只有满足以下条件，才算完成：

1. 项目可以成功构建。
2. docker-compose 可以启动最小依赖环境。
3. 至少 3 个 Spring Boot 服务可以成功启动。
4. 至少 2 个 Flink 作业具备可运行实现。
5. 样例事件可以从接入接口进入 Kafka，并经过 Flink 处理后被查询接口读取。
6. 所有关键模块有 README 或文档说明。
7. 关键模块具备基础测试。
8. 最终输出交付总结，说明：
   - 完成项
   - 未完成项
   - 已知风险
   - 下一阶段建议

---

## 输出偏好

在与用户交互时：
- 先给计划，再实施
- 描述尽量简洁
- 重点解释关键设计决策
- 不重复无意义的过程描述
- 对未完成内容保持明确和诚实

---

## 禁止事项

1. 禁止为凑完整度引入无必要的重量级中间件。
2. 禁止把生产配置硬编码在代码里。
3. 禁止只生成文档不生成可运行代码。
4. 禁止大面积生成空实现、伪代码或无法构建的骨架。
5. 禁止忽略测试、部署文件和运行说明。
6. 禁止随意改变既有成熟结构，除非确有必要并说明原因。
