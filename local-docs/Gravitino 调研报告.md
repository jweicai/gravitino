# Gravitino 调研报告

## 1. 调研背景

随着企业数据平台逐步从单一数仓向湖仓一体、多引擎、多地域和多资产类型演进，传统仅面向单一元数据系统的方案越来越难以支撑统一治理诉求。尤其在以下场景中，元数据平台面临明显挑战：

- 数据资产类型持续扩展，不再只有表，还包括文件集、消息 Topic、模型等对象。
- 查询与计算引擎多样化，Spark、Flink、Trino 等引擎并存。
- 安全治理要求提高，需要统一认证、鉴权、策略、审计与血缘能力。
- 数据分布在不同存储、不同区域、不同云环境中，元数据访问需要具备联邦化特征。

Apache Gravitino 试图解决的正是这一类问题。它并不只是一个传统意义上的元数据服务，而是一个面向异构数据与 AI 资产的统一元数据与治理控制面。

## 2. 调研目标

本次调研主要回答以下问题：

- Gravitino 的产品定位和架构边界是什么。
- 它能解决哪些典型场景问题。
- 它与传统 Metastore 或单一 Catalog 方案相比的优势和限制是什么。
- 在企业内部落地时，应采用怎样的实施路径。
- 是否值得进入 PoC 或试点阶段，以及推荐的推进方式。

## 3. 结论摘要

### 3.1 总体结论

Gravitino 适合被定位为统一元数据治理控制面，而不是单一元数据后端替代品。它的核心价值在于：

- 用一致的对象模型管理多种资产类型。
- 用统一 API 和连接器对接多种执行引擎。
- 在元数据目录能力之外，逐步叠加认证、授权、策略、血缘和维护任务能力。

从仓库结构和核心代码来看，Gravitino 已经形成较完整的平台化能力版图，覆盖 server、catalog、client、engine connector、lineage、optimizer、Iceberg REST、Lance REST 等多个方面，具备作为平台中枢演进的技术基础。

### 3.2 适用判断

强烈建议关注或试点的场景：

- 企业内部存在多个异构元数据源，需要统一访问入口。
- 同时使用 Spark、Flink、Trino 等多个计算引擎。
- 希望对表、文件、消息、模型等多类型资产进行统一治理。
- 计划建设湖仓平台、数据中台或数据治理中台。

不建议直接重度投入的场景：

- 当前只有单一 Hive Metastore 或单一数据库元数据管理诉求。
- 组织内没有专门的平台工程能力支撑 connector、权限、兼容性与运维治理。
- 希望快速引入一个极简、低维护、低学习成本的轻量 catalog 服务。

## 4. 产品定位分析

根据项目说明，Gravitino 将自己定义为高性能、地理分布式、联邦化的 metadata lake，用于统一管理不同来源、不同类型、不同区域的数据与 AI 资产元数据。

它的关键特点不是“把所有业务元数据复制进一套新系统”，而是通过 connector 机制与各类外部元数据系统直接集成，并在上层提供一致的抽象模型与 API。

从抽象模型上看，Gravitino 的顶层对象为 `Metalake`，其下包含多个 `Catalog`，而 Catalog 又细分为不同类型。这意味着它更像一个统一元数据命名空间与治理框架，而不是单纯的元数据库。

### 4.1 顶层对象模型

从 API 设计可以看出，Gravitino 的核心层级是：

- Metalake：顶层管理单元。
- Catalog：资产域或接入源。
- Schema：Catalog 下的逻辑命名空间。
- Metadata Object：具体资产对象。

其中可被治理的对象不仅包含 `TABLE`，还包括：

- `FILESET`
- `TOPIC`
- `MODEL`

这说明 Gravitino 从设计上就是多资产元数据平台，而不是只为关系型表服务。

### 4.2 Catalog 类型

Gravitino 当前定义的 Catalog 类型包括：

- `RELATIONAL`
- `FILESET`
- `MESSAGING`
- `MODEL`

这类设计直接体现出它试图统一覆盖数据库、文件系统、对象存储、消息系统与模型注册场景。

## 5. 架构分析

## 5.1 整体架构特征

从源码结构看，Gravitino 采用典型的平台化分层设计：

- `api`：统一抽象接口与对象模型。
- `core`：核心逻辑实现与内部能力。
- `server`：REST API 服务入口与服务编排。
- `catalogs`：不同元数据源的适配器实现。
- `clients`：Java、Python、CLI、文件系统等客户端。
- `trino-connector`、`spark-connector`、`flink-connector`：引擎接入层。
- `iceberg`、`lance`：特定生态的 REST 服务支持。
- `lineage`：血缘能力。
- `maintenance`：维护任务、Optimizer 与作业编排能力。

这说明 Gravitino 不只是一个服务端，而是一个覆盖元数据控制面、接入面和运维面的产品族。

## 5.2 服务端架构

服务端主入口位于 `server/src/main/java/org/apache/gravitino/server/GravitinoServer.java`。

从初始化流程看，Server 的关键职责包括：

- 初始化 Gravitino 环境与全量组件。
- 初始化 Jetty Web Server。
- 初始化认证器和鉴权提供方。
- 初始化 Lineage 服务。
- 装配并暴露 REST API。
- 注册 HTTP 指标、UI 过滤器、版本过滤器和系统过滤器。

Server 通过 Jersey + Jetty 暴露 API，并通过 Dispatcher 向外统一提供不同对象域的操作入口。已装配的 Dispatcher 包括：

- MetalakeDispatcher
- CatalogDispatcher
- SchemaDispatcher
- TableDispatcher
- PartitionDispatcher
- FilesetDispatcher
- TopicDispatcher
- TagDispatcher
- PolicyDispatcher
- CredentialOperationDispatcher
- ModelDispatcher
- FunctionDispatcher
- LineageDispatcher
- JobOperationDispatcher
- StatisticDispatcher

这说明 Gravitino 的 server 更像一个统一编排入口，负责聚合多类元数据与治理能力。

## 5.3 元数据存储模式

默认配置显示，Gravitino 自身实体存储采用关系型后端，默认示例为 JDBC + H2：

- `gravitino.entity.store = relational`
- `gravitino.entity.store.relational = JDBCBackend`

这表明：

- Gravitino 自身的系统元数据、管理对象与治理状态会保存在关系型后端中。
- 业务元数据本身则通过 catalog 直接对接外部系统。

因此，Gravitino 更适合被理解为控制面元数据系统，而不是把业务数据系统完全替换掉的“单一真实存储”。

## 5.4 缓存与性能设计

默认配置中存在较完整的缓存能力：

- Catalog 缓存驱逐周期。
- Entity Cache 开关。
- 最大缓存条目数。
- 过期时间。
- 加权淘汰策略。
- 缓存统计开关。

这说明项目设计者已经考虑到联邦化元数据查询的性能问题。对于多 catalog、多对象、大量读请求场景，缓存会是影响可用性的重要机制。

## 6. 能力分析

## 6.1 统一元数据管理

这是 Gravitino 的核心价值点。

统一管理主要体现在以下方面：

- 统一命名与对象层级模型。
- 统一 REST API。
- 统一 Java / Python Client。
- 统一 Web UI。
- 统一多类对象的治理能力挂载方式。

相比每个系统各自维护 catalog、权限和访问入口，Gravitino 的意义在于把“管理平面”抽到上层统一化。

## 6.2 多类型 Catalog 支持

从仓库结构可以直接看到内置或官方维护的 Catalog 范围较广，包括但不限于：

- Hive Catalog
- Iceberg Catalog
- Paimon Catalog
- Hudi Catalog
- Generic Lakehouse Catalog
- JDBC Catalog（MySQL、PostgreSQL、Doris、StarRocks 等）
- Kafka Catalog
- Fileset Catalog
- Model Catalog

此外还有 contrib 模块，例如 ClickHouse、Hologres、OceanBase 等，体现出一定的扩展性。

## 6.3 多引擎对接能力

Gravitino 在执行引擎接入方面投入较多，仓库内直接提供：

- Trino Connector
- Spark Connector
- Flink Connector

其中 Trino Connector 针对不同版本做了显式分段模块维护，Spark 也按 3.3、3.4、3.5 区分，这从一个侧面说明：

- 项目对真实引擎兼容问题是认真处理的。
- 但也意味着生产中必须重视版本矩阵和升级治理。

## 6.4 安全能力

Gravitino 的安全能力至少覆盖了两个层面：

- 认证
- 授权

从默认配置看，授权默认关闭；但文档中已经给出较完整的 OAuth/OIDC 认证配置方式，包括：

- OIDC Provider
- Azure AD
- JWKS Token Validation
- Static Key OAuth Provider

在企业场景中，这意味着它具备与统一身份系统集成的能力，但真正落地时仍需完成企业用户、组、角色与引擎侧权限的映射设计。

## 6.5 Policy 治理能力

从文档看，Gravitino 从 1.0.0 开始引入政策系统，支持对元数据对象关联策略。

其治理特点包括：

- 支持对 `CATALOG`、`SCHEMA`、`TABLE`、`FILESET`、`TOPIC`、`MODEL` 绑定策略。
- 支持内置策略和自定义策略。
- 支持策略继承。
- 支持父子对象策略合并展示。

这类能力对于企业内部建立“分层治理规则”非常有价值，例如：

- 在 Catalog 层定义基础治理要求。
- 在 Schema 层细化到业务域。
- 在 Table 或 Fileset 层做例外配置。

## 6.6 Lineage 血缘能力

Lineage 模块是 Gravitino 另一个很有潜力的能力点。

项目文档显示，Gravitino 提供可插拔的 lineage framework，用于接收、处理并输出 OpenLineage 事件。其主要特点：

- Source、Processor、Sink 可插拔。
- 支持 HTTP Source。
- 支持将事件输出到日志或 HTTP Sink。
- 支持和外部 OpenLineage 系统对接。

这意味着 Gravitino 并不是封闭式血缘系统，而是可作为企业血缘链路中的一环，与现有生态做集成。

## 6.7 表维护与 Optimizer

`maintenance` 模块说明 Gravitino 正在向“治理动作执行平台”演进。

Optimizer 的工作流包括：

1. 收集统计和指标。
2. 评估规则并生成候选动作。
3. 选择任务模板并提交作业。
4. 追踪状态与结果。

这说明 Gravitino 不只是“登记元数据”，而是希望进一步承载表维护的策略化编排，例如：

- Iceberg compaction
- 基于规则的维护任务提交
- 作业状态追踪与日志观察

对于湖仓平台，这种能力非常有现实意义。

## 7. 与传统方案相比的价值

## 7.1 相对 Hive Metastore

与 Hive Metastore 相比，Gravitino 的优势主要在于：

- 不只服务于 Hive/Spark 传统表元数据。
- 支持更多对象类型。
- 具备更明确的平台治理能力扩展方向。
- 对多引擎接入更加友好。
- 有更明确的安全、血缘、维护任务扩展面。

但它的代价也更明显：

- 复杂度更高。
- 学习成本更高。
- 运维边界更广。
- 生态兼容测试要求更高。

## 7.2 相对单一 Catalog 服务

如果拿单一场景的 Catalog 服务做比较，Gravitino 的优势在于统一治理平面，而不是在某一个单点功能上做到极致。

因此，在选型上要避免误判：

- 如果你只需要一个引擎下的单一 catalog，Gravitino 可能过重。
- 如果你需要统一多源、多引擎、多对象治理，它的综合价值会更高。

## 8. 风险与挑战分析

## 8.1 统一抽象与底层差异的矛盾

虽然 Gravitino 暴露统一 API，但不同 catalog 的底层能力并不一致。例如：

- 某些 catalog 天然支持丰富 schema/table 能力。
- 某些 fileset 或 messaging catalog 在对象语义上与 relational 并不等价。
- 某些操作在一类 catalog 中可变更，在另一类 catalog 中可能只读或不支持。

因此，统一模型带来的最大挑战之一是能力语义对齐。

## 8.2 权限治理的真实落地复杂度

Gravitino 可以提供策略、角色和认证能力，但企业里真正生效的访问控制通常涉及：

- IdP / SSO
- 对象存储 ACL
- 引擎侧权限体系
- Ranger / IAM / 自定义鉴权系统

因此，Gravitino 更适合作为治理中枢，而不是自动解决所有权限问题的万能方案。

## 8.3 兼容性治理成本

从工程结构看，Trino 和 Spark 等引擎适配有显式版本拆分，这说明其生产落地必须重视：

- 引擎版本矩阵管理
- Connector 升级测试
- Catalog 行为回归验证
- 平台灰度发布机制

## 8.4 运维复杂度上升

如果启用的组件较多，最终需要维护的将不仅是一个 server，还包括：

- Catalog 配置
- Client 与 Connector
- Auth 与授权体系
- Lineage sink/source
- Job executor
- Optimizer 配置
- 指标与日志

这意味着它适合有一定平台化投入能力的组织。

## 8.5 组织协同问题

在企业内部，元数据、权限、血缘、任务编排和引擎接入往往归属不同团队。Gravitino 作为统一控制面，要真正发挥价值，必须有清晰的责任边界设计，否则容易出现：

- 平台想统一，但业务团队不配合。
- 元数据建好了，但权限策略没人维护。
- 血缘接入了，但没人消费结果。
- Optimizer 能跑，但没有标准作业模板和变更流程。

## 9. 适用场景判断

## 9.1 推荐场景

- 建设企业统一元数据平台。
- 数据湖、湖仓、关系型数据库、消息系统并存。
- 多引擎统一接入治理。
- 需要逐步建设策略、血缘、维护自动化。
- 有平台团队可以承担中长期演进。

## 9.2 谨慎场景

- 小团队、单数据源、单引擎。
- 仅希望快速建表和查表，不做治理整合。
- 缺少 SSO、权限平台、运维体系支持。
- 对新系统引入非常保守，无法承担版本兼容验证。

## 10. 可落地实施方案

本次建议采用“三阶段递进式落地”，避免一开始就把所有能力同时引入。

## 10.1 第一阶段：PoC 最小可用验证

### 目标

验证 Gravitino 是否具备成为统一元数据入口的可行性。

### 建议范围

- 只选择 1 个业务域作为试点。
- 只接入 2 类典型资产。
- 只打通 1 个主要执行引擎。
- 暂不引入复杂血缘处理和自动化维护。

### 推荐组合

可优先选择以下组合之一：

- Hive / Iceberg + S3 或 HDFS + Trino
- Iceberg + Spark
- JDBC 数据源 + Fileset + Python / Java Client

### 推荐步骤

1. 部署单实例 Gravitino。
2. 使用 PostgreSQL 或 MySQL 作为实体存储，不建议长期使用 H2。
3. 创建一个 Metalake，作为试点业务域的顶层空间。
4. 创建两个样板 Catalog，例如一个 relational、一个 fileset 或 iceberg。
5. 用 Java Client、Python Client 和 REST API 进行对象操作验证。
6. 选定一个主要引擎完成接入。
7. 使用 Web UI 验证管理体验。

### 验收标准

- 能在同一个入口中管理异构资产。
- 能正常完成 catalog、schema、object 的生命周期操作。
- 至少一个引擎能通过 Gravitino 完成元数据访问。
- 基本认证方案可打通。

## 10.2 第二阶段：治理能力试生产

### 目标

在统一目录之上叠加基础治理能力。

### 建议引入能力

- OAuth / OIDC 认证
- 用户、组、角色
- Policy 继承策略
- OpenLineage 事件采集与转发

### 推荐步骤

1. 接入企业统一身份认证。
2. 建立最小角色模型，例如平台管理员、域管理员、只读用户。
3. 以 Catalog 和 Schema 为主设计策略继承，不建议一开始全部堆到 Table 层。
4. 将血缘事件先通过 HTTP Source 接入，再输出到日志或外部 HTTP Sink。
5. 建立治理操作审计和变更流程。

### 验收标准

- 用户身份可映射至企业 IdP。
- 至少一类对象的访问策略可稳定运作。
- 至少一条数据链路的血缘可采集与验证。

## 10.3 第三阶段：平台级生产化

### 目标

将 Gravitino 纳入企业数据平台正式组件体系。

### 建议引入能力

- 多 Catalog 扩展接入
- 标准化 Catalog 模板
- 多引擎 Connector 发布体系
- Optimizer 与维护任务模板
- 监控、日志、容量与兼容性治理

### 推荐步骤

1. 建立 catalog onboarding 模板和接入规范。
2. 建立 connector 版本矩阵和测试矩阵。
3. 启用 metrics、日志归档、缓存统计与告警。
4. 试点引入 Optimizer，例如 Iceberg compaction。
5. 建立灰度发布和回滚方案。
6. 建立跨团队责任划分机制。

### 验收标准

- 多引擎、多资产统一接入。
- 策略和权限具备稳定运行机制。
- 血缘与维护任务进入标准化运维流程。
- 平台升级具备明确的兼容验证与回滚预案。

## 11. 推荐的内部推进方式

建议采用“业务试点 + 平台共建”的推进模式。

### 11.1 组织建议

- 数据平台团队负责 Gravitino Server、Catalog 接入规范与 Connector 管理。
- 安全团队负责认证、角色与权限映射方案。
- 数据治理团队负责策略、血缘与元数据标准。
- 业务团队负责试点资产接入与验证。

### 11.2 里程碑建议

#### 里程碑一：2~4 周

- 完成环境部署。
- 选定试点域。
- 接入两个样板 Catalog。
- 完成客户端与一个引擎的联通验证。

#### 里程碑二：4~8 周

- 引入 OIDC。
- 设计最小权限模型。
- 打通至少一条血缘链路。
- 输出 PoC 评估报告。

#### 里程碑三：8~12 周

- 试运行治理方案。
- 沉淀 Catalog 模板。
- 规划生产试点范围。

## 12. 最终建议

综合来看，Gravitino 的价值不在于替代某一个单点元数据服务，而在于为企业构建统一元数据治理控制面提供基础。

### 12.1 建议结论

建议进入 PoC，但不建议一步到位进行大规模替换。

### 12.2 原因

- 其统一对象模型和多引擎接入能力有明显平台价值。
- Policy、Lineage、Optimizer 等能力具备较强扩展潜力。
- 但其生产化落地需要较强的平台工程能力和组织协同能力。

### 12.3 推荐策略

- 短期：验证其统一目录与接入价值。
- 中期：验证其认证、策略与血缘价值。
- 长期：将其纳入数据平台治理与维护体系。

## 13. 附录：本次调研的核心观察点

本次判断主要基于以下项目事实：

- 项目自我定位为 federated metadata lake，强调统一元数据与治理控制面。
- 仓库模块覆盖 server、catalog、client、connector、lineage、maintenance 等完整平台能力。
- 服务端通过多 Dispatcher 暴露丰富对象域能力，显示出其控制面平台定位。
- Catalog 类型明确覆盖 relational、fileset、messaging、model 等多资产形态。
- 安全、Policy、Lineage、Optimizer 均已有独立能力面，不再只是基础 catalog 服务。

## 14. 一句话总结

Gravitino 值得作为企业统一元数据与治理控制面的候选方案进入 PoC，但应坚持“小范围验证、分阶段演进、治理先行、兼容性受控”的落地原则。
