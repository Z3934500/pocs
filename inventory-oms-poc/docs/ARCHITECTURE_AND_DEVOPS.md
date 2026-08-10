# Inventory OMS：总体架构、SRE 与多 AZ 部署设计

本文把当前 inventory-oms-poc 的代码实现、生产适配器、秒杀热路径和生产运维方案放在同一张图和同一套运行手册里。图中的实线表示同步调用，虚线表示异步消息或观测链路。

## 1. 总体架构图

~~~mermaid
flowchart TB
    Client[Client / API Gateway]
    CICD[CI/CD\nMaven · Test · Scan · SBOM · Image · Helm/GitOps]
    ECR[Container Registry\nECR immutable tag/digest]
    EKS[EKS private cluster\n3 AZ · node groups/Karpenter]

    subgraph APP[OMS application services]
      Order[Order Service\nOrder aggregate + Saga]
      Inv[Inventory Service\nStock + Reservation owner]
      Pay[Payment Service\nPayment + Ledger owner]
      Recon[Reconciliation Job\nrepair / audit]
    end

    subgraph DB[Service-owned durable state]
      ODB[(Order DB\nAurora PostgreSQL)]
      IDB[(Inventory DB\nAurora PostgreSQL)]
      PDB[(Payment DB\nAurora PostgreSQL)]
    end

    subgraph DELIVERY[Reliable delivery]
      OO[Order Outbox Relay]
      IO[Inventory Outbox Relay]
      PO[Payment Outbox Relay]
      Broker{Kafka/MSK\nor SQS/SQS FIFO}
      DLQ[Broker DLQ]
      Inbox[Inbox / idempotent consumers]
    end

    subgraph HOT[Flash-sale hot path]
      Redis[(Redis Cluster\nMulti-AZ + replica)]
      Lua[Lua atomic admission\nquota + user/request dedup]
      Stream[Redis Stream\nconsumer group + Redis DLQ]
    end

    subgraph OBS[Observability]
      Prom[Prometheus / AMP]
      Graf[Grafana dashboards]
      Alert[Alertmanager / PagerDuty]
      Logs[CloudWatch / OpenSearch]
      Trace[OpenTelemetry / X-Ray]
    end

    Client --> Order
    Order -->|REST reserve / commit / release| Inv
    Order -->|REST capture / refund / status| Pay
    Order --> ODB
    Inv --> IDB
    Pay --> PDB
    Order --> OO
    Inv --> IO
    Pay --> PO
    OO -.-> Broker
    IO -.-> Broker
    PO -.-> Broker
    Broker -.-> Inbox
    Broker -.-> DLQ
    Client -->|seckill admission| Inv
    Inv --> Lua
    Lua --> Redis
    Lua -.-> Stream
    Stream -->|persist after DB transaction| Inv
    Recon --> ODB
    Recon --> IDB
    Recon --> PDB
    CICD --> ECR --> EKS
    EKS -.-> Prom
    Prom --> Graf
    Prom --> Alert
    EKS -.-> Logs
    EKS -.-> Trace

    classDef data fill:#e8f4ff,stroke:#2878b5;
    classDef app fill:#eef9ee,stroke:#3b873e;
    classDef infra fill:#fff5e6,stroke:#c77b00;
    class ODB,IDB,PDB,Redis data;
    class Order,Inv,Pay,Recon,Inbox app;
    class EKS,ECR,Broker,DLQ,Prom,Graf,Alert,Logs,Trace,CICD infra;
~~~

### 边界和所有权

| 边界 | 唯一写入者 | 核心数据 | 一致性手段 |
|---|---|---|---|
| Order | order-service | 订单、Saga 状态、订单 Outbox | 本地事务 + 幂等键 + Outbox |
| Inventory | inventory-service | SKU 库存、库存预留、库存 Outbox | SKU 行锁 + @Version + 状态机 |
| Payment | payment-service | 支付交易、Ledger、支付 Outbox | 支付幂等键 + provider reference 唯一约束 |
| Broker | Kafka/MSK 或 SQS | 事件传输 | 至少一次投递、重试、DLQ、Inbox |
| 秒杀 Redis | inventory-service 热路径 | 短时配额、用户/请求去重、Stream | Lua 单脚本原子执行；DB 仍是事实源 |
| Reconciliation | reconciliation-job | 对账结果和修复建议 | 周期扫描、可审计、人工确认高风险修复 |

Outbox、Kafka/SQS 和 Saga/TCC 是三个层次：Outbox 解决本地数据库提交与消息发送之间的间隙；Kafka/SQS 负责传输；Saga/TCC 负责跨服务业务补偿。它们是组合关系，不是替代关系。

## 2. 业务流程、业务场景和组件

### 2.1 正常下单流程

~~~mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant O as Order Service
    participant I as Inventory Service
    participant P as Payment Service
    participant DB as Local DB + Outbox
    participant B as Kafka/SQS
    C->>O: Create order + Idempotency-Key
    O->>O: lock/idempotency check
    O->>DB: PENDING order + order outbox (one local tx)
    O->>I: reserve(orderId, sku, qty, key)
    I->>DB: SKU row lock + reservation + outbox (one local tx)
    I-->>O: RESERVED
    O->>P: capture(orderId, amount, payment key)
    P->>DB: payment + ledger + outbox (one local tx)
    P-->>O: CAPTURED
    O->>I: commit(orderId)
    I->>DB: reservation lock + SKU row lock + outbox
    I-->>O: COMMITTED
    O->>DB: CONFIRMED + outbox
    DB-->>B: Relay after commit; retry/DLQ on failure
    B-->>C: downstream notification/order event
~~~

### 2.2 失败、超时和重试

| 场景 | 业务结果 | 代码/组件 |
|---|---|---|
| 客户端重复提交 | 返回第一次结果，不创建第二笔订单 | Idempotency-Key、唯一约束 |
| 库存不足 | 订单失败，不调用支付 | Inventory SKU 行锁、状态校验 |
| 支付明确失败 | 释放已预留库存，订单进入失败/补偿状态 | Saga compensation、release |
| 支付超时或返回未知 | 不直接重复扣款；查询 provider 状态，再决定提交或退款 | payment status query、重试/对账 |
| Commit 调用超时 | 重试幂等的 commit(orderId) | 预留状态机、订单锁、Outbox |
| 进程在 DB commit 后崩溃 | Outbox relay 重新发送 | lease claim、backoff、DLQ |
| Broker 重复投递 | 只产生一次业务效果 | Inbox / eventId 幂等 |
| 预留过期 | 释放库存并发出 expired/released 事件 | 定时任务、同一补偿路径 |
| 秒杀 Redis 接受后 DB 暂时不可用 | Stream 保留 pending，重试；超过次数进入 DLQ 并释放配额 | Redis Stream consumer、DLQ |
| Redis/DB 配额初始化出现未知结果 | 停止开售，交由 reconciliation 核对，不人工盲改 | quota launch workflow、对账 |

### 2.3 秒杀高并发流程

~~~mermaid
flowchart LR
    A[开售前] --> B[DB SKU 行锁]
    B --> C[availableQty -> seckillAllocatedQty]
    C --> D[Redis stock key 初始化]
    U[大量用户请求] --> L[Lua 单脚本]
    L -->|用户/请求重复| R1[幂等拒绝/返回原结果]
    L -->|配额不足| R2[SOLD_OUT]
    L -->|成功| R3[DECRBY + SADD + HSET + XADD]
    R3 --> S[Redis Stream]
    S --> W[有界 consumer group]
    W --> T[DB reservation tx]
    T -->|commit 成功| ACK[XACK]
    T -->|暂时失败| RETRY[保留 pending，指数退避]
    RETRY --> W
    T -->|超过最大次数| D[Redis DLQ + 释放配额]
~~~

当前代码中 Redis/Lua 是可运行的 opt-in 实现，并非把 Redis 当库存事实源：

- 开售前先在 DB 中锁住 SKU，把有限配额从 availableQty 移到 seckillAllocatedQty。
- Lua 在 Redis 内原子检查配额、用户去重、请求去重、扣减并写入 Stream。
- Stream consumer 只有在 DB 本地事务成功后才 XACK；失败记录进入 pending 重试。
- 普通 DB 预留不再消费已经分配给秒杀的配额，避免 Redis admission 与普通路径重复扣减。
- Redis Cluster key 使用同一个 SKU hash tag，例如 oms:seckill:stock:{SKU-1}，保证脚本涉及的 key 位于同一 slot。

## 3. 当前锁的粒度和死锁分析

### 3.1 当前锁模型

| 路径 | 当前锁 | 粒度 | 说明 |
|---|---|---|---|
| 普通 reserve | findBySkuForUpdate() | 单个 SKU 一行 | 同一 SKU 的扣减串行；不同 SKU 可以并行 |
| 普通 reserve 的重复订单检查 | findByOrderIdForUpdate()（已有记录时） | 单个订单预留一行 | 再由数据库唯一约束兜底并发重复创建 |
| commit/release | 先锁 reservation，再锁对应 SKU | 一个订单预留行 + 一个 SKU 行 | 代码路径保持 reservation -> SKU 的顺序 |
| 过期扫描 | 先查候选，再按订单加锁，之后锁 SKU | 单个过期订单 + SKU | 与 release 共用补偿逻辑 |
| 秒杀配额初始化/释放 | findBySkuForUpdate() | 单个 SKU 一行 | 开售配额变更与普通库存更新互斥 |
| Outbox relay | claim/lease 条件更新 | 单条 Outbox 行 | 短 lease 防止多个 relay 同时发送同一事件 |
| Redis admission | Lua 脚本 | 同一 SKU 的 Redis key 集合 | Redis 单线程脚本原子执行，不是 Java synchronized |

所以现在不是“全局库存锁”，而是“每个 SKU 一把数据库行锁”。高并发热点 SKU 会形成队列，这是正确性和吞吐之间的明确 trade-off；不同 SKU 不会因为同一把 Java 锁而互相阻塞。

### 3.2 线程安全

- Spring service 默认是 singleton，但不保存请求级库存、用户或订单状态；共享事实放在 DB/Redis。
- scheduler 使用 bounded ScheduledThreadPoolExecutor，不是每个请求创建线程池。
- Redis Lua 把“检查 + 扣减 + 去重 + 入 Stream”放在一个原子脚本中。
- Outbox relay 使用 DB claim/lease；发布在数据库事务之外，避免把网络 IO 放进库存事务。
- 数据库唯一约束、@Version 和状态机共同处理重复请求；不能只依赖内存锁。

### 3.3 死锁发生可能性和规避

当前实现通过所有库存生命周期路径统一采用“reservation -> SKU”顺序来降低死锁概率，并对数据库锁获取失败/死锁做 3 次、50ms 起始的指数退避重试。当前 POC 只有一个 SKU/订单模型，因此不会在同一事务中锁定多个 SKU。

生产新增多 SKU 订单时必须：

1. 先按规范化 SKU 排序；
2. 按排序顺序一次性锁定所有 SKU；
3. 在事务内完成校验和扣减；
4. 设置数据库 lock_timeout/事务超时，记录 lock wait 和 deadlock graph；
5. 不要在持有库存锁时调用支付、HTTP、Kafka 或 SQS。

重试不是死锁设计本身，只是兜底。若死锁率上升，应先检查锁顺序、长事务、缺索引和外部调用是否误放入事务，再调大重试次数。

### 3.4 Java/Spring 编码规范映射

- 使用构造器注入，不把依赖隐藏在 field injection 中；业务服务、repository 和 adapter 的边界清晰。
- @Transactional 只包住本地状态变更和 Outbox 写入；Kafka/SQS/支付 HTTP 等外部 IO 在事务外执行。
- @Retryable 只用于可重放的本地幂等事务，并限制次数和退避；不能用它包住非幂等扣款。
- 用 @ConditionalOnProperty 选择 logging、Kafka 或 SQS adapter，用接口隔离业务层和传输层。
- scheduler 使用显式的 Spring bounded executor；@Scheduled 方法只负责触发短批次，不创建无界线程。
- 方法引用用于签名完全匹配且能提高可读性的场景，例如 PaymentService::response、String::trim；需要记录上下文、处理异常或组合多个字段时使用 lambda/普通方法。
- 状态迁移通过领域方法和显式状态校验完成，controller 不直接修改 entity 字段；DTO/command 做输入边界校验。

## 4. DevOps/SRE：如何打包和发布

### 4.1 三层制品

| 层 | 制品 | 发布原则 |
|---|---|---|
| Java | 每个服务的 versioned JAR | Maven reactor 编译，测试和依赖检查通过后生成 |
| Container | order-service、inventory-service、payment-service 镜像 | 多阶段构建、JRE 17、non-root、不可变 Git SHA/digest tag |
| Deployment | Helm chart + values | chart lint/template，环境配置与镜像版本分离，GitOps 或受控 helm upgrade |

本项目 Dockerfile 已经采用 Maven build stage + Temurin 17 JRE runtime stage。生产建议进一步使用固定基础镜像 digest、BuildKit cache、SBOM、漏洞扫描和 Cosign 签名；不要用 latest 作为生产发布标识。

### 4.2 推荐流水线

~~~mermaid
flowchart LR
    A[Commit / PR] --> B[mvn clean verify]
    B --> C[SAST + dependency/license scan]
    C --> D[Docker BuildKit multi-stage]
    D --> E[Trivy/Grype + SBOM]
    E --> F[Cosign sign image]
    F --> G[Push ECR by Git SHA]
    G --> H[helm lint + helm template]
    H --> I[Deploy dev/staging]
    I --> J[smoke + contract + load checks]
    J --> K[GitOps promotion / helm upgrade --atomic]
    K --> L[post-deploy SLO checks]
    L -->|failure| M[rollback + incident]
~~~

常用命令：

~~~bash
# 全量编译、测试和校验
mvn -B -ntp clean verify

# 只打包某个服务及其依赖模块
mvn -B -ntp -pl inventory-service -am -DskipTests package

# 构建镜像；CI 中 TAG 应替换为 Git SHA
docker build -f inventory-service/Dockerfile -t $REGISTRY/inventory-service:$GIT_SHA .
docker build -f order-service/Dockerfile -t $REGISTRY/order-service:$GIT_SHA .
docker build -f payment-service/Dockerfile -t $REGISTRY/payment-service:$GIT_SHA .

# Helm 校验
helm lint deploy/helm/oms-services -f deploy/helm/oms-services/values-production.yaml
helm template oms deploy/helm/oms-services \
  -f deploy/helm/oms-services/values-production.yaml

# 受控发布；GitOps 场景由 Argo CD/Flux 执行同样的 desired state
helm upgrade --install oms deploy/helm/oms-services \
  -n oms --create-namespace \
  -f deploy/helm/oms-services/values-production.yaml \
  --set global.registry=$REGISTRY \
  --set global.tag=$GIT_SHA \
  --atomic --wait --timeout 10m
~~~

CI 不应把 AWS key 写入镜像或日志。EKS 使用 IRSA/Pod Identity，Kafka/MSK、SQS、Redis 和数据库权限按服务最小化授予。镜像发布成功不等于业务发布成功，必须等待 readiness、smoke test、Outbox age 和错误率恢复。

## 5. 监控指标设计（SRE）

### 5.1 指标分层

| 层 | 必选指标 | 目的 |
|---|---|---|
| RED/API | request rate、5xx/业务错误率、p50/p95/p99 latency | 判断用户是否受影响 |
| JVM | heap、GC pause、线程数、CPU、class loading | 判断进程容量和泄漏 |
| DB/USE | connection pool active/pending、事务时长、lock wait、deadlock、慢 SQL | 判断库存串行化和数据库瓶颈 |
| Outbox/Inbox | pending 数、最老消息年龄、publish success/fail、attempt、DLQ、重复事件 | 判断可靠投递是否退化 |
| Kafka/SQS | consumer lag、producer error、under-replicated、visible/not-visible、oldest age、DLQ | 判断消息积压和丢失风险 |
| Redis | memory、eviction、Lua latency/error、blocked clients、ops/sec、Stream pending/DLQ | 判断秒杀 admission 是否安全 |
| Kubernetes | ready replicas、restart/OOM、CPU throttling、HPA desired/current、node pressure | 判断集群容量和发布风险 |
| 业务 | reserve success/failed、stock sold-out、payment unknown、compensation、reconciliation drift | 直接反映 OMS 业务健康 |

已有代码的业务 Counter 包括 inventory.reservation.created、inventory.reservation.committed、inventory.reservation.released、inventory.seckill.reservation.persisted、订单 Saga 结果和支付 capture/refund 结果。生产应再为关键路径补 Timer/histogram：reserve、capture、commit、DB lock wait、Outbox publish、Kafka/SQS send、Redis Lua 和 Stream persist。

业务指标 label 要低基数：建议 service、operation、result、reason、transport。不要把 orderId、userId 或 SKU 直接作为 Prometheus label；这些放在结构化日志和 trace attributes 中，否则秒杀流量会造成指标基数爆炸。

### 5.2 SLO 和告警示例

| 告警 | 建议阈值/窗口 | 级别 | 第一动作 |
|---|---|---|---|
| API 5xx | 5 分钟持续 > 2% | P1 | 看最近发布、Pod、下游依赖 |
| reserve p99 | 10 分钟超过基线 2 倍或 > 500ms | P1/P2 | 看 SKU 锁等待、DB pool、CPU throttling |
| Outbox oldest age | > 60s warning，> 5m critical | P1 | 检查 relay、DB claim、broker/IAM |
| Kafka lag / SQS oldest age | 超过业务允许 SLA | P1 | 检查 consumer、分区/可见性超时、DLQ |
| Redis Lua error/latency | 错误 > 0 或 p99 超过 20ms | P1 | 关闭/降级秒杀入口，检查 Redis |
| 秒杀 Stream pending | 持续增长或 DLQ > 0 | P1 | 保留消息，检查 DB、consumer、锁等待 |
| deadlock/lock timeout | 5 分钟内持续出现 | P1/P2 | 抓锁图和慢 SQL，检查锁顺序 |
| payment unknown | 任何持续增长 | P1 | 禁止重复扣款，走 provider query/reconciliation |
| Pod unavailable/restart | ready < desired 或 OOM 增长 | P1/P2 | describe/events/previous logs，必要时回滚 |

建议至少建立四张 Grafana dashboard：业务 SLO、API/JVM、数据库/库存锁、消息/秒杀/集群。告警必须链接到本文 Troubleshooting 对应章节，并记录 correlation ID、eventId、orderId，而不是只发一个数字。

## 6. Infra 层、集群部署和多 AZ

### 6.1 推荐生产拓扑

~~~text
Region
├── VPC
│   ├── Private subnet AZ-a: EKS nodes + pods
│   ├── Private subnet AZ-b: EKS nodes + pods
│   ├── Private subnet AZ-c: EKS nodes + pods
│   └── NAT/VPC endpoints: ECR, CloudWatch, SQS, STS 等
├── EKS control plane（AWS 托管，多 AZ）
├── Managed node group / Karpenter（至少 3 个 AZ）
├── Aurora PostgreSQL：Multi-AZ writer + reader/failover
├── MSK：至少 3 brokers，跨 3 AZ，replication factor 3
├── ElastiCache Redis：replication group，跨 AZ，automatic failover
└── SQS：regional managed service + per-queue DLQ
~~~

应用服务是无状态的，生产基线建议每个服务 3 个 Pod、每个 AZ 至少 1 个；HPA 负责从 3 扩到 20，Cluster Autoscaler/Karpenter 负责补充节点。副本数不是数据复制：订单、库存和支付数据必须由各自的 Aurora/PostgreSQL 持久化，不能依赖 Pod 的 emptyDir 或本地 H2。

组件的 replica 安排：

- EKS worker node：至少 3 个节点，分布在 3 AZ；关键服务可用独立 node pool/taint。
- Order/Inventory/Payment：基线 3 replicas，topologySpreadConstraints 按 zone 和 hostname 分散，PDB minAvailable: 2。
- Kafka/MSK：3 brokers 跨 3 AZ，topic replication factor 3，min.insync.replicas=2；producer acks=all。
- Redis：启用副本、Multi-AZ automatic failover、TLS/ACL；秒杀需要评估 cluster mode 和单 SKU 热点。
- Aurora：Multi-AZ writer/standby，读副本只承接明确允许的读流量；库存扣减和状态变更走 writer。
- SQS：不需要自建 replica；设置 visibility timeout、redrive policy、DLQ，并监控消息年龄。
- Reconciliation：作为单实例/CronJob/带 leader election 的任务运行，避免多个实例同时修复同一业务事实。

### 6.2 Kubernetes 发布安全

Deployment 应使用 RollingUpdate、maxUnavailable: 0、maxSurge: 1、terminationGracePeriodSeconds 和 startupProbe。readiness 失败时不接新流量，preStop/优雅停机需要给正在处理的请求、Outbox claim 和 broker client 留出时间。

当前 chart 已有 3 个生产副本、资源 request/limit、readiness/liveness、非 root、只读根文件系统和 CPU HPA；本次已补充多 AZ spread、hostname 反亲和、PDB、滚动更新和 startup probe。生产仍需把 values 中的镜像 tag 替换为 immutable SHA/digest，并通过 IaC 创建 3 AZ node group、数据库、MSK、Redis、IAM、NetworkPolicy 和告警规则。

NetworkPolicy 默认只允许应用 Pod 间流量、DNS 和 kube-system 访问；实际生产要按调用矩阵收紧到 namespace/service labels，并显式允许数据库、MSK、Redis 的私网出口。应用内部的 /internal/inventory/seckill/* 端点必须叠加 IAM/mTLS/JWT 或网关策略，不能因为网络隔离就视为已授权。

## 7. Troubleshooting Runbook

### 7.1 统一排障顺序

1. 先确认影响范围：哪个 API、哪个 SKU/订单、哪个 AZ、开始时间和当前发布版本。
2. 先看 SLO/RED dashboard 和最近 30 分钟发布、扩容、依赖故障。
3. 通过 trace/correlation ID 串起 Order -> Inventory -> Payment -> Outbox -> Broker。
4. 先止损，再修复：秒杀可暂停入口或限流；不要直接删除 Outbox、Redis Stream 或人工改库存。
5. 修复后观察至少一个业务 SLA 周期，并运行 reconciliation 验证库存、订单、支付和 Ledger。

### 7.2 常见故障

#### Pod Pending、NotReady 或 CrashLoopBackOff

~~~bash
kubectl get pods -n oms -o wide
kubectl describe pod <pod> -n oms
kubectl get events -n oms --sort-by=.lastTimestamp
kubectl logs <pod> -n oms --all-containers --tail=200
kubectl logs <pod> -n oms --previous --tail=200
~~~

检查节点资源、TopologySpread、镜像拉取、ServiceAccount/IAM、启动探针和配置注入。若是 OOM，查看 kubectl top pod、JVM heap/GC 和 container limit；先恢复容量，再分析泄漏或批量大小。

#### API 变慢或库存锁等待升高

沿着 API p99 -> CPU throttling -> DB pool -> lock wait/deadlock -> downstream latency 排查。确认热点是否集中在单 SKU；单 SKU 行锁排队是当前模型的预期现象。不要通过增大线程池掩盖 DB 锁瓶颈，也不要在锁内调用支付或消息网络请求。必要时暂停热点 SKU 的秒杀入口、降低 admission rate，并在 DB 侧抓取 lock graph/慢 SQL。

#### Outbox backlog 或消息发布失败

先查 pending 数、最老消息 age、in-flight lease、attempt 和 DLQ，再查 relay Pod 日志、DB claim、Kafka/SQS 网络、DNS、TLS 和 IAM。修复 broker 后让 relay 自然重试；只有在确认消费方幂等、事件仍完整且经过审批后才 replay。不要批量删除 pending/dead-letter 记录。

#### Kafka lag、SQS backlog 或 DLQ 增长

检查 consumer Pod 是否 ready、consumer group 是否 rebalance、Kafka partition lag/under-replicated，或 SQS visibility timeout/receive error/oldest message age。按 backlog 扩 consumer，但不能突破数据库锁和下游容量；扩容后仍要观察 DB lock wait。DLQ 消息先保留 payload、eventId 和错误原因，修复消费者后按事件类型重放。

#### Redis 秒杀 pending、Lua 错误或库存异常

立刻检查 Redis memory、eviction、blocked clients、Lua latency/error、Stream pending 和 DLQ。若 admission 的结果不确定，暂停开售，不要重复初始化配额。核对 DB 的 availableQty、seckillAllocatedQty、已落库 reservation、Redis stock、Stream pending 数；通过 reconciliation 决定补发、释放或人工审核。

#### DB deadlock、lock timeout 或状态不一致

保留数据库 deadlock graph、事务 SQL、orderId/SKU、应用 trace。确认是否违反 reservation -> SKU 的锁顺序、是否存在多 SKU 未排序、缺索引、长事务或外部调用在事务内。当前代码最多进行有限重试；如果同一业务命令最终失败，依靠幂等键重放，而不是新建另一笔订单。

#### 单 AZ 或节点池故障

~~~bash
kubectl get nodes -L topology.kubernetes.io/zone
kubectl get pods -n oms -o wide
kubectl get pdb -n oms
kubectl get deploy -n oms
kubectl cordon <node>   # 仅在确认节点故障、按变更流程执行
~~~

确认 Pod 是否已经跨 AZ 分布、PDB 是否允许驱逐、HPA/Cluster Autoscaler 是否有容量、Aurora/Redis/MSK 是否完成 failover。不要在容量不足时强行 drain 整个 AZ；先扩容健康 AZ 的节点，再逐步迁移。

#### 发布后回归或需要回滚

~~~bash
helm history oms -n oms
helm rollback oms <REVISION> -n oms --wait --timeout 10m
kubectl rollout status deployment -n oms
kubectl get pods -n oms -o wide
~~~

回滚后继续观察 Outbox、Inbox、broker lag、payment unknown 和 reconciliation。代码回滚不会自动回滚已经提交的业务数据，数据库 migration 必须向后兼容；消息 schema 使用 schemaVersion，消费者要兼容旧事件。

## 8. 当前 PoC 与生产完成度

| 项目 | 当前状态 | 生产上线前动作 |
|---|---|---|
| Kafka/SQS | 真实条件适配器已编译，可由配置切换；默认 logging | 建 topic/queue/DLQ、权限、TLS、容量、故障演练和 live smoke test |
| Redis/Lua | 已实现 opt-in admission + Stream consumer | Redis ACL/TLS、XAUTOCLAIM、多副本演练、配额开售工作流和对账 |
| 数据库 | 本地 H2/ddl-auto=update 便于运行 | Aurora/PostgreSQL、Flyway/Liquibase、备份恢复和 lock/slow SQL 监控 |
| 内部接口 | 有 seckill endpoint，但代码中仍标注需保护 | IAM/mTLS/JWT、网关 allow-list、审计和限流 |
| Payment | POC provider stub；未知状态查询和补偿路径已有 | 外部 provider adapter、签名回调、超时/幂等/对账；不要把长外部调用放在 DB 事务内 |
| Observability | Actuator/Prometheus 和业务 Counter 已有 | Timer/histogram、trace propagation、dashboard、告警、日志脱敏和 on-call |
| Kubernetes | Helm 基础部署、3 replicas、HPA、probe、安全上下文 | 三 AZ node pool、Aurora/MSK/Redis 私网、NetworkPolicy、IaC 和灾备演练 |

结论：这套设计已经把“高并发入口”和“最终一致性落库”分成两条可审查的路径；但生产级并不等于打开一个开关。真正上线还需要把托管基础设施、权限、迁移、告警、故障演练和 reconciliation 一起纳入发布门禁。
## 9. Tomcat 与 Elastic Beanstalk：传统 Java 应用部署选型

### 9.1 适用场景

Elastic Beanstalk 的 Tomcat 平台适合以下应用：

- 已经打包为 WAR 的传统 Servlet/JSP 应用；
- 依赖 Tomcat 容器、Filter、Listener、Session 或旧版 Java Web 规范；
- 希望减少 EC2、Tomcat、Auto Scaling 和负载均衡器的基础运维；
- 需要通过负载均衡环境部署多个实例，并使用滚动、不可变或流量拆分发布。

典型链路如下：

~~~text
客户端
   |
   v
Application Load Balancer
   |
   v
Elastic Beanstalk Load-balanced Environment
   |
   +--> Auto Scaling Group
   |      +--> EC2 + Tomcat + WAR
   |      +--> EC2 + Tomcat + WAR
   |
   +--> RDS/Aurora（独立数据库）
~~~

Elastic Beanstalk 的 Tomcat 环境适合上传 WAR 包；如果应用是 Spring Boot 可执行 JAR，通常更自然的选择是 Java/容器平台，而不是为了使用 Tomcat 强行改成 WAR。

### 9.2 部署策略不能简单等同于零停机

| 策略 | 特点 | 适用场景 |
|---|---|---|
| Rolling | 分批替换实例；发布期间新旧版本会同时服务 | 小版本、兼容接口、可接受部分容量下降 |
| Rolling with additional batch | 先增加一批新实例，再分批替换 | 希望发布期间维持完整容量 |
| Immutable | 创建全新的 Auto Scaling Group，健康后再替换旧环境 | 风险较高的版本或平台升级 |
| Traffic splitting | 新旧版本同时运行，按比例切流 | Canary 验证和指标驱动发布 |
| All at once | 一次性替换全部实例 | 可接受短暂影响、追求最快发布 |

Rolling 不是绝对的零停机保证：批次中的实例会暂时退出服务，且新旧版本可能同时处理请求。应结合健康检查、readiness、连接排空、数据库 Schema 向后兼容和会话设计。

对生产环境的建议：

- 普通兼容发布：Rolling with additional batch；
- 重大版本或运行时更新：Immutable；
- 需要灰度验证：Traffic splitting；
- 数据库采用 Expand/Contract，先兼容旧版本，再切换应用，最后清理旧字段；
- Session 放到 ElastiCache Redis 或其他共享存储，不放在单台 Tomcat 本地内存；
- Tomcat 和应用日志输出到 CloudWatch Logs，ALB 访问日志落到 S3；
- 开启 Elastic Beanstalk managed platform updates 时，明确维护窗口、版本范围和回滚策略。

### 9.3 与当前 OMS POC 的关系

当前 POC 的实际部署路线是：

~~~text
Spring Boot JAR
    -> Docker 镜像
    -> ECR
    -> EKS Deployment
    -> Kubernetes Service / Internal ALB
~~~

因此 Tomcat/Elastic Beanstalk 不需要加入当前订单、库存和支付业务代码。它可以作为以下场景的迁移参考：

- 接入已有的传统 Tomcat/WAR 订单门户；
- 迁移旧版 Java 管理后台；
- 在暂未采用 EKS 的团队中快速托管 Java Web 应用；
- 对比 PaaS、ECS、EKS 的运维边界。

选型记忆：

~~~text
传统 WAR + Tomcat + 想降低基础运维 -> Elastic Beanstalk Tomcat
Spring Boot 容器 + 微服务 + Kubernetes 生态 -> EKS
简单容器服务 + 不需要 Kubernetes API -> ECS/Fargate
短时、事件驱动、无状态函数 -> Lambda
~~~

### 9.4 相关监控指标

Tomcat/Elastic Beanstalk 环境至少应监控：

| 层级 | 指标或日志 | 排障问题 |
|---|---|---|
| ALB | TargetResponseTime、HTTP 5xx、UnHealthyHostCount | 请求是否到达、后端是否健康 |
| Beanstalk | EnvironmentHealth、部署事件、实例替换 | 环境或发布是否异常 |
| Tomcat | 请求数、响应时间、线程池、连接器队列 | Servlet 容器是否排队 |
| JVM | Heap、GC pause、线程、CPU、Class Loading | 是否 OOM、GC 或线程阻塞 |
| 应用 | 结构化日志、trace_id、业务错误 | 哪个订单或接口失败 |
| 数据层 | RDS、Redis、外部 API 延迟 | 是否为下游依赖造成慢请求 |

官方参考：

- Elastic Beanstalk 部署策略：https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/using-features.rolling-version-deploy.html
- Elastic Beanstalk 托管平台更新：https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/environment-platform-update-managed.html
- Elastic Beanstalk Tomcat 平台：https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/platforms-glossary.html

## 10. Java/PHP 混合应用：Elastic Beanstalk 环境隔离与蓝绿发布

当现有系统同时包含 Java 和 PHP、功能迭代频繁，又要求高可用和低运维时，Elastic Beanstalk 可以作为托管运行平台。但要注意：Java/Tomcat 和 PHP 是不同的 Elastic Beanstalk 平台，通常应创建**独立环境**，不要把两个原生平台版本误认为可以直接放进同一个标准环境。

### 10.1 推荐架构

~~~text
用户
  -> Route 53 / CloudFront / API Gateway
       | host/path 路由
       +--> Java Beanstalk 环境（ALB + ASG，跨多个 AZ）
       +--> PHP Beanstalk 环境（ALB + ASG，跨多个 AZ）
                    |
                    +--> 共享的外部 RDS/Redis/S3 等托管服务
~~~

Elastic Beanstalk 当前提供 Java、Tomcat、PHP 和 .NET 等托管平台。Java 和 PHP 分环境部署可以分别选择运行时、扩容策略、健康检查和发布节奏；如果业务必须把两种语言打包成一个部署单元，应评估 Docker/ECS/Fargate，而不是强行混用两个 Beanstalk 原生平台。

### 10.2 高可用配置

- 每个 Beanstalk 环境使用 Load balanced environment，实例分布在至少两个 Availability Zone。
- 使用 Auto Scaling Group 设置合理的 `MinSize`、`DesiredCapacity` 和 `MaxSize`，并结合 CPU、ALB RequestCountPerTarget 或延迟指标做目标跟踪。
- 会话、上传文件和业务状态放到 Redis、S3 或数据库，应用实例保持无状态，避免扩容或替换实例时丢失状态。
- 数据库不要由 Beanstalk 环境临时创建和绑定；使用独立的 RDS Multi-AZ，并将连接信息放入 Secrets Manager/Parameter Store。

### 10.3 Blue/Green + CNAME Swap 操作流程

~~~text
Blue：production 环境 -> 当前生产流量
Green：staging 环境  -> 新版本、独立 URL、预生产测试

部署 Green -> 健康检查/Smoke Test -> Swap Environment URLs
    -> CNAME 指向 Green
    -> 观察指标 -> 保留 Blue 作为快速回滚环境
~~~

具体流程：

1. 克隆当前生产环境或使用同等配置创建 Green 环境。
2. 将新版本分别部署到 Java/PHP 对应的 Green 环境。
3. 通过 Green 的独立 URL 做健康检查、接口回归、数据库兼容性和容量测试。
4. 在 Elastic Beanstalk 中执行 **Actions -> Swap environment URLs**，交换两个环境的 CNAME。
5. 观察 ALB 5xx、目标健康数、P95 延迟、JVM/ PHP-FPM 指标和业务成功率。
6. 发现问题时再次 Swap 回 Blue；确认 DNS 缓存和连接已稳定后，再终止旧环境。

这属于快速蓝绿切换，不应承诺所有客户端都“瞬时无感”：DNS 缓存、长连接、异步任务和外部回调仍可能带来短暂差异。数据库 schema 变更应采用 expand-contract，确保 Blue 和 Green 在切换窗口内都能读写兼容的数据结构。

### 10.4 POC 落地边界

当前 POC 使用 Spring Boot/H2，尚未真正创建 Elastic Beanstalk 环境、ALB、ASG 或 RDS。该方案属于生产部署路径：应用包、环境变量、健康检查、Secrets、数据库迁移和回滚脚本应由 IaC/CI/CD 管理，业务代码不应依赖 Beanstalk 的临时本地磁盘或本地 Session。

### 10.5 官方参考

- Elastic Beanstalk 支持的平台：https://docs.aws.amazon.com/elasticbeanstalk/latest/platforms/platforms-supported.html
- Elastic Beanstalk Blue/Green 与 CNAME Swap：https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/using-features.CNAMESwap.html
- Elastic Beanstalk 环境域名和 CNAME：https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/customdomains.html

## 11. .NET + Oracle Lift-and-Shift：最小开发改动的高可用迁移

如果现有应用依赖 Windows/IIS/.NET Framework，且目标是“尽量不改应用代码、提高可用性、减少自建服务器运维”，可采用“应用托管 + 同构数据库托管”的 Lift-and-Shift 方案：

~~~text
用户
  -> Elastic Beanstalk .NET on Windows Server
       -> ALB + Auto Scaling Group，跨多个 AZ
       -> Amazon RDS for Oracle Multi-AZ

本地 Oracle
  -> AWS DMS Full Load + CDC
  -> RDS for Oracle
~~~

### 11.1 迁移步骤

1. 盘点 .NET Framework 版本、IIS 模块、Windows 服务、Oracle Client/ODP.NET 驱动、存储过程和外部依赖，确认 Elastic Beanstalk .NET on Windows Server 的兼容性。
2. 在 AWS 侧创建 VPC、私有子网、ALB、Elastic Beanstalk 多 AZ 环境和 RDS for Oracle Multi-AZ；应用实例不要直接暴露数据库端口到公网。
3. 通过 Site-to-Site VPN 或 Direct Connect 打通本地 Oracle 与 AWS DMS Replication Instance 的网络，配置最小权限的源端和目标端点。
4. 使用 DMS 先做 Full Load，再用 CDC 持续同步变更；验证表行数、关键业务数据、索引、约束、存储过程和时区/字符集。
5. 在维护窗口冻结写入，等待 CDC 延迟清零，执行最终校验，将应用连接字符串切换到 RDS Oracle 端点。
6. 观察应用错误率、数据库 CPU/IOPS、连接数、DMS CDC 延迟和业务交易结果，确认稳定后再下线旧环境。

AWS 官方迁移指引也将“原生工具做全量 + DMS 做持续复制”作为 Oracle 到 RDS for Oracle 的常见混合路径；DMS 可以降低停机窗口，但不自动解决 Oracle 特性、许可证、版本和 SQL 兼容性问题。

### 11.2 为什么不是其他方案

| 方案 | 主要问题 |
|---|---|
| 重构为 Lambda | 需要改造为无状态、事件驱动和短生命周期函数，不符合最小开发改动 |
| .NET Framework 直接迁到 Amazon Linux | Windows/IIS/.NET Framework 依赖可能不兼容；只有确认跨平台 .NET 运行时后才适合 Linux |
| Oracle 迁移到 DynamoDB | SQL、事务、Schema 和数据访问层都要重写，迁移风险大 |
| EC2 自建 Windows + Oracle | 可行但需要自行管理补丁、容量、负载均衡、数据库备份和故障转移，运维成本更高 |

### 11.3 关键边界

- RDS Multi-AZ 解决数据库高可用和自动故障转移，不等于读扩展；报表读扩展要使用 Read Replica、Aurora Reader Endpoint 或分析库。
- DMS CDC 是异步复制，切换前必须监控 CDC 延迟并执行最终校验，不能只看任务状态为 `running`。
- 应用和数据库都要配置监控：Beanstalk/ALB 5xx、UnHealthyHostCount、响应延迟、实例替换、RDS Events、CPU/IOPS、连接数和 DMS 延迟。

### 11.4 官方参考

- 使用 DMS 迁移 Oracle 到 RDS for Oracle：https://docs.aws.amazon.com/dms/latest/sbs/chap-manageddatabases.oracle2rds.html
- AWS Prescriptive Guidance：Oracle 到 RDS for Oracle：https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/migrate-an-on-premises-oracle-database-to-amazon-rds-for-oracle.html

## 12. 运维升级、降级与紧急修复：Run Command、Maintenance Window 和 Patch Manager

### 12.1 先按任务类型选工具

| 场景 | 推荐工具 | 关键原因 |
|---|---|---|
| 严重 CVE、立即执行自定义修复脚本 | Systems Manager Run Command | 按需发送命令，适合应急响应和第三方软件自定义修复 |
| 每周/每月补丁、驱动和软件维护 | Maintenance Window + Run Command/Automation | 预先定义时间、目标、并发和停止条件，减少对业务的影响 |
| 标准 OS/应用补丁合规 | Patch Manager/Quick Setup patch policy | 使用 patch baseline 管理批准/拒绝补丁，支持扫描和安装 |
| 持续保证节点配置一致 | State Manager | 周期性检查并纠正配置漂移 |
| 可审计的多步骤升级、制作 AMI | Systems Manager Automation | 用 Runbook 编排备份、摘流、升级、验证和回滚 |
| 应用版本发布或降级 | CI/CD + AMI/ASG、ECS/EKS rollout、Beanstalk Blue/Green | 以版本和流量为中心，支持健康检查和快速回滚 |

题目中如果出现“严重安全漏洞、立即修复、第三方软件、自定义命令”，应优先选 **Run Command**；Maintenance Window 不是不能执行命令，而是它要求按照预先定义的维护时段运行，更适合计划内维护。

### 12.2 Run Command 紧急修复 Runbook

~~~text
发现漏洞/CVE
       -> 确认受影响节点和修复脚本
       -> 先在 staging/一台 canary 节点 dry-run
       -> SSM Run Command 按标签分批执行
       -> 输出到 CloudWatch Logs/S3
       -> 健康检查、版本验证、业务 Smoke Test
       -> 成功：扩大范围；失败：停止并回滚
       -> 记录 CloudTrail、Command ID、审批和结果
~~~

执行前提：目标节点必须是 Systems Manager managed node，SSM Agent 正常运行，实例角色或混合节点角色具有必要权限，并且私有子网具备到 Systems Manager、SSM Messages、EC2 Messages 等端点的私网路径。Run Command 不是绕过 IAM 的 SSH 替代品，调用者仍需要 `ssm:SendCommand`，目标节点也必须授权读取命令所需的 S3/Secrets 等资源。

一个按标签目标执行 Linux 自定义脚本的示例：

~~~bash
aws ssm send-command --document-name AWS-RunShellScript --targets Key=tag:Service,Values=inventory --parameters commands='sudo /opt/oms/bin/security-hotfix.sh --verify' --comment "Emergency security hotfix" --timeout-seconds 600 --output-s3-bucket-name oms-ssm-command-output
~~~

生产脚本要求：

- 使用幂等逻辑，重复执行不会破坏服务；
- 明确退出码，任一步骤失败都返回非零状态；
- 不在命令参数、输出、日志中打印密码、Token 或完整证书私钥；
- 使用标签、Resource Group 或明确实例清单，禁止默认对全账户节点执行；
- 配置 `MaxConcurrency`、`MaxErrors`、超时和通知，避免一次性打满整个集群；
- 记录脚本版本、变更单、操作者、目标节点和验证结果。

### 12.3 Maintenance Window 计划内升级

Maintenance Window 可以注册 Run Command、Automation、Lambda 或 Step Functions 任务。适合例行补丁、驱动升级、软件安装和可预知的维护窗口。典型流程是：

1. 使用 Patch Manager 扫描缺失补丁和合规状态。
2. 在 staging 目标验证补丁、重启行为和应用健康检查。
3. 创建 Maintenance Window，配置 cron/rate、时区、持续时间和 cutoff。
4. 先摘除一个节点的负载，再运行 Patch Manager/Run Command，重启并验证。
5. 通过 ALB/服务发现健康检查后放回流量，再处理下一批节点。
6. 维护窗口结束前保留足够的 cutoff 时间，不在窗口末尾启动无法完成的长任务。

Maintenance Window 解决“什么时候执行、怎样分批执行”；它不替代 Patch Manager 的补丁基线，也不替代应用发布系统的版本管理。

### 12.4 Patch Manager 的边界

Patch Manager 通过 patch baseline 定义批准或拒绝的补丁，并使用操作系统包管理器、Windows Update 等机制执行扫描和安装。它非常适合标准 OS 安全补丁与合规报表；但对于任意第三方软件的紧急自定义升级、复杂配置迁移或厂商专用安装器，仍可能需要 Run Command/Automation 自定义脚本。

特别注意：Patch Manager 不是通用的 major-version upgrade 工具，也不会替代应用的蓝绿发布。对 Windows Server 的应用补丁支持还受到 Microsoft 应用范围限制；升级前要验证操作系统、软件仓库、重启和回滚能力。

### 12.5 “升级/降级服务”不能只靠 Run Command

如果这里的“服务”指 OMS 的 Order、Inventory、Payment 应用版本，正确的降级路径应是：

~~~text
新版本镜像/AMI
       -> staging 验证
       -> Canary 或 Blue/Green
       -> 逐步切流
       -> 指标异常则恢复旧版本流量
       -> 保留旧镜像/AMI 直到观察窗口结束
~~~

不要直接在生产节点上用 Run Command 修改 JAR、运行时或配置后把它当作正式版本发布。紧急修复可以先用 Run Command 止血，但事后必须把修复固化到 Docker image、AMI、启动模板或正式发布包，否则 Auto Scaling 新实例会丢失修复，形成配置漂移。

数据库降级尤其要谨慎：应用可以先采用 backward-compatible 的 expand-contract schema 变更，再回滚应用版本；如果旧版本无法读取新字段或新约束，不能只切回旧镜像。OMS 的订单、库存、支付和 Outbox/Inbox 数据还需要保证幂等、事件兼容和对账可恢复。

### 12.6 运维升级的观测与审计

| 层面 | 指标/日志 | 目的 |
|---|---|---|
| SSM | Command ID、成功/失败、执行时长、目标节点、退出码 | 确认命令是否真正执行 |
| 主机 | Agent 状态、CPU、内存、磁盘、重启、服务状态 | 发现补丁或脚本对节点的影响 |
| 负载均衡 | HealthyHostCount、5xx、TargetResponseTime | 判断节点能否重新接流量 |
| 应用 | error rate、P95/P99 latency、业务成功率、Outbox age | 确认升级没有破坏订单链路 |
| 安全审计 | CloudTrail、Session Manager 日志、SSM 输出到 CloudWatch/S3 | 追踪谁在什么时候对哪些节点执行了什么 |
| 回滚 | 版本、AMI、镜像 digest、DB migration version | 确认回滚对象和数据兼容性 |

### 12.7 POC 落地边界

当前 POC 使用本地 H2 和应用进程，尚未接入 SSM Agent、Run Command、Maintenance Window、Patch Manager 或真实节点池。本节属于生产运维 Runbook：infrastructure-live 负责 SSM/IAM/Endpoint/日志桶，CI/CD 负责镜像或 AMI，应用发布负责版本切换，业务团队负责订单、库存、支付和数据库迁移的兼容性验收。

### 12.8 官方参考

- Systems Manager Run Command：https://docs.aws.amazon.com/systems-manager/latest/userguide/run-command.html
- Run Command 控制台执行：https://docs.aws.amazon.com/systems-manager/latest/userguide/running-commands-console.html
- Maintenance Windows：https://docs.aws.amazon.com/systems-manager/latest/userguide/maintenance-windows.html
- Patch Manager：https://docs.aws.amazon.com/systems-manager/latest/userguide/patch-manager.html
- Patch baseline：https://docs.aws.amazon.com/systems-manager/latest/userguide/patch-manager-create-a-patch-baseline.html
- AWS-RunPatchBaseline：https://docs.aws.amazon.com/systems-manager/latest/userguide/patch-manager-aws-runpatchbaseline.html
