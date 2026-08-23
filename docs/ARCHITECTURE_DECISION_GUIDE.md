# Architecture Decision Guide

架构决策指南：基于本 POC 组合的技术选型地图。

---

## 一、整体分层模型

```
┌─────────────────────────────────────────────────────────────┐
│                     OLTP 层（事务/当下状态）                    │
│   oms-oltp-poc / inventory-oms-poc                          │
│   FastAPI + SQLite / Spring Boot + MySQL                    │
│   关键词：ACID、高并发、库存预占、Saga补偿、Outbox               │
└──────────────────────────┬──────────────────────────────────┘
                           │ CDC / Kafka Outbox 事件
┌──────────────────────────▼──────────────────────────────────┐
│                     数据治理层（契约/质量）                      │
│   data-governance-poc                                       │
│   关键词：数据契约、鲜度检查、漂移检测、对账                       │
└──────────────────────────┬──────────────────────────────────┘
                           │ Bronze → Silver → Gold
┌──────────────────────────▼──────────────────────────────────┐
│                     OLAP 层（历史/聚合）                        │
│   oee-data-platform / cce-feature-platform                  │
│   关键词：Medallion建模、特征工程、Dashboard、ML特征             │
└──────────────────────────┬──────────────────────────────────┘
                           │ 特征/推理/知识
┌──────────────────────────▼──────────────────────────────────┐
│                     AI/自动化层                                │
│   knowledge-cockpit / KB(RAG)                               │
│   关键词：向量检索、Bedrock、GenAI、企业知识库                    │
└─────────────────────────────────────────────────────────────┘
```

**核心原则**：OLTP 做的是"现在对不对"，OLAP 做的是"过去发生了什么"，AI 层做的是"知识怎么用"。
边界不是技术边界，是**一致性需求的边界**。

---

## 二、Architecture Tiers：L / M / H

| 维度 | Light | Medium | Heavy |
|---|---|---|---|
| Compute | Lambda (Python/Java SnapStart) | EKS 2–3 replicas | EKS 3AZ × 3+ + Karpenter |
| Database | Aurora Serverless v2 + RDS Proxy | Aurora PG + Read Replica | Aurora HA + PgBouncer |
| Messaging | SQS FIFO | SQS FIFO / EventBridge | MSK Kafka (acks=all) |
| Cache | — | ElastiCache Redis | Redis Cluster (hash tag) |
| Seckill | ❌ 不适合 | ⚠ ~1K TPS (Redis Lua) | ✅ 标准大促设计 |
| 秒杀冷启动风险 | Lambda + ACU 扩容各有 1-2min 延迟 | Redis Lua 门控可抵挡 | Redis Cluster 分片抗峰 |
| 代表 POC | oms-oltp-poc (SQLite 原型) | inventory-oms-poc (Spring Boot) | arch.html 生产蓝图 |

---

## 三、中间件存在的意义：每加一层解决什么

### Redis — 换延迟和并发

| 场景 | 原因 |
|---|---|
| 库存预占（高并发） | `DECR` 原子操作 < 1ms；MySQL 行锁高并发下排队 |
| 幂等键去重 | `SET NX EX` 天然"只执行一次" |
| Seckill Lua 五步原子 | HGET→SISMEMBER→GET→DECRBY→XADD，单 Slot，无跨槽风险 |

底层数据结构：String = SDS；ZSet = **跳表 O(log N)**；Hash Tag 保证同 SKU 的所有键落同一 Slot。

**代价**：缓存击穿、雪崩、DB 双写一致性。加 Redis 的触发器是并发 > 500 TPS 且对延迟敏感。

---

### Kafka (MSK) — 换解耦和持久化异步

| 场景 | 原因 |
|---|---|
| Outbox 事件发布 | 事务提交与事件发布原子性，不丢消息 |
| OLTP → OLAP CDC 桥 | 下游 OEE/CCE 按自己节奏消费，互不影响 |
| Saga 补偿协调 | 服务间通过 Topic 协调，无强耦合 RPC |

底层数据结构：分区日志（追加写 + 顺序读），磁盘顺序写比随机写快约 100x。

SQS vs EventBridge vs MSK 选型：

| 维度 | EventBridge | SQS FIFO | MSK Kafka |
|---|---|---|---|
| 顺序保证 | 无 | Group 级（5min 窗口） | Partition 级，严格 |
| 回放 | Archive 最长 1 年 | 不支持 | 任意 Offset |
| 吞吐 | ~10K/s | ~3K/s | 百万/s |
| 适合 | 管理事件、低频路由 | 支付回调、幂等交付 | Seckill、OLAP 数据流 |

---

### Elasticsearch — 换全文检索和复杂查询

底层数据结构：**倒排索引**（词 → 文档列表），词查找 O(1)，空间换时间。

适合场景：订单全文搜索、日志分析（ELK）、知识库混合检索（BM25 + 向量）。

代价：JVM 内存开销大；写入近实时非实时；需要额外同步管道（Canal/Logstash）。

---

### 向量数据库（pgvector / Weaviate）— 换语义相似度检索

底层数据结构：**HNSW 图**（Hierarchical Navigable Small World），近似最近邻，查询 O(log N)。

适用于 RAG / 特征检索场景。RDBMS B+树无法高效计算嵌入向量余弦距离。

---

## 四、什么时候用 Chaos Mesh

| 阶段 | 是否引入 | 原因 |
|---|---|---|
| 开发阶段 | ❌ | 先把功能做对 |
| 集成测试 | ✅ | 验证 Saga 补偿、Outbox 重试路径 |
| 预生产/Staging | ✅ | 验证 HPA、PreStop 钩子、优雅关闭 |
| 生产（Game Day） | ✅ | 有限爆炸半径的受控演练 |

**核心判断**：多服务依赖 + 有状态事务流程（Saga）+ K8s 自动化运维（HPA/PreStop）时引入。

本 POC 对应场景（`cce-feature-platform/chaos_testing/`）：

| 故障类型 | 验证目标 | 对应模式 |
|---|---|---|
| Pod Kill | Outbox 消息重启后不丢失 | Outbox 持久性 |
| 网络延迟注入 | Redis 超时后库存锁正确释放 | Saga 补偿路径 |
| 网络分区 | Kafka 断开后订单状态机正确等待 | 幂等键 + 重试 |
| CPU/内存压力 | HPA 在正确时机扩容 | HPA 配置验证 |

`cce-deployment-with-prestop.yaml`：验证 Pod 被驱逐时正在处理的请求能否优雅完成。

---

## 五、数据结构与架构的映射

### 栈（Stack / LIFO）

Saga 补偿天然是栈结构：

```
执行：Step1 → Step2 → Step3（失败）
补偿：Step2_rollback → Step1_rollback   ← 栈的 Pop 顺序
```

---

### 堆（Heap / Priority Queue）

| 场景 | 原因 |
|---|---|
| 超时订单处理（`/api/reservations/expire`） | 最小堆按 `expires_at` 排序，O(1) 找最近过期，O(log N) 插入 |
| Kafka 多分区消费合并 | 堆合并多分区有序消息流（归并排序变体） |
| Cron Job 时间轮 | 最小堆找下一个最早触发的任务 |

生产级实现应用最小堆维护优先队列，而不是全表扫描 `expires_at < NOW()`。

---

## 五-A、数据过期清理策略

### 1. Redis Key TTL — 内存层自动过期

Redis 的 `SET key value EX seconds` 在 TTL 到期后由后台线程惰性删除（访问时检查）+ 定期扫描主动删除。

```
幂等键：SET idempotency:{key} 1 EX 300        # 5 分钟去重窗口
Seckill 库存：SET stock:{sku} {qty} EX 7200   # 活动结束后 2 小时自动清理
Session Token：SET session:{id} {data} EX 1800
```

**风险点**：Seckill 活动期间 `maxmemory-policy` 绝对不能设 `allkeys-lru`，否则库存 key 可能被提前驱逐，导致超卖。监控指标：`redis_evicted_keys_total > 0` 期间 → P1 告警。

---

### 2. 预订超时 → Saga 补偿流程

```
预订创建
  └─ inventory_reservations.expires_at = NOW() + 15min
     └─ outbox_events: reservation.created

[Scheduler 每 60s 扫描，生产用最小堆]
  └─ 找到 expires_at < NOW() AND status = RESERVED

超时触发补偿
  └─ available_stock += qty          # 归还库存
  └─ reservation.status = EXPIRED
  └─ order.status = CANCELLED
  └─ outbox_events: inventory.released + order.cancelled
```

**关键**：库存归还和状态变更必须在同一个事务里，否则出现"订单取消了但库存没还"的幽灵库存。

```sql
-- 原子性过期处理（单条 SQL，避免多步竞争）
UPDATE inventory_reservations
SET status = 'EXPIRED'
WHERE expires_at < NOW()
  AND status = 'RESERVED'
RETURNING sku_id, quantity, order_id;
-- 同事务内归还 available_stock，写 outbox
```

---

### 3. Outbox 事件清理

Outbox 表不能无限增长，已投递的事件需要归档或删除。

| 策略 | 适用场景 | 做法 |
|---|---|---|
| 定期删除 | 不需要回放，只要送达 | `DELETE WHERE status='SENT' AND sent_at < NOW()-7d`（每日 CronJob） |
| 归档到冷存储 | 需要审计/回放 | 先 INSERT INTO outbox_archive，再 DELETE from outbox，保留 90 天 |
| 分区表裁剪 | 高吞吐场景 | 按月分区，`DROP PARTITION` 比全表 DELETE 快 100x |

**Outbox 继续监控**：清理不能删掉 `status=PENDING` 或 `IN_FLIGHT` 的事件，只清理 `SENT` 且超过保留期的。

```sql
-- 安全的 Outbox 清理（只动已完成的）
DELETE FROM order_outbox_event
WHERE status = 'SENT'
  AND sent_at < NOW() - INTERVAL '7 days';
```

---

### 4. 数据库历史数据冷热分离

OLTP 表（`customer_order`、`inventory_movements`）随时间膨胀，影响查询性能。

```
热数据（近 90 天）  → Aurora 主库，正常索引查询
温数据（90天-1年）  → Aurora Read Replica / 归档表，只读
冷数据（> 1 年）    → S3 Parquet + Athena，按需查询，成本降 95%
```

**触发 CDC 的时机**：归档操作本身也应写 Outbox 事件，让下游 OLAP（OEE/CCE Bronze 层）知道数据已移出主库，避免对账漏项。

**实现参考**：

```sql
-- 分区表示例（按月）
CREATE TABLE customer_order_2026_08
  PARTITION OF customer_order
  FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

-- 超过保留期后直接 DROP PARTITION（瞬间完成，不锁表）
ALTER TABLE customer_order DETACH PARTITION customer_order_2024_01;
DROP TABLE customer_order_2024_01;
```

---

### 5. 各层清理策略汇总

| 层级 | 数据类型 | 清理机制 | 保留策略 |
|---|---|---|---|
| Redis | 幂等键、Seckill 库存 | TTL 自动过期 | 5min – 2h |
| OLTP DB | 预订记录、Outbox 事件 | CronJob 定期删除/归档 | Outbox 7天，订单 90天热 |
| OLAP Bronze | CDC 原始事件 | Delta Lake VACUUM | 保留 30 天快照 |
| OLAP Gold | 特征/聚合表 | 增量更新，不删除历史 | 永久保留（SCD Type 2） |
| 日志/Traces | CloudWatch / X-Ray | 自动过期策略 | 日志 30 天，Traces 7 天 |

---

### 跳表（Skip List）

Redis ZSet 底层、LevelDB/RocksDB MemTable。有序 + O(log N) 范围查询，并发控制粒度比红黑树细。

---

### B+ 树

MySQL InnoDB 主键和二级索引的底层。

- 联合索引遵循最左前缀原则（节点排序方式决定）
- `BETWEEN` / `ORDER BY` 走索引（叶节点双向链表）
- 随机写慢于顺序写（页分裂）→ Kafka 选择追加日志的根本原因

---

### LSM 树（Log-Structured Merge Tree）

写优化，适合时序/事件流场景。ClickHouse、Cassandra、Kafka 底层存储思想来源。

OEE 传感器数据写入选 ClickHouse 而非 MySQL 的根本原因：时序数据是追加不修改，LSM 写入吞吐远优于 B+树。

---

## 六、行业选型推演

### 决策树

```
Q1：有没有瞬时并发尖峰（秒杀/抢票）？
  → 有 ──→ Heavy（Redis Lua + MSK 削峰）
  → 没有 → Q2

Q2：有没有跨服务事务一致性（支付 + 库存原子性）？
  → 有 ──→ 至少 Medium（Saga + Outbox + SQS FIFO）
  → 没有 → Light 即可

Q3：有没有数据回放/审计要求（监管/BI/ML）？
  → 有 ──→ MSK（可任意回放）> SQS（不能回放）
  → 没有 → SQS FIFO 够用

Q4：有没有跨境/多地域部署？
  → 有 ──→ PII 合规层 + VPC 隔离 + 数据分类（L1/L3）
  → 没有 → 单 Region 架构

Q5：是否有时序/非结构化数据大量写入？
  → 设备传感器/日志 → 时序 DB（ClickHouse/InfluxDB）替代 Redis
  → 文本/向量检索 → 向量 DB（pgvector/Weaviate）替代 ES
```

---

### 行业对照表

| 行业 | Light | Medium | Heavy | 核心驱动力 |
|---|---|---|---|---|
| 电商/零售 | 初创 < 1万DAU | 成长期 1-50万DAU | 大促/平台级 > 50万 | 秒杀并发 |
| 酒店/OTA | 中小型连锁 PMS | 区域 OTA 平台 | 国际 OTA（跨境支付） | PII 合规 + 跨境监管 |
| 工业制造/OEE | 单厂小型 | 多厂中型（Kafka 设备流） | 大型离散制造（Flink + Delta） | 时序数据密度 |
| 金融/支付 | — | 小型支付服务商 | 银行核心 / 互联网支付 | 幂等审计 + 双分录 |
| 企业知识库/GenAI | S3 + Bedrock 托管 RAG | EKS + OpenSearch + Bedrock | 私有化 VPC + 私有模型 | 数据安全合规 |

**规律**：驱动 Tier 跃升的往往不是流量，而是**监管要求**（金融/跨境）或**业务模式**（秒杀/大促）。

---

## 七、POC 项目速查

| POC | 对话场景 | 核心故事 |
|---|---|---|
| `oms-oltp-poc` | OLTP 架构、Saga、Outbox、幂等 | Python 快速原型，ACID 事务 + 补偿路径 |
| `inventory-oms-poc` | Java/Spring Boot DDD、微服务边界 | 企业级 Controller/Service/Repository 分层 |
| `data-governance-poc` | 数据契约、数据 SRE、鲜度检查 | 监控 Outbox 与库存状态，确保 OLAP 可信 |
| `oee-data-platform` | 工业 OLAP、Medallion 建模 | 设备 OEE 指标 Bronze→Silver→Gold |
| `cce-feature-platform` | CDP、特征工程、实时特征存储 | 客户标签 + 分群 + Campaign 决策 |
| `knowledge-cockpit` | GenAI、RAG、企业知识库 | 多云部署 + Bedrock + 向量检索 |
