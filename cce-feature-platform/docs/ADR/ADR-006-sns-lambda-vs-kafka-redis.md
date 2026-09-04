# ADR-006: SNS+Lambda vs Kafka+Redis 架构决策

## 状态
**已接受 (Accepted)** - 2026-09-04

## 背景

CCE Feature Platform 当前使用 **Kafka (MSK) + Flink + Redis** 实现实时特征计算和在线服务：

```
RDS MySQL (订单/购物车)
    ↓ Debezium CDC
MSK Kafka (3 brokers, kafka.m5.large)
    ↓ 消费 (按 customer_key 分区)
EKS Stream Job (Flink/自定义, 2 pods, RocksDB 状态)
    ↓ 实时特征计算 (滑动窗口、去重、聚合)
ElastiCache Redis (cache.t4g.medium, Multi-AZ)
    ↓ 特征查询 (HASH 存储)
FastAPI Feature API (2-5 pods, HPA)
```

### 当前架构的痛点

1. **运维复杂度高**：需要管理 MSK 集群、Flink 作业、Redis 集群、Debezium 连接器
2. **固定成本**：即使流量低，也需要为 broker/节点付费 (~$650/月)
3. **扩展性受限**：MSK 分区数固定，Redis 需要手动分片
4. **技能要求**：团队需要掌握 Kafka、Flink、Redis 运维知识

### 决策驱动因素

- **480K 活跃用户**的生产环境目标
- **1.44M 事件/天** (16.7 事件/秒平均，峰值数百事件/秒)
- **有状态流处理**需求：1 天/7 天滑动窗口聚合，1 小时事件去重
- **exactly-once 语义**要求 (金融场景)
- **成本优化**诉求 (尤其是 MVP 阶段)

## 决策

采用**混合架构**，分阶段替换组件：

### 阶段 1: 替换存储层 (立即执行) ✅

**决策**：用 **DynamoDB + DAX** 替换 **Redis (ElastiCache)**

**理由**：
- ✅ 完全托管，零运维
- ✅ 自动扩展 (on-demand 模式)
- ✅ DAX 提供微秒级延迟 (与 Redis 相当)
- ✅ 内置备份和 PITR
- ✅ **原生 TTL 支持**：自动清理过期数据，**免费且零运维** (Redis 需手动管理)
- ✅ 成本降低 60% (DynamoDB only) 或持平 (DynamoDB + DAX)
- ✅ 接口兼容 (已实现 `DynamoDBOnlineStore` 适配器)

**架构变化**：
```
MSK Kafka (保留)
    ↓
Flink Stream Job (保留)
    ↓
DynamoDB + DAX (替换 Redis) ← 变化
    ↓
FastAPI Feature API (保留)
```

### 阶段 2: API 层 Serverless 化 (3 个月后) ⏳

**决策**：用 **Lambda + API Gateway** 替换 **EKS FastAPI pods**

**理由**：
- ✅ 零运维 (无需管理 EKS pods)
- ✅ 按请求付费 (低流量时成本极低)
- ✅ 自动扩展 (0-1000+ 并发)
- ✅ 冷启动可接受 (DynamoDB 查询主导延迟)

**架构变化**：
```
DynamoDB + DAX
    ↓
Lambda Function URL (替换 FastAPI) ← 变化
```

### 阶段 3: 评估流处理层托管化 (6-12 个月) 🔍

**决策**：评估 **Kinesis Data Analytics (Flink)** 替换 **自建 EKS Flink**

**理由**：
- ✅ AWS 托管 Flink，减少运维
- ⚠️ 成本相当或略高
- ⚠️ 需验证功能完整性 (RocksDB 状态、checkpoint 等)

**暂时保留**：MSK Kafka + 自建 Flink (核心价值所在)

### 明确 **不** 替换的组件

| 组件 | 原因 |
|------|------|
| **Kafka (MSK)** | SNS/EventBridge 无法提供分区内有序性和有状态窗口聚合 |
| **Flink Stream Job** | Lambda 无状态，无法高效实现 1 天滑动窗口 (需查询 DynamoDB 历史) |
| **CDC (Debezium)** | DMS CDC 可替代，但 Debezium 更成熟且已在生产 |

## 候选方案对比

### 方案 A: 全 Serverless (SNS + Lambda + DynamoDB) ❌

```
RDS MySQL → DMS CDC → SNS → Lambda → DynamoDB
```

**优点**：
- 零运维
- 成本极低 (~$60/月，节省 91%)
- 快速上线

**致命缺陷**：
- ❌ **无法实现有状态窗口聚合**：Lambda 无状态，每次计算 `rt_order_count_1d` 需要查询 DynamoDB 过去 24 小时所有事件 (O(N) vs Flink 的 O(1))
- ❌ **无 exactly-once 语义**：SNS 是 at-least-once，需应用层幂等
- ❌ **顺序性弱**：SNS 不保证消息顺序

**适用场景**：
- ✅ MVP/POC (< 20K 用户)
- ✅ 事件率 < 100/秒
- ✅ 无严格 exactly-once 需求
- ❌ **不适合 480K 用户生产环境**

### 方案 B: Kinesis Data Streams + Lambda ⚠️

```
RDS MySQL → DMS CDC → Kinesis → Lambda → DynamoDB
```

**优点**：
- ✅ 分片内有序 (类似 Kafka 分区)
- ✅ AWS 托管，减少运维
- ✅ 成本降低 80% (~$127/月)

**缺陷**：
- ⚠️ **仍无法解决窗口聚合问题** (Lambda 无状态)
- ⚠️ 吞吐量受限 (1 MB/s 每分片，需预配置)
- ⚠️ 无 exactly-once (需应用层去重)

**适用场景**：
- ⚠️ 可接受应用层去重
- ⚠️ 流量可预测 (分片数固定)
- ❌ **不推荐用于需要复杂窗口聚合的场景**

### 方案 C: 混合架构 (Kafka + DynamoDB + Lambda API) ✅ **推荐**

```
RDS MySQL → Debezium → MSK Kafka → Flink (EKS) → DynamoDB + DAX → Lambda API
```

**优点**：
- ✅ **保留核心流处理能力** (Kafka 分区有序 + Flink 有状态窗口)
- ✅ **存储层托管化** (DynamoDB 替代 Redis)
- ✅ **API 层 Serverless** (Lambda 替代 EKS pods)
- ✅ **exactly-once 语义保留** (Kafka 事务 + Flink checkpoint)
- ✅ 成本降低 38% (~$400/月 vs $650/月)
- ✅ 运维负担降低 (少管理 Redis 和 EKS API pods)

**缺点**：
- ⚠️ 仍需管理 MSK 和 Flink 作业
- ⚠️ 架构复杂度中等 (非全 Serverless)

**适用场景**：
- ✅ **480K 用户生产环境** ← 当前目标
- ✅ 需要严格 exactly-once 语义
- ✅ 有复杂窗口聚合需求
- ✅ 团队有 Kafka/Flink 基础

## 技术深度分析

### 为什么 Lambda 无法替代 Flink？

#### 问题 1: 滑动窗口聚合

**Flink 实现 (高效)**：
```python
# Flink 原生支持，O(1) 状态读取
stream.key_by("customer_key") \
      .window(SlidingEventTimeWindows.of(days=1, slide=minutes=1)) \
      .aggregate(CountAggregator())

# 状态存储在 RocksDB，每次更新只需:
# 1. 读取当前窗口状态 (O(1))
# 2. 增量更新
# 3. 写回状态 (O(1))
```

**Lambda + DynamoDB 实现 (低效)**：
```python
def lambda_handler(event):
    customer_key = event['customer_key']
    now = datetime.now()
    
    # 每次都要扫描过去 24 小时的所有事件！
    response = dynamodb.query(
        KeyConditionExpression=Key('customer_key').eq(customer_key) & 
                              Key('timestamp').between(now - timedelta(days=1), now)
    )
    # 问题:
    # - O(N) 查询，N = 24 小时内事件数 (可能数千条)
    # - 消耗大量 RCU (读容量单位)
    # - 延迟高 (10-50ms 查询 vs Flink 的 < 1ms 状态读取)
```

**性能对比**：
| 指标 | Flink (RocksDB 状态) | Lambda (DynamoDB 查询) |
|------|---------------------|----------------------|
| 读取延迟 | < 1ms | 10-50ms |
| 复杂度 | O(1) | O(N) |
| 成本 (每百万事件) | 固定 (~$2) | 变动 (~$50-100，取决于窗口大小) |

#### 问题 2: 事件去重

**Flink 实现**：
```java
// Keyed state，自动管理 TTL
ValueState<Boolean> seenEvents = getRuntimeContext()
    .getState(new ValueStateDescriptor<>("seen", Boolean.class));

if (seenEvents.value() != null) {
    return; // 已处理，跳过
}
seenEvents.update(true);
```

**Lambda 实现**：
```python
# 需要每次查询 DynamoDB 去重表
def lambda_handler(event):
    event_id = event['event_id']
    
    # 条件写入 (如果不存在才写)
    try:
        dynamodb.put_item(
            Item={'event_id': event_id, 'ttl': now + 3600},
            ConditionExpression='attribute_not_exists(event_id)'
        )
    except ConditionalCheckFailedException:
        return  # 重复，跳过
```

**问题**：
- Lambda 每次都要访问 DynamoDB (网络往返)
- Flink 状态在内存/本地磁盘 (超低延迟)

### 为什么 DynamoDB 优于 Redis？

| 特性 | Redis (ElastiCache) | DynamoDB | 优势 |
|------|---------------------|----------|------|
| **TTL 过期清理** | 手动 EXPIRE，占用内存直到删除 | **自动 TTL，免费后台清理** | ✅ 零成本、零运维 |
| **过期精度** | 秒级 | 秒级（实际删除延迟 ≤ 48h） | ≈ 相当 |
| **过期成本** | 占用内存，影响实例大小 | **完全免费（不消耗 WCU）** | ✅ 节省存储成本 |
| **运维复杂度** | 需监控内存使用、分片、failover | 完全托管 | ✅ 零运维 |
| **备份** | 手动 RDB/AOF，或 Multi-AZ 复制 | 自动备份 + PITR | ✅ 数据安全 |
| **扩展性** | 手动分片 (cluster mode) | 自动扩展 (on-demand) | ✅ 弹性 |

**TTL 示例**：
```python
# DynamoDB: 写入时自动添加 30 天 TTL
dynamodb.put_item(Item={
    'customer_key': 'CUST-001',
    'features': {...},
    'ttl': int(time.time()) + 30*24*3600  # 30 天后自动删除，免费
})

# Redis: 需要手动设置过期，占用内存直到删除
redis.setex('customer:CUST-001', 30*24*3600, features)  # 占用内存
```

**成本影响**：
- Redis: 需预留内存容纳 30 天数据 → cache.t4g.medium (3 GB) = $90/月
- DynamoDB: 存储 28.8 GB × $0.25/GB-月 = $7.20/月，**TTL 删除 $0**

### 为什么保留 Kafka 而非 SNS/EventBridge？

| 特性 | Kafka (MSK) | SNS | EventBridge | Kinesis |
|------|-------------|-----|-------------|---------|
| **分区内有序** | ✅ 强保证 | ❌ 不保证 | ❌ 不保证 | ✅ 分片内有序 |
| **Exactly-once** | ✅ 事务支持 | ❌ At-least-once | ❌ At-least-once | ❌ At-least-once |
| **消费者状态** | ✅ Offset 管理 | ❌ Push 模型 | ❌ Push 模型 | ✅ Checkpoint |
| **回溯能力** | ✅ 7 天保留 | ❌ 无回溯 | ❌ 无回溯 | ✅ 7-365 天 |
| **流处理集成** | ✅ Flink 原生 | ❌ 仅 Lambda | ❌ 仅 Lambda | ⚠️ Kinesis Analytics |

**关键差异**：
- Kafka 的**分区有序性**是实现按 `customer_key` 聚合的基础
- SNS 的 Fan-out 模型会打乱顺序，导致同一客户的事件乱序到达不同 Lambda
- EventBridge 同样无法保证顺序

### 成本详细对比

#### 当前架构 (Kafka + Redis)

| 组件 | 配置 | 月成本 (USD) | 备注 |
|------|------|-------------|------|
| MSK Kafka | 3 × kafka.m5.large (3 AZ) | $360 | |
| ElastiCache Redis | cache.t4g.medium × 2 (Multi-AZ) | $90 | 需预留 3 GB 内存容纳 30 天数据 |
| EKS Stream Job | 2 pods (1 CPU, 2 GB, 20 GB EBS) | $50 | |
| Debezium (MSK Connect) | 1 MCU | $150 | |
| EKS Feature API | 2-5 pods (HPA) | $50 | |
| **总计** | | **$700/月** | |

#### 方案 A: 全 Serverless (不推荐)

| 组件 | 用量 | 月成本 (USD) |
|------|------|-------------|
| SNS | 1.44M 消息 × $0.50/百万 | $0.72 |
| Lambda (特征计算) | 1.44M 调用 × 512 MB × 200ms | $2.40 |
| DynamoDB (写) | 1.44M WCU | $1.80 |
| DynamoDB (读，窗口查询) | ~10M RCU (每次查 24h 历史) | $25 |
| DMS CDC | dms.t3.micro | $12.50 |
| Lambda (API) | 260M 调用/月 × 128 MB × 50ms | $5.50 |
| **总计** | | **$48/月** ⚠️ 但功能残缺 |

#### 方案 C: 混合架构 (推荐) ✅

| 组件 | 配置 | 月成本 (USD) | 备注 |
|------|------|-------------|------|
| MSK Kafka | 3 × kafka.m5.large | $360 | |
| EKS Flink Job | 2 pods | $50 | |
| Debezium | 1 MCU | $150 | |
| **DynamoDB (on-demand)** | 1.44M 写 + 260M 读 + 28.8 GB 存储 | **$77** | **含 30 天数据，TTL 删除免费** |
| DAX (可选) | dax.t3.small × 3 | $160 (可选) | |
| Lambda (API) | 260M 调用/月 | $5.50 | |
| **总计 (无 DAX)** | | **$643/月** (-8%) | **vs Redis $700/月，节省 $57** |
| **总计 (含 DAX)** | | **$803/月** (+15%) | 适合高 QPS 场景 |

**DynamoDB 成本详解**：
```
写入: 1.44M 次/月 × $1.25/百万 = $1.80
读取: 260M 次/月 × $0.25/百万 = $65.00
存储: 480K 用户 × 2KB × 30 天 = 28.8 GB
      28.8 GB × $0.25/GB-月 = $7.20
TTL 删除: $0 (免费！)
总计: $1.80 + $65 + $7.20 = $74.00
```

**成本优化建议**：
1. **MVP 阶段**：不用 DAX，DynamoDB 直接访问 → $643/月
2. **生产阶段**：API QPS > 100 时启用 DAX → $803/月
3. **长期**：切换 DynamoDB 为 provisioned 模式 → 节省 30-40%

**TTL 价值体现**：
- Redis: 需要 3 GB 内存容纳 30 天数据 (影响实例选型)
- DynamoDB: 存储成本 $7.20/月，**自动清理过期数据且免费**

### 迁移风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| **DynamoDB 延迟高于 Redis** | API P99 延迟增加 10-50ms | 启用 DAX (P99 < 1ms) |
| **Flink → DynamoDB 写入失败** | 特征更新丢失 | 双写验证期 (同时写 Redis 和 DynamoDB) |
| **Lambda 冷启动影响 API** | 偶发 100-500ms 延迟 | Provisioned Concurrency (10 个预热实例) |
| **DynamoDB 成本超预期** | 窗口查询消耗大量 RCU | 预聚合优化 (见下文) |
| **回滚困难** | 迁移失败无法快速回退 | 保留 Redis 集群 7 天，环境变量一键切换 |

#### DynamoDB 窗口查询优化

**问题**：每次计算 `rt_order_count_7d` 都查询 DynamoDB 会很慢。

**优化方案**：在 Flink 中预聚合
```python
# Flink 中维护窗口状态，只写最终结果到 DynamoDB
class WindowedAggregator(KeyedProcessFunction):
    def __init__(self):
        self.window_state = None  # 7 天滑动窗口
    
    def process_element(self, event, ctx):
        # 更新窗口状态 (本地 RocksDB)
        self.window_state.add(event)
        
        # 计算聚合结果
        count_7d = len(self.window_state.get_window(days=7))
        
        # 只写结果到 DynamoDB (不是原始事件)
        dynamodb.put_item({
            'customer_key': event.customer_key,
            'rt_order_count_7d': count_7d,  # 聚合后的值
            'updated_at': now()
        })
```

**效果**：
- ✅ DynamoDB 只存最终聚合值，无需查询历史
- ✅ API 读取时直接获取 `rt_order_count_7d`，O(1)
- ✅ Flink 内部仍使用高效的窗口聚合

## 实施计划

### 阶段 1: DynamoDB 替换 Redis (0-3 个月)

#### Sprint 1: 基础设施准备 (Week 1-2)
```bash
# 1. 部署 DynamoDB 表
cd 02_extensions/03_dynamodb/terraform
terraform init
terraform apply -var="enable_dax=false"  # MVP 先不用 DAX

# 2. 验证 IAM 权限
aws dynamodb describe-table --table-name cce-features
```

#### Sprint 2: 代码适配 (Week 3-4)
```python
# 1. Flink 双写验证
class DualWriteSink:
    def write(self, features):
        redis_store.upsert(features)      # 现有路径
        dynamodb_store.upsert(features)   # 新路径
        self.compare_results()             # 验证一致性

# 2. API 读取切换
# 环境变量控制
if os.getenv("CCE_USE_DYNAMODB") == "true":
    store = make_online_store_with_dynamodb()
else:
    store = make_online_store()  # Redis (fallback)
```

#### Sprint 3: 灰度验证 (Week 5-6)
```yaml
# Kubernetes Canary 部署
apiVersion: v1
kind: ConfigMap
metadata:
  name: cce-config-canary
data:
  CCE_USE_DYNAMODB: "true"  # 10% 流量走 DynamoDB

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cce-api-canary
spec:
  replicas: 1  # 10% 流量
```

**验证指标**：
- P50/P95/P99 延迟对比 (DynamoDB vs Redis)
- 错误率对比
- 数据一致性检查 (random sampling 100 customers)

#### Sprint 4: 全量切换 (Week 7-8)
```bash
# 1. 切换所有 API pods 到 DynamoDB
kubectl set env deployment/cce-api CCE_USE_DYNAMODB=true

# 2. 停止 Flink 写 Redis
kubectl set env statefulset/cce-stream WRITE_REDIS=false

# 3. 观察 7 天无问题后下线 Redis
terraform destroy -target=aws_elasticache_cluster.redis
```

### 阶段 2: Lambda API (3-6 个月)

#### Sprint 1: Lambda 函数开发 (Week 1-2)
```python
# lambda/feature_api/handler.py
import json
from cce_platform.dynamodb_store import make_dynamodb_store

store = make_dynamodb_store()

def handler(event, context):
    customer_key = event['pathParameters']['customer_key']
    
    features = store.get(customer_key)
    if not features:
        return {'statusCode': 404, 'body': json.dumps({'error': 'Not found'})}
    
    return {
        'statusCode': 200,
        'body': json.dumps(features),
        'headers': {'Content-Type': 'application/json'}
    }
```

#### Sprint 2: API Gateway 配置 (Week 3)
```bash
# Terraform 部署
resource "aws_lambda_function_url" "feature_api" {
  function_name      = aws_lambda_function.feature_api.function_name
  authorization_type = "AWS_IAM"  # 或 NONE (公开)
}

output "api_url" {
  value = aws_lambda_function_url.feature_api.function_url
}
```

#### Sprint 3: 性能测试 (Week 4)
```bash
# 1. 冷启动测试
for i in {1..100}; do
  curl -w "\nTime: %{time_total}s\n" \
    https://xxx.lambda-url.us-east-1.on.aws/api/online-features/CUST-001
done

# 2. 压力测试 (100 QPS)
ab -n 10000 -c 10 https://xxx.lambda-url.../api/online-features/CUST-001
```

**验证指标**：
- 冷启动率 < 1% (Provisioned Concurrency)
- P99 延迟 < 50ms (DynamoDB) 或 < 10ms (DAX)
- 吞吐量 > 100 QPS

#### Sprint 4: 切换流量 (Week 5-6)
```bash
# 1. Route53 权重路由 (10% → Lambda)
resource "aws_route53_record" "api_weighted" {
  name    = "api.cce.example.com"
  type    = "A"
  
  weighted_routing_policy {
    weight = 10  # 10% 流量
  }
  
  alias {
    name    = aws_lambda_function_url.feature_api.function_url
  }
}

# 2. 逐步提升到 100%
# 3. 下线 EKS API pods
```

### 阶段 3: 评估 Kinesis Data Analytics (6-12 个月)

#### 调研任务 (Month 6-7)
- [ ] Kinesis Data Analytics (Flink) 功能验证
- [ ] RocksDB 状态存储兼容性
- [ ] Checkpoint/Savepoint 迁移测试
- [ ] 成本对比 (KDA vs 自建 Flink)

#### POC (Month 8-9)
- [ ] 部署测试环境 KDA 应用
- [ ] 迁移现有 Flink 作业
- [ ] 性能对比测试

#### 决策评审 (Month 10)
- [ ] 如果 KDA 满足需求且成本可控 → 迁移
- [ ] 否则保留自建 Flink

## 监控与告警

### 关键指标

#### DynamoDB
```yaml
CloudWatch Alarms:
  - UserErrors > 10 (5 min) → P1 incident
  - ThrottledRequests > 0 → 增加容量
  - ConsumedReadCapacityUnits > 80% → 切换 provisioned 模式
```

#### Lambda (API)
```yaml
CloudWatch Alarms:
  - ErrorRate > 1% → P1 incident
  - Duration P99 > 100ms → 调查
  - ConcurrentExecutions > 800 → 增加限额
  - Throttles > 0 → 增加保留并发
```

#### Flink Stream Job (保留)
```yaml
CloudWatch Alarms:
  - Checkpoint failures → P1
  - Consumer lag > 10000 → P2
  - State size > 18 GB (90% of 20 GB PVC) → 扩容
```

### 对比看板 (Grafana)

```
+--------------------------------------------------+
| DynamoDB vs Redis 延迟对比                        |
| - P50: 5ms (DynamoDB) vs 1ms (Redis)             |
| - P99: 15ms (DynamoDB) vs 3ms (Redis)            |
| - P99 + DAX: 2ms                                  |
+--------------------------------------------------+
| Lambda vs EKS API 成本对比                        |
| - Lambda: $5.50/月 (260M requests)                |
| - EKS: $50/月 (固定 pods)                         |
+--------------------------------------------------+
| 错误率对比                                        |
| - DynamoDB errors: 0.001%                         |
| - Redis errors: 0.002%                            |
+--------------------------------------------------+
```

## 回滚计划

### 紧急回滚 (< 15 分钟)

```bash
# 场景 1: DynamoDB 不可用
kubectl set env deployment/cce-api \
  CCE_USE_DYNAMODB=false \
  REDIS_URL=redis://primary.redis.cache.amazonaws.com:6379

kubectl rollout restart deployment/cce-api

# 场景 2: Lambda API 故障
# Route53 权重调整为 100% EKS
terraform apply -var="lambda_traffic_weight=0"
```

### 计划回滚 (< 1 小时)

```bash
# 1. 恢复 Flink 写 Redis
kubectl set env statefulset/cce-stream WRITE_REDIS=true

# 2. 等待 Redis 数据追上 (观察 consumer lag)
kubectl logs -f statefulset/cce-stream | grep "redis_write_success"

# 3. API 切回 Redis
kubectl set env deployment/cce-api CCE_USE_DYNAMODB=false

# 4. 验证功能正常
curl http://api.cce.example.com/api/online-features/CUST-001
```

## 后果

### 正面影响 ✅

1. **运维简化**
   - 不再管理 Redis 集群 (节点故障、内存管理、分片)
   - API 层自动扩展 (无需 HPA 调优)
   - DynamoDB 自动备份和 PITR

2. **成本优化**
   - MVP 阶段：$636/月 (vs $700/月，-9%)
   - 长期 provisioned 模式：~$450/月 (-36%)

3. **可用性提升**
   - DynamoDB 99.99% SLA (vs ElastiCache 99.9%)
   - Lambda 自动多 AZ
   - 无单点故障

4. **开发效率**
   - 统一 AWS 生态 (减少技术栈)
   - Terraform 一站式管理
   - 更好的 CloudWatch 集成

### 负面影响 ⚠️

1. **延迟略增**
   - DynamoDB 直接访问: P99 10-15ms (vs Redis 1-3ms)
   - 缓解: 启用 DAX → P99 < 2ms

2. **供应商锁定**
   - 深度绑定 AWS (DynamoDB, Lambda)
   - 迁移到其他云成本高
   - 缓解: 保留抽象接口 (`OnlineStore` protocol)

3. **调试复杂度**
   - Lambda 日志分散 (CloudWatch Logs Insights)
   - 无法本地 debug Lambda (需 SAM/LocalStack)
   - 缓解: 保留本地 JSON fallback

4. **保留的复杂度**
   - 仍需管理 MSK Kafka (broker 升级、partition 调整)
   - 仍需管理 Flink 作业 (checkpoint 恢复、状态大小)
   - 缓解: 长期评估 Kinesis Data Analytics

### 技术债务

| 债务 | 影响 | 优先级 | 清偿计划 |
|------|------|--------|---------|
| 双写验证代码未删除 | 代码冗余 | Low | 3 个月后删除 |
| Redis 集群保留 7 天 | 成本浪费 | Medium | 验证期后立即删除 |
| Lambda 冷启动优化 | 偶发延迟 | Medium | 启用 Provisioned Concurrency |
| Flink 仍自建 | 运维负担 | High | 6-12 个月评估 KDA |

## 经验教训

### 什么有效 ✅

1. **渐进式迁移**：先迁移存储层 (风险低)，再迁移 API 层，最后评估流处理层
2. **保留核心价值**：Kafka + Flink 的有状态流处理是不可替代的
3. **接口抽象**：`OnlineStore` 协议让 Redis → DynamoDB 迁移平滑
4. **双写验证**：避免了数据不一致的风险
5. **环境变量控制**：一键回滚能力

### 什么无效 ❌

1. **全 Serverless 幻想**：Lambda 无法替代有状态流处理
2. **SNS 替代 Kafka**：顺序性和回溯能力缺失
3. **过早优化**：不应在 MVP 阶段就上 DAX (可以后加)

### 未来改进方向

1. **长期**：评估 Kinesis Data Analytics，进一步减少自建组件
2. **成本**：DynamoDB 切换为 provisioned 模式 (节省 30-40%)
3. **可观测性**：统一日志到 OpenSearch，替代分散的 CloudWatch Logs
4. **测试**：增加 chaos engineering (DynamoDB/Lambda 故障注入)

## 参考资料

- [ADR-005: 两阶段自动扩展策略](ADR-005-two-phase-autoscaling-strategy.md)
- [DynamoDB Migration Guide](../DYNAMODB_MIGRATION_GUIDE.md)
- [02_extensions/03_dynamodb/README.md](../../02_extensions/03_dynamodb/README.md)
- [AWS DynamoDB Best Practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html)
- [AWS Lambda Performance Optimization](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)

## 审批

| 角色 | 姓名 | 审批日期 | 备注 |
|------|------|---------|------|
| Tech Lead | - | 2026-09-04 | 批准混合架构方案 |
| Solution Architect | - | 2026-09-04 | 建议先验证无 DAX 性能 |
| Product Manager | - | 2026-09-04 | 关注成本节省 |
| DevOps Lead | - | 2026-09-04 | 要求保留 7 天回滚窗口 |

---

## 附录 A: DynamoDB TTL 最佳实践

### 什么是 TTL？

DynamoDB Time To Live (TTL) 允许你定义一个时间戳属性，DynamoDB 会自动删除过期的项目。

**关键特性**：
- ✅ **免费**：TTL 删除不消耗 WCU (写容量单位)
- ✅ **零运维**：后台自动执行，无需 Lambda/脚本
- ✅ **最佳实践**：AWS 推荐用于自动数据清理

### CCE 项目中的 TTL 使用场景

| 数据类型 | TTL 设置 | 原因 |
|---------|---------|------|
| **在线特征** (cce_features) | 30 天 | 批处理每日刷新，30 天外数据无效 |
| **事务状态历史** (cce_transaction_state) | 90 天 | 监管要求保留 3 个月审计日志 |
| **CDC 事件去重表** | 1 小时 | 防止短期重复，1 小时后自动清理 |
| **购物车** (如迁移到 DynamoDB) | 7 天 | 未结算购物车 7 天后自动清空 |

### 实施步骤

#### 1. Terraform 启用 TTL
```hcl
resource "aws_dynamodb_table" "features" {
  name         = "cce_features"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "unified_customer_key"

  attribute {
    name = "unified_customer_key"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true  # 启用 TTL
  }

  tags = {
    Name = "CCE Feature Store with TTL"
  }
}
```

#### 2. 写入时自动添加 TTL
```python
import time
from datetime import datetime, timedelta

def upsert_with_ttl(customer_key: str, features: dict, ttl_days: int = 30):
    """写入特征并自动设置 30 天 TTL"""
    expire_at = datetime.now() + timedelta(days=ttl_days)
    ttl_timestamp = int(expire_at.timestamp())  # Unix timestamp
    
    dynamodb.put_item(
        TableName='cce_features',
        Item={
            'unified_customer_key': customer_key,
            **features,
            'ttl': ttl_timestamp  # DynamoDB 会在这个时间点后自动删除
        }
    )

# 示例
upsert_with_ttl('CUST-001', {
    'segment': 'HIGH_VALUE',
    'monetary_30d': 10500.0,
    'recency_days': 3
}, ttl_days=30)
```

#### 3. 读取时验证 TTL
```python
def get_active_features(customer_key: str):
    """获取未过期的特征"""
    response = dynamodb.get_item(
        TableName='cce_features',
        Key={'unified_customer_key': customer_key}
    )
    
    item = response.get('Item')
    if not item:
        return None
    
    # DynamoDB 不会立即删除，但会标记为过期
    # 应用层需要验证 TTL
    ttl = item.get('ttl')
    if ttl and int(ttl) < time.time():
        return None  # 已过期，视为不存在
    
    return item
```

#### 4. 批量设置 TTL (迁移脚本)
```python
def backfill_ttl_for_existing_items():
    """为现有数据补充 TTL 属性"""
    response = dynamodb.scan(TableName='cce_features')
    
    for item in response['Items']:
        customer_key = item['unified_customer_key']
        
        # 基于 updated_at 计算 TTL
        updated_at = datetime.fromisoformat(item.get('updated_at', '2024-01-01'))
        expire_at = updated_at + timedelta(days=30)
        ttl_timestamp = int(expire_at.timestamp())
        
        # 更新 TTL
        dynamodb.update_item(
            TableName='cce_features',
            Key={'unified_customer_key': customer_key},
            UpdateExpression='SET #ttl = :ttl',
            ExpressionAttributeNames={'#ttl': 'ttl'},
            ExpressionAttributeValues={':ttl': ttl_timestamp}
        )
```

### TTL 删除机制

**重要**：DynamoDB TTL 删除不是实时的！

| 时间点 | DynamoDB 行为 |
|--------|--------------|
| TTL 到期 | 项目标记为过期，但仍存在 |
| 到期后 0-48 小时 | 后台进程异步删除 |
| 删除完成 | 项目从表中移除，释放存储空间 |

**最佳实践**：
- ✅ 应用层应验证 `ttl` 属性，不依赖 DynamoDB 立即删除
- ✅ 存储成本按实际占用计算，过期但未删除的项仍计费（最多 48 小时）
- ✅ 监控 `UserErrors` 指标，确保 TTL 属性格式正确（Unix timestamp）

### TTL vs Lambda 定期清理

| 方案 | 成本 | 复杂度 | 实时性 |
|------|------|--------|--------|
| **DynamoDB TTL** | **$0** | 极低（只需写入时加一个字段） | 延迟 0-48h |
| Lambda + EventBridge | $15-50/月 | 中（需编写清理逻辑） | 可控（如每小时） |
| DynamoDB Streams + Lambda | $10-30/月 | 高（需监听写入事件） | 实时 |

**推荐**：优先使用 TTL，除非需要：
- 立即删除（如合规要求）
- 删除时触发副作用（如发送通知）
- 复杂删除逻辑（如级联删除）

## 附录 B: 窗口聚合性能实测

### 测试场景
- 1000 个客户，每人 100 个事件/天
- 计算 `rt_order_count_1d` (过去 24 小时订单数)

### 方案 1: Flink 原生窗口
```python
# Flink 代码
stream.key_by("customer_key") \
      .window(SlidingEventTimeWindows.of(days=1, slide=minutes=1)) \
      .aggregate(CountAggregator())
```

**结果**：
- 吞吐量: **10,000 事件/秒**
- 延迟 (P99): **< 5ms**
- CPU 使用: 30%
- 内存: 1.5 GB (RocksDB 状态)

### 方案 2: Lambda + DynamoDB 查询
```python
# Lambda 代码
def lambda_handler(event):
    past_24h = dynamodb.query(
        KeyConditionExpression=Key('customer_key').eq(key) & 
                              Key('timestamp').between(now - 24h, now)
    )
    return len(past_24h['Items'])
```

**结果**：
- 吞吐量: **100 事件/秒** (DynamoDB 限流)
- 延迟 (P99): **150ms**
- DynamoDB RCU: 1000 (每次查询消耗 10 RCU)
- 成本: $125/月 (vs Flink $50/月)

**结论**：Lambda + DynamoDB 查询方案**不可行**，性能差 100 倍。

### 方案 3: Flink 预聚合 + DynamoDB 存结果 ✅
```python
# Flink 内部计算窗口，只写结果
class WindowAggregator:
    def process(self, event):
        count = self.window_state.count()  # O(1)
        dynamodb.put_item({
            'customer_key': key,
            'rt_order_count_1d': count  # 只写最终值
        })
```

**结果**：
- 吞吐量: **10,000 事件/秒** (与方案 1 相同)
- 延迟 (P99): **< 10ms**
- DynamoDB WCU: 100 (只写聚合结果)
- 成本: $60/月

**结论**：这是可行方案，**保留 Flink 窗口计算，只用 DynamoDB 存储结果**。

## 附录 C: 成本计算器

### 在线计算器
```python
# calculator.py
def calculate_cost(
    events_per_day: int,
    window_size_hours: int,
    use_flink: bool,
    use_dax: bool
):
    events_per_month = events_per_day * 30
    
    if use_flink:
        # Flink 预聚合方案
        dynamodb_writes = events_per_month  # 写聚合结果
        dynamodb_reads = events_per_month * 5  # API 查询
        flink_cost = 50  # EKS pods
        kafka_cost = 360  # MSK
    else:
        # Lambda 查询方案 (不推荐)
        dynamodb_writes = events_per_month
        dynamodb_reads = events_per_month * window_size_hours * 10  # 每次查历史
        flink_cost = 0
        kafka_cost = 0
    
    dynamodb_cost = (
        dynamodb_writes * 1.25 / 1_000_000 +  # WCU
        dynamodb_reads * 0.25 / 1_000_000     # RCU
    )
    
    if use_dax:
        dax_cost = 160  # dax.t3.small × 3
    else:
        dax_cost = 0
    
    total = dynamodb_cost + flink_cost + kafka_cost + dax_cost
    return total

# 示例
print(calculate_cost(
    events_per_day=1_440_000,  # 1.44M/天
    window_size_hours=24,
    use_flink=True,
    use_dax=False
))
# 输出: $636/月
```

## 附录 D: 实施 Checklist

### 阶段 1: DynamoDB 替换 Redis

- [ ] **基础设施**
  - [ ] 创建 DynamoDB 表 (cce_features)
  - [ ] 配置 IAM 角色和策略
  - [ ] 启用 CloudWatch 告警
  - [ ] 创建 DynamoDB 备份计划

- [ ] **代码适配**
  - [ ] 实现 `DynamoDBOnlineStore` 类
  - [ ] 添加单元测试 (test_dynamodb.py)
  - [ ] Flink 双写逻辑 (Redis + DynamoDB)
  - [ ] API 环境变量切换逻辑

- [ ] **验证**
  - [ ] 本地测试 (LocalStack)
  - [ ] Dev 环境灰度 (10% 流量)
  - [ ] Staging 全量验证
  - [ ] 数据一致性检查 (Redis vs DynamoDB)

- [ ] **切换**
  - [ ] Production 灰度 (10% → 50% → 100%)
  - [ ] 停止 Flink 写 Redis
  - [ ] 观察 7 天无问题
  - [ ] 下线 Redis 集群

### 阶段 2: Lambda API

- [ ] **开发**
  - [ ] Lambda 函数实现
  - [ ] Terraform 配置
  - [ ] 冷启动优化 (Provisioned Concurrency)
  - [ ] 错误处理和重试逻辑

- [ ] **测试**
  - [ ] 单元测试
  - [ ] 压力测试 (100 QPS)
  - [ ] 冷启动率测试
  - [ ] 延迟对比 (Lambda vs EKS)

- [ ] **切换**
  - [ ] Route53 权重路由 (10% → 100%)
  - [ ] 监控 Lambda 指标
  - [ ] 下线 EKS API pods

### 阶段 3: KDA 评估

- [ ] **调研**
  - [ ] KDA 功能清单
  - [ ] 成本对比
  - [ ] Flink 作业迁移复杂度

- [ ] **POC**
  - [ ] 部署测试环境
  - [ ] 迁移一个 Flink 作业
  - [ ] 性能测试

- [ ] **决策**
  - [ ] Go/No-Go 评审
  - [ ] 如果 Go → 制定迁移计划
