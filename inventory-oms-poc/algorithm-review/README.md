# Hot100 算法与 OMS / DevSecOps 场景回顾

来源：`Leetcode hot100(Python)`。目录覆盖哈希、双指针、滑动窗口、子串、普通数组、矩阵、链表、二叉树、图论、回溯、二分查找、栈、堆、贪心、动态规划、多维动态规划和技巧。

这套代码不是把 LeetCode 算法直接塞进生产系统，而是抽取“数据结构 + 不变量 + 复杂度”作为工程设计模型。生产实现仍要考虑 Redis 原子性、数据库唯一约束、Kafka/SQS 投递语义、权限、审计和故障恢复。

## 1. 题型到业务能力

| Hot100 模式 | 真正适合的业务场景 | 复杂度 | 工程边界 |
|---|---|---:|---|
| 哈希表：两数之和、字母异位词、最长连续序列 | 支付幂等键、provider reference、对账 join key、去重 | 平均 O(n) 时间，O(n) 空间 | 不替代数据库 unique constraint；多实例必须共享状态 |
| 滑动窗口：无重复子串、最小覆盖子串、窗口最大值 | API 限流、重放窗口、消费速率统计、最近一段时间的异常计数 | O(n) 时间 | Redis 生产实现用 Lua + ZSET/Stream，保证原子性 |
| 双指针：移动零、合并区间前处理 | 原地过滤无效事件、排序流的边界扫描 | O(n) 或 O(n log n) | “接雨水”不等于 HPA 资源碎片计算，只能作为双指针思想类比 |
| K 路归并/堆：合并 K 个升序链表、前 K 高频、数据流中位数 | 微信/支付宝/银联结算流合并、Outbox/账单差异 Top-K | O(N log K) 或 O(N log K) | 流输入必须有明确排序键、tie-breaker 和 watermark |
| 图论：岛屿数量、课程表、Trie | Terraform/Kubernetes 依赖拓扑、数据血缘、敏感字段跨区域路径检查 | O(V+E) | Terraform 本身已有依赖图；自定义工具用于 lint/审计，不替代 Terraform |
| BFS/DFS | 影响面分析、故障依赖遍历、数据出境路径发现 | O(V+E) | 图节点必须带 region、data_class、owner 等治理元数据 |
| 链表：环检测、LRU | LRU 本地缓存；Saga 用图/SCC 更准确，不应把分布式事务直接当链表 | LRU get/put O(1) | 分布式死循环需要超时、状态机版本、最大重试和对账 |
| 栈：有效括号、字符串解码、每日温度 | 配置/DSL 结构校验、单调栈找下一次阈值、日志解析 | O(n) | JSON/JWT 生产使用安全解析库，不自己解析支付 token |
| 二分查找 | 查找满足 SLO 的最小容量、日志时间范围、版本定位 | O(log n) | 目标函数必须单调；扩容不一定满足单调假设 |
| 贪心：跳跃游戏、区间划分、股票 | 发布窗口合并、优先级队列调度、分批迁移策略 | 通常 O(n) 或 O(n log n) | 必须证明贪心选择性质，不能把它泛化成任意扩容策略 |
| DP：零钱兑换、跳跃、最长递增子序列 | 离散支付渠道成本规划、容量/套餐组合、有限预算优化 | 依问题而定，例如 O(target × options) | 这是规划/推荐模型，不直接决定真实支付路由 |
| 多维 DP：编辑距离、最长公共子序列 | 规则版本差异、配置 drift 相似度、字段 schema 变更比较 | O(mn) | 不用于密码学或支付签名校验 |
| 矩阵/数组 | 资源/区域 × 时间的容量表、日历维护窗口、前缀统计 | 常见 O(n)、O(n log n) | 先确认数据是否真的适合稠密数组 |

## 2. 这份代码实现的高价值样例

```text
algorithm-review/
  python/
    oms_algorithms.py       # 类型注解、dataclass、可直接运行的断言 demo
  go/
    go.mod
    oms_algorithms.go       # package algorithms，标准库实现
  cpp/
    CMakeLists.txt
    oms_algorithms.cpp      # C++17，标准库实现和 demo main
  README.md
```

三种语言保持相同的领域命名：

- `sliding_window_allow`：模拟单实例限流/重放窗口；生产换成 Redis 原子脚本。
- `deduplicate_keys`：模拟幂等 key 的 HashSet；生产最终约束仍在 Order/Payment 数据库。
- `merge_settlement_streams`：K 路有序结算流归并，适合 reconciliation。
- `topological_order`：检查 IaC 或数据管道是否有环，并给出部署/处理顺序。
- `find_cross_region_violation`：在数据血缘图中寻找从允许区域到禁止区域的路径。
- `LruCache`：本地热点配置/风控规则二级缓存，O(1) get/put。
- `min_channel_cost`：离散成本规划的 DP 示例，不代表真实支付 provider 自动路由。

## 3. 支付链路如何使用这些模式

1. 请求到达 Order：HashSet 思维用于快速判断当前进程是否见过 key，但真正幂等由 `idempotency_key` 唯一约束保证。
2. Payment capture：使用稳定 `provider_ref` 和 provider request id；超时进入 `PAYMENT_UNKNOWN`，先查询状态，不能盲目重试。
3. Outbox relay：使用 Outbox 表的状态和重试次数；不要用内存链表代替持久化事件。
4. Reconciliation：按 `(occurred_at, provider, transaction_id)` 对多个结算流做 K 路归并，再用 HashMap 建索引找差异。
5. 数据隐私：先给血缘图节点加 `region`、`data_class`、`retention`、`owner`，再用 BFS 找越境路径；算法只发现路径，审批和访问控制仍由治理平台负责。

## 4. 要主动纠正的类比

- “接雨水计算 Pod 资源碎片”不是严谨生产算法。HPA 依据 CPU、内存、RPS、队列延迟等指标和控制器策略。
- “Saga 是环形链表”不准确。Saga 是状态机/有向图；环检测应使用 DFS 颜色标记、拓扑排序或 SCC，并结合超时和补偿上限。
- XOR 只能解决非常特定的成对数据问题，不能直接处理亿级支付对账。真实对账应使用 provider transaction id、排序归并、Hash join、窗口聚合和可重放流水。
- Min Stack 不负责 Secrets Manager 版本回滚。密钥轮换使用 KMS/Secrets Manager 的版本和 IAM 审批；栈算法最多用于本地配置解析。
- “Fibonacci 预测连接池”不是容量规划依据。生产用历史指标、SLO、压测、队列模型和 autoscaling policy。

## 5. 运行方式

Python：

```bash
python python/oms_algorithms.py
```

Go：

```bash
go test ./...
```

C++：

```bash
cmake -S cpp -B cpp/build
cmake --build cpp/build
cpp/build/oms_algorithms
```


