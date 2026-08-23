# JVM AutoTuner 测试计划

## 测试策略：Shift-Left

> **Shift-left 核心原则**：把发现问题的时机前移到代码提交阶段，而不是等到集成或生产环境。
>
> | 层级 | 执行时机 | 工具 | 反馈延迟 |
> |---|---|---|---|
> | L0 单元/包 | 每次 `git push`，CI 自动触发 | `go test` | < 30 秒 |
> | L1 Kind E2E | PR merge 后，或 Runner 支持 privileged 时自动触发 | Go + Kind | < 5 分钟 |
> | L2 Perf/故障注入 | 手动触发，Sprint 周期性执行 | JMeter/wrk + Prometheus | 15–60 分钟 |
>
> Shift-left 意味着：**L0 必须覆盖所有控制逻辑分支**，包括 Prometheus 故障、边界值、幂等性。L1 覆盖真实 Kubernetes API 链路。L2 仅用于容量取证和验收，不应是第一道防线。

---

## 范围

验证 `go-operator/jvm-autotuner` 的控制链路：

```
Prometheus heap metric -> reconcile -> Deployment JAVA_OPTS/-Xmx -> rolling update -> CR status
```

---

## 目录结构

```
test/
├── TEST_PLAN.md
├── run-tests.ps1          # Windows 本地：执行 Go 单元 + 可选 Kind E2E
├── run-tests.sh           # Linux/CI：同上
├── run-load-test.ps1      # Windows 本地：wrk 预压测 + JMeter 入口
├── collect-hpa-evidence.ps1  # Windows 本地：采集 HPA/Prometheus 证据
├── collect-hpa-evidence.sh   # Linux/CI：同上（curl 版本）
├── fixtures/
│   ├── kind/
│   │   ├── namespace.yaml
│   │   ├── rbac.yaml
│   │   ├── order-service.yaml    # busybox 启动延迟夹具
│   │   ├── hpa.yaml
│   │   └── prometheus-stub.yaml
│   ├── kafka/
│   │   └── docker-compose.kafka.yml
│   └── redis/
│       └── toxiproxy.yaml
└── load/
    ├── wrk/
    └── jmeter/
```

---

## L0：单元/包测试（Shift-Left 核心层）

**位置**：`internal/controller/jvmautotuner_controller_test.go`

执行：

```bash
cd oms-oltp-poc/go-operator/jvm-autotuner
go test ./... -count=1 -v
```

### 用例

| ID | 场景 | 通过标准 |
|---|---|---|
| U-01 | heap 高于 80% | Xmx 增加一个 `stepMB` |
| U-02 | heap 低于 40% | Xmx 减少一个 `stepMB` |
| U-03 | heap 等于 40%/80% | 按当前实现保持 no-op |
| U-04 | 达到 max/min | Xmx 不越过边界 |
| U-05 | JAVA_OPTS 缺少 Xmx | 从中点初始化并追加 Xmx |
| U-06 | 多容器 Deployment | 只修改目标容器 |
| U-07 | Prometheus 5xx/超时/空结果/非法 JSON | 不 Patch，30 秒重试 |
| U-08 | MaxHeapQuery 无数据 | 回退配置的 maxHeapMB |
| U-09 | 重复 reconcile | Xmx 不变时不重复 Patch |

### Prometheus 故障夹具（Shift-Left 优先补）

在 L0 层用 `httptest.Server` 模拟 Prometheus，无需真实集群：

```go
type metricServer struct {
    statusCode int
    body       string
}

func (s *metricServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(s.statusCode)
    _, _ = w.Write([]byte(s.body))
}

// 测试流程
// 1. Deployment 初始 JAVA_OPTS=-Xmx1024m
// 2. stub 返回 503
// 3. 调用 Reconcile()
// 4. 断言 JAVA_OPTS 仍然是 -Xmx1024m
// 5. 断言 Result.RequeueAfter == 30s
// 6. stub 切换为正常 heap metric（返回 90%）
// 7. 再次 Reconcile()
// 8. 断言 JAVA_OPTS 变成 -Xmx1280m
```

---

## L1：Kind E2E

**Runner 要求**：Docker privileged mode（`[runners.docker] privileged = true`）。

执行：

```bash
cd oms-oltp-poc/go-operator/jvm-autotuner
go test -tags=e2e ./e2e -v -count=1 -timeout 3m
```

### 用例

| ID | 场景 | 通过标准 |
|---|---|---|
| E-01 | Kind metric -> rollout | 1024m -> 1280m，Pod available，status 为 1280/90 |

### Pod 启动变慢夹具

`test/fixtures/kind/order-service.yaml` 使用 busybox 模拟 JVM 启动延迟（替代 pause 镜像）：

```yaml
containers:
  - name: order-service
    image: busybox:1.36
    command:
      - /bin/sh
      - -c
      - |
        sleep 90
        while true; do
          printf 'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK' | nc -l -p 8080
        done
    readinessProbe:
      tcpSocket:
        port: 8080
      initialDelaySeconds: 2
      periodSeconds: 5
```

验证点：`kube_pod_status_ready` 时间 - `kube_pod_created` 时间 = 启动耗时；Ready 前不接流量。

接入真实业务服务时替换为：

```yaml
image: registry.example.com/oms/order-service:${CI_COMMIT_SHA}
env:
  - name: JAVA_OPTS
    value: "-Xmx1024m"
readinessProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8081
```

---

## L2：Perf 环境全链路

### P-01 单机预压测

```bash
.\test\run-load-test.ps1 -Url http://order-service/api/health -Tool wrk -DurationSeconds 300
```

记录：QPS、p95/p99、CPU、RSS、YGC/FGC、GC pause。

### P-02 集群 JMeter 分布式压测

记录：Kafka Lag、HPA desired replicas、Pod created/ready、错误率。

### Perf 时间线取证

```bash
.\test\collect-hpa-evidence.ps1 -PrometheusUrl http://prometheus:9090
```

保存以下 Prometheus 时间序列：

```promql
kafka_consumergroup_lag{namespace="oms-prod", group="order-service"}
kube_hpa_status_desired_replicas{namespace="oms-prod", horizontalpodautoscaler="order-service"}
kube_deployment_status_replicas_available
kube_pod_created{namespace="oms-prod", pod=~"order-service-.*"}
kube_pod_status_ready{namespace="oms-prod", pod=~"order-service-.*", condition="true"}
```

计算：

```
Lag->HPA 延迟       = HPA desired replicas 增加时间 - Lag 首次越过阈值时间
扩容生效延迟         = 新 Pod Ready 时间 - HPA desired replicas 增加时间
总保护缺口           = 新 Pod Ready 时间 - Lag 首次越过阈值时间
```

---

## 故障测试

### 1. Prometheus 不可用

**测试层**：L0（httptest stub，无需集群）。见上方 Shift-Left 夹具代码。

预期：Deployment 不变，RequeueAfter=30s；恢复后继续 reconcile。

### 2. Kafka Consumer 降速

**测试层**：L2 Perf 环境（需要完整链路：Kafka -> exporter -> Prometheus -> Adapter -> HPA）。

验收不是"是否扩容"，而是完整因果链：

```
Lag 上升 -> HPA desired replicas 增加 -> 新 Pod Ready -> consumer 吞吐提升 -> Lag 回落
```

注入故障：降低 consumer 并发或暂停一个 consumer，再恢复。

### 3. Pod 启动变慢

**测试层**：L1 Kind（busybox + sleep 夹具）。

确认 ReadinessProbe 通过前不接流量，记录启动耗时。

### 4. ServiceAccount 权限不足

**测试层**：L1 Kind（最小 RBAC 夹具）。

创建只读 ServiceAccount（无 patch/update），让 Controller 使用它运行：

```yaml
# test/fixtures/kind/rbac.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: jvm-autotuner-readonly
  namespace: jvm-autotuner-e2e
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: jvm-autotuner-readonly
  namespace: jvm-autotuner-e2e
rules:
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["get", "list", "watch"]    # 故意不含 patch/update
  - apiGroups: ["jvm.oms.io"]
    resources: ["jvmautotuners"]
    verbs: ["get", "list", "watch"]
```

验证权限：

```bash
kubectl auth can-i get deployments \
  --as=system:serviceaccount:jvm-autotuner-e2e:jvm-autotuner-readonly \
  -n jvm-autotuner-e2e   # 期望: yes

kubectl auth can-i patch deployments \
  --as=system:serviceaccount:jvm-autotuner-e2e:jvm-autotuner-readonly \
  -n jvm-autotuner-e2e   # 期望: no

kubectl auth can-i update jvmautotuners/status \
  --as=system:serviceaccount:jvm-autotuner-e2e:jvm-autotuner-readonly \
  -n jvm-autotuner-e2e   # 期望: no
```

通过标准：日志含 `Forbidden`，Deployment JAVA_OPTS 未被修改，测试不因日志存在而报成功。

### 5. Redis 故障（仅当 order-service 接入 Redis 时）

**测试层**：L2，使用 Toxiproxy（`test/fixtures/redis/toxiproxy.yaml`）。

```
order-service -> Toxiproxy:6379 -> Redis:6379
```

注入延迟：`toxiproxy-cli toxic add redis-proxy -t latency -a latency=2000`

注入超时：`toxiproxy-cli toxic add redis-proxy -t timeout -a timeout=100`

若业务未实现熔断，通过标准只能为：**错误可见且快速失败**，不可声称验证了降级。

---

## L3：JVM 内存分析（Eclipse MAT Hands-On）

> **目的**：在 Perf/Staging 环境对 JVM 堆转储进行离线分析，定位内存泄漏、大对象和 GC 压力根因，为 AutoTuner 的 `-Xmx` 建议值提供实证依据。

### 前置条件

- Java 服务（如 `order-service`）运行中，可访问
- 已安装 Eclipse MAT（Memory Analyzer）：https://www.eclipse.org/mat/downloads.php
- JDK `jmap` 或 `jcmd` 可用（或 JVM 启动参数包含 `-XX:+HeapDumpOnOutOfMemoryError`）

### Step 1：获取 Heap Dump

**方法 A：JVM 启动参数（推荐，生产安全）**

```yaml
# Deployment env
env:
  - name: JAVA_OPTS
    value: >-
      -Xmx1024m
      -XX:+HeapDumpOnOutOfMemoryError
      -XX:HeapDumpPath=/tmp/heapdump.hprof
```

OOM 时自动生成；从 Pod 取出：

```bash
kubectl cp oms-prod/<pod-name>:/tmp/heapdump.hprof ./heapdump.hprof
```

**方法 B：jcmd 手动触发（不中断服务）**

```bash
# 找到 Pod 内 JVM PID
kubectl exec -n oms-prod <pod-name> -- jcmd 1 VM.heap_dump /tmp/heapdump.hprof

# 拷回本地
kubectl cp oms-prod/<pod-name>:/tmp/heapdump.hprof ./heapdump.hprof
```

**方法 C：jmap（会 STW，仅用于调试环境）**

```bash
kubectl exec -n oms-prod <pod-name> -- \
  jmap -dump:format=b,file=/tmp/heapdump.hprof 1
kubectl cp oms-prod/<pod-name>:/tmp/heapdump.hprof ./heapdump.hprof
```

### Step 2：打开 Eclipse MAT

1. 启动 Eclipse MAT（`MemoryAnalyzer.exe` 或 `./MemoryAnalyzer`）。
2. File → Open Heap Dump → 选择 `heapdump.hprof`。
3. 首次打开会自动生成索引（进度条，大文件需 1–3 分钟）。
4. 弹出向导选 **Leak Suspects Report**，点 Finish。

> 如果堆超过 4 GB，先调大 MAT 自身堆：编辑 `MemoryAnalyzer.ini`，设置 `-Xmx6g`。

### Step 3：Leak Suspects Report

MAT 自动生成嫌疑报告，关注：

| 指标 | 含义 | 阈值参考 |
|---|---|---|
| Problem 1/2/3... | 最大内存持有者 | 单一问题 > 堆的 20% 需调查 |
| Accumulated Objects | 同类对象积累 | 同类 > 10 万实例需确认是否泄漏 |
| Reference Chain | 谁持有这些对象 | 找到 GC Root |

### Step 4：Dominator Tree（定位大对象）

Window → Heap Dump Details → **Dominator Tree**

- 按 **Retained Heap** 降序排列
- 展开前 10 条，识别持有最多内存的对象树
- 右键 → **List Objects → with outgoing references** 追踪引用链

记录：

```
对象类型            Shallow Heap    Retained Heap    占比
com.example.Cache   512 KB          1.2 GB           62%
```

### Step 5：OQL 查询（精准定位）

Window → Heap Dump Details → **OQL**

查找所有 byte[] 超过 1 MB：

```sql
SELECT * FROM byte[] s WHERE s.@retainedHeapSize > 1048576
```

查找 ThreadLocal 泄漏嫌疑：

```sql
SELECT * FROM java.lang.ref.WeakReference s
  WHERE s.referent != null
```

查找特定类实例数：

```sql
SELECT COUNT(*) FROM com.example.order.OrderRequest
```

### Step 6：Histogram（类级别内存分布）

Window → Heap Dump Details → **Histogram**

- 按 **Retained Heap** 排序
- 重点关注：`char[]`、`byte[]`、`Object[]`、`HashMap$Entry[]`
- 右键某类 → **List Objects → with incoming references** 找到谁创建了这些对象

### Step 7：对比两次 Dump（泄漏确认）

1. 在负载测试开始时取第一次 dump（heapdump-t0.hprof）。
2. 30 分钟后取第二次 dump（heapdump-t1.hprof）。
3. File → **Compare Baselines**，选两个文件。
4. 增长最多的类即为泄漏嫌疑。

### Step 8：结合 AutoTuner 结论

将 MAT 分析结果与 AutoTuner 运行数据对照：

| 分析维度 | MAT 数据来源 | AutoTuner 数据来源 |
|---|---|---|
| 当前堆实际占用 | Retained Heap 总量 | Prometheus `jvm_memory_used_bytes` |
| 安全 `-Xmx` 建议值 | Retained Heap + 20% 余量 | `maxHeapMB` 配置 |
| 内存泄漏风险 | 对比两次 Dump 的增长 | 持续 reconcile 后 Xmx 是否单调增长 |
| GC 压力 | Histogram 中 char[]/byte[] 占比 | YGC/FGC 频率和 GC pause |

若 MAT 显示 Retained Heap 持续增长但无泄漏对象，可适当上调 `maxHeapMB`；若发现泄漏，应修复代码而非单纯扩大堆。

### 通过标准

- [ ] 完成一次完整 Heap Dump 采集（方法 A/B/C 之一）
- [ ] MAT Leak Suspects Report 无 Critical 问题，或已记录已知问题
- [ ] Dominator Tree 前 3 条已确认业务合理性
- [ ] 两次 Dump 对比中增长 > 50 MB/30min 的类已有解释
- [ ] 输出 `mat-analysis-<date>.md`，记录 Xmx 建议值和发现的问题

---

## CI 分工

| Job | 触发方式 | 说明 |
|---|---|---|
| `jvm-autotuner-unit-test` | 自动，push 到 `oms-oltp-poc/go-operator/jvm-autotuner/` | Go 单元测试，验证控制器逻辑 |
| `jvm-autotuner-kind-e2e` | Runner 支持 privileged 后自动；否则 manual | 验证真实 Kubernetes 控制链路 |
| `jvm-autotuner-perf` | manual | 压测 + HPA 证据采集 |
| `run-load-test.ps1` | Windows 本地 | wrk 预压测辅助脚本 |
| `collect-hpa-evidence.ps1` | Windows 本地 | Prometheus/HPA 证据采集 |

> GitLab Linux CI 不执行 `.ps1` 脚本。如需在 CI 中自动化压测证据采集，需补充对应 `.sh` 脚本或将 Prometheus 查询改为 CI `curl` 命令。

### GitLab CI 片段参考

```yaml
jvm-autotuner-unit-test:
  image: golang:1.22-bookworm
  variables:
    GOFLAGS: -mod=readonly
  cache:
    paths: [.go-cache/]
  script:
    - cd oms-oltp-poc/go-operator/jvm-autotuner
    - GOMODCACHE=$CI_PROJECT_DIR/.go-cache/mod GOCACHE=$CI_PROJECT_DIR/.go-cache/build go test ./... -count=1 -v
  only:
    changes:
      - oms-oltp-poc/go-operator/jvm-autotuner/**

jvm-autotuner-kind-e2e:
  image: golang:1.22-bookworm
  services:
    - docker:27.4.1-dind
  when: manual          # Runner 配好 privileged 后改为 on_success
  allow_failure: true   # Runner 配好后改为 false
  script:
    - apt-get install -y docker.io curl
    - curl -Lo /usr/local/bin/kind https://kind.sigs.k8s.io/dl/latest/kind-linux-amd64 && chmod +x /usr/local/bin/kind
    - curl -Lo /usr/local/bin/kubectl https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl && chmod +x /usr/local/bin/kubectl
    - kind create cluster --name jvm-autotuner-e2e
    - export KUBECONFIG=$(kind get kubeconfig --name jvm-autotuner-e2e)
    - cd oms-oltp-poc/go-operator/jvm-autotuner
    - go test -tags=e2e ./e2e -v -count=1 -timeout 3m
  after_script:
    - kind delete cluster --name jvm-autotuner-e2e || true
```

---

## L4：GC 压力与 OOM 边界

> **逻辑**：AutoTuner 通过 `jvm_memory_used_bytes / Xmx` 驱动扩缩容，但 GC 的根因不总是堆大小不足——Eden 分配速率、Old Gen 碎片、Metaspace 溢出都不能靠调大 `-Xmx` 解决。这一层的目标是区分"堆不够"和"有泄漏/分配过快"，让 AutoTuner 的决策有实证依据。

### GC-01 Eden 区高速分配（MinorGC 频繁但 heap 稳定）

**场景**：order-service 高并发处理订单，短命对象大量分配在 Eden 区，YGC 每 2 秒一次，但 OldGen 占用不高，heap used% 维持在 50%。

**测试方法（L2 Perf）**：

```bash
# JVM 启动参数加入 GC 日志
JAVA_OPTS: >-
  -Xmx1024m -Xms512m
  -XX:+UseG1GC
  -Xlog:gc*:file=/tmp/gc.log:time,uptime:filecount=5,filesize=20m
  -XX:+PrintGCDateStamps
```

压测期间采集：

```promql
# YGC 次数（Micrometer 暴露）
jvm_gc_pause_seconds_count{action="end of minor GC"}
jvm_gc_pause_seconds_sum{action="end of minor GC"}

# Old Gen 占用
jvm_memory_used_bytes{area="heap", id=~".*Old.*"}
```

**判定**：若 heap used% < 60% 但 YGC 频率 > 1次/秒，根因是 allocation rate，不是 Xmx 不足。此时 AutoTuner 不应触发 scale-up（heapPct 未超阈值）——验证 AutoTuner 在此场景下保持 no-op。

**通过标准**：AutoTuner `LastAction` 为 `no-op`；GC 日志中 Minor GC pause < 50ms。

---

### GC-02 Old Gen 占满触发 Full GC（AutoTuner 响应窗口验证）

**场景**：长时间运行后 OldGen 持续增长，Full GC STW 期间 Prometheus scrape 超时，AutoTuner 得到 no-data 响应。

**测试方法（L0 + L2）**：

L0 单元测试模拟 Prometheus 在 Full GC 窗口返回空结果：

```go
// stub 返回 status=success 但 result=[]
body: `{"status":"success","data":{"result":[]}}`
// 期望：queryPrometheus 返回 err（"no data for query"）
// 期望：Reconcile 返回 RequeueAfter=30s，Deployment 不变
```

L2 Perf 环境：在真实 Full GC 发生时观察 `oms_kafka_lag_refresh_failures_total` 是否同步增加（AdminClient 超时），以及 AutoTuner CR status 的 `LastAction` 是否保持 no-op 而非错误 scale-up。

---

### GC-03 OOMKilled（exit 137）后 AutoTuner 重启恢复

**场景**：Pod 被 Kubernetes OOMKilled，重启后 JAVA_OPTS 初始状态与 AutoTuner CR spec 可能不一致。

**测试方法（L1 Kind）**：

1. 部署 Deployment，初始 `-Xmx512m`，`maxHeapMB=2048`
2. 模拟 OOM：`kubectl exec -- /bin/sh -c "while true; do yes; done"` 触发内存耗尽
3. 观察 Pod RestartCount 增加
4. 验证 AutoTuner 下一次 Reconcile 后能正确读取新 Pod 的 JAVA_OPTS（`extractXmx` 重新解析），而不是使用上次 CR status 中缓存的旧值

**通过标准**：Pod 重启后 CR `CurrentXmxMB` 反映实际 Deployment 中的值，无静默跳过。

---

### GC-04 Metaspace OOM（AutoTuner 感知盲区）

**场景**：大量动态类加载（如频繁创建 Lambda、CGLIB代理）导致 Metaspace 耗尽，heap used% 正常，AutoTuner 不触发，Pod 因 `java.lang.OutOfMemoryError: Metaspace` 崩溃。

**测试层**：L3 MAT + 启动参数审计

验证方法：

```bash
# 检查 Metaspace 配置
kubectl exec -n oms-prod <pod> -- jcmd 1 VM.flags | grep -i metaspace

# MAT 中查看 Metaspace 相关类
# OQL：查找 ClassLoader 持有的类数量
SELECT COUNT(*) FROM java.lang.Class
```

**结论记录**：AutoTuner 当前设计不覆盖 Metaspace 溢出，需在 Deployment 模板中显式配置 `-XX:MaxMetaspaceSize=256m` 并由 JVM 参数管控，非 AutoTuner 职责。

---

## L5：启动慢的多层根因

> **逻辑**：启动慢会导致 ReadinessProbe 超时、Liveness 重启、HPA 扩容后新 Pod 长期 NotReady，实际上并没有缓解 Lag。需要区分根因来源：JVM 初始化 / Spring 容器 / Kafka rebalance / DB 连接池 / 镜像拉取。

### SLO 基线

| 组件 | 可接受 Ready 时间 | 测量方式 |
|---|---|---|
| order-service（JVM） | < 60 秒 | `kube_pod_status_ready` - `kube_pod_created` |
| inventory-service（JVM） | < 60 秒 | 同上 |
| KafkaConsumerLagMetrics refresh | < 10 秒首次刷新 | `oms_kafka_consumer_lag_last_refresh_age_seconds` |
| Redis Stream Consumer 首批消费 | < 5 秒 | `oms_seckill_stream_pending` 开始下降 |

### SU-01 JVM 堆初始化慢（-Xms 过小导致多次扩堆）

**场景**：`-Xms128m -Xmx1024m`，JVM 启动时触发多次堆扩展（每次扩展都可能触发 GC），导致 Spring Boot 初始化变慢。

**测试方法（L1 Kind）**：对比两种配置的启动耗时：

```yaml
# 配置 A：Xms 远小于 Xmx
JAVA_OPTS: "-Xmx1024m -Xms128m"

# 配置 B：Xms = Xmx（消除堆扩展开销）
JAVA_OPTS: "-Xmx1024m -Xms1024m"
```

观察：GC 日志中扩堆次数 vs Pod Ready 时间差值。

**与 AutoTuner 的关联**：AutoTuner 只改 `-Xmx`，不改 `-Xms`。若 JAVA_OPTS 包含 `-Xms`，多次 scale-up 后可能出现 `-Xmx512m -Xms1024m`（Xms > Xmx），JVM 拒绝启动。**L0 单元测试需覆盖此边界**：

```go
// U-10：JAVA_OPTS 含 -Xms，scale-up 后 Xmx 仍大于 Xms
// JAVA_OPTS = "-Xms512m -Xmx600m", stepMB=256, maxHeapMB=2048
// 期望结果：-Xmx 增加到 856m，-Xms 不变，Xmx > Xms 成立
```

### SU-02 Kafka Consumer Rebalance 延迟首次消费

**场景**：inventory-service 启动后，`KafkaConsumerLagMetrics` 和 Kafka consumer 需要完成 partition rebalance 才能开始消费，期间 Lag 会短暂上升，误触 HPA。

**测试方法（L2 Perf）**：

1. 向 topic 写入 N 条消息，然后滚动重启 inventory-service
2. 观察 rebalance 期间的 Lag 尖峰（`oms_kafka_consumer_lag` 指标）
3. 验证 HPA 是否因为短暂 Lag 尖峰扩容，新 Pod 是否参与 rebalance 导致二次中断

```promql
# rebalance 期间 lag 尖峰
oms_kafka_consumer_lag{topic="inventory.reserve"}

# consumer 刷新失败（rebalance 期间 AdminClient 可能超时）
oms_kafka_lag_refresh_failures_total
```

**通过标准**：记录 rebalance 持续时间；确认 HPA `scaleDown.stabilizationWindowSeconds` 设置足够大（建议 ≥ 300s），避免因启动 Lag 尖峰频繁扩缩。

### SU-03 Redis Stream Consumer 首次消费前积压

**场景**：`RedisSeckillStreamConsumer` 启动后先读取 pending 消息（`ReadOffset.from("0-0")`），若积压量大，首次 poll 耗时超过 `consumer-block-ms=50ms` * batchSize，导致其他 `@Scheduled` 任务排队。

**测试方法（L2 本地 + Redis）**：

```bash
# 向 Redis Stream 写入 10000 条积压消息
redis-cli XADD 'oms:seckill:stream:{SKU-001}' '*' sku SKU-001 orderId o1 userId u1 qty 1 idempotencyKey k1
# 重复 10000 次（用脚本）

# 启动 inventory-service，观察
redis-cli XLEN 'oms:seckill:stream:{SKU-001}'   # pending 消化速度
```

观察 `oms_seckill_stream_pending` Gauge 的下降曲线；计算消化速率 = batchSize / poll 延迟。

---

## L6：Kafka 分区与 Rebalance

> **逻辑**：`KafkaConsumerLagMetrics` 通过 AdminClient 采集 Lag，但 Lag 的根因可能是分区分配不均、leader 切换或分区数 < consumer 数。这一层验证 Lag 指标的准确性和 HPA 响应的合理性。

### KP-01 分区数 < Consumer 数（空闲 Consumer）

**场景**：topic `inventory.reserve` 只有 3 个 partition，但 inventory-service 部署了 5 个副本，有 2 个 consumer 永远空闲，Lag 集中在 3 个活跃 partition。

**验证方法（L2）**：

```bash
# 检查 consumer group 分配
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group <group-id>
# 期望：CONSUMER-ID 列有 2 行为 "-"（未分配）
```

**判定**：`oms_kafka_consumer_lag` 采集的是 group 级别总 Lag，不受空闲 consumer 影响；但 HPA 基于 Lag 扩容到 5+ 副本时，新 Pod 不会加速消费，应在 HPA maxReplicas 中不超过 partitionCount。

### KP-02 Partition Leader 切换（Lag 临时尖峰）

**场景**：Kafka broker 重启，导致某些 partition 的 leader 切换，consumer 短暂断开并重连，Lag 临时上升后回落。

**测试方法（L2 docker-compose）**：

```bash
# docker-compose.kraft.yml 环境中重启 broker
docker compose restart kafka

# 观察
docker exec -it kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group inventory-service
```

```promql
oms_kafka_consumer_lag{topic="inventory.reserve"}        # 期望短暂尖峰后回落
oms_kafka_lag_refresh_failures_total                     # 期望 leader 切换期间有增加
oms_kafka_consumer_lag_up{topic="inventory.reserve"}     # 期望切换期间变为 0，恢复后变为 1
```

**通过标准**：Lag 在 broker 恢复后 60 秒内回落；`oms_kafka_consumer_lag_up` 恢复为 1；`oms_kafka_lag_last_refresh_age_seconds` 不超过 2×refresh-ms。

### KP-03 AdminClient 超时导致 Lag 数据陈旧

**场景**：`KafkaConsumerLagMetrics.refresh()` 中 `adminClient.listConsumerGroupOffsets().get()` 无超时设置，Kafka 响应慢时会无限等待，阻塞 Spring `@Scheduled` 线程池，导致 `ReservationExpirationScheduler` 也停止执行。

**L0 代码审计（不需要运行）**：

```java
// 当前代码（KafkaConsumerLagMetrics.java:93）
adminClient.listConsumerGroupOffsets(groupId)
           .partitionsToOffsetAndMetadata()
           .get();   // ← 无超时，Kafka 挂起时永久阻塞
```

**建议修复**：`.get(5, TimeUnit.SECONDS)`

**L2 验证**：用 Toxiproxy 给 Kafka broker 加 5s 延迟，观察 `oms_kafka_lag_last_refresh_age_seconds` 是否持续增大，以及过期预约扫描（`ReservationExpirationScheduler`）是否也停止执行（通过日志行数验证）。

---

## L7：中间件故障——数据库慢查询与连接池

> **逻辑**：`ReservationExpirationScheduler` 每 60 秒扫描过期预约，若无索引则全表扫描，DB 连接池耗尽，Kafka consumer 线程等待连接，Lag 上升。这不是 JVM 堆问题，AutoTuner 不应响应。

### DB-01 过期预约全表扫描（索引缺失）

**测试方法（L2 本地 DB）**：

```sql
-- 验证索引存在
EXPLAIN ANALYZE
SELECT * FROM inventory_reservation
WHERE status = 'PENDING' AND expires_at < NOW();

-- 预期：Index Scan on idx_reservation_status_expires（或等效）
-- 警告：Seq Scan 表示索引缺失，expiration-scan-ms=60000 下全表扫描会锁住连接池
```

写入 100 万条过期记录，触发 scheduler，观察：

```promql
# DB 连接池耗尽
hikaricp_connections_pending{pool="HikariPool-1"}   # 期望 = 0

# Kafka Lag 不应因 DB 慢而上升
oms_kafka_consumer_lag{topic="inventory.reserve"}   # 若此时上升，说明 consumer 线程被 DB 阻塞
```

**与 AutoTuner 的关联**：验证 AutoTuner 在此场景下保持 no-op（heap 未超阈值），确认 Lag 上升的根因是 DB 而非 JVM 堆不足。

### DB-02 连接池耗尽时 AutoTuner Prometheus 查询的影响

**场景**：order-service DB 连接池耗尽，Micrometer JVM metrics 采集可能受影响（HTTP endpoint 慢响应），Prometheus scrape 超时，AutoTuner 收到 no-data。

**测试方法（L0）**：已由 GC-02 中的 L0 测试覆盖（stub 返回空结果 → no-op）。

**L2 补充**：观察 Prometheus `up{job="order-service"}` 在连接池耗尽期间是否变为 0；若变为 0，AutoTuner 应 RequeueAfter 而非误报 scale-up。

---

## L8：AutoTuner 自身竞态与边界

> **逻辑**：controller-runtime 保证同一 CR 的 Reconcile 串行执行，但在极端配置或并发外部操作下仍有需要验证的边界。

### AT-01 并发 Patch 冲突（ResourceVersion Mismatch）

**场景**：AutoTuner 与外部 CI 工具（如 kubectl rollout）同时 Patch 同一 Deployment，导致 `resourceVersion` 冲突，返回 `409 Conflict`。

**L0 测试**：mock `r.Patch` 返回 `apierrors.NewConflict(...)`，断言 Reconcile 返回 error（由 controller-runtime 触发 exponential backoff 重试），而非 swallow error 导致静默失败。

### AT-02 ReconcilePeriod 极短导致写放大

**场景**：`reconcilePeriod: "1s"`，Prometheus heap 持续在 scale-up 阈值附近振荡（如 79%→81%→79%），每次 Reconcile 都触发 Patch，导致 Deployment 持续 rolling update。

**L0 测试**：

```go
// U-11：连续两次 Reconcile，第一次 heapPct=82%（scale-up），第二次 heapPct=78%（scale-down）
// 期望：两次都触发 Patch，Deployment 有两次 rolling update
// 记录：这是当前设计的已知行为，需在 CR spec 中设置合理的阈值间距
// 建议：ScaleUpThreshold=80, ScaleDownThreshold=40（间距 40%，避免振荡）
```

**L2 验证**：观察 Deployment `kube_deployment_metadata_generation` 的增长速率；若 reconcilePeriod=1s 且阈值间距 < 10%，单小时内 Patch 次数可能 > 3600，应触发告警。

### AT-03 -Xms > -Xmx 配置校验（Shift-Left，L0）

**背景**：controller 的 `replaceXmx` 只替换 `-Xmx`，若 JAVA_OPTS 含 `-Xms`，多次 scale-down 后可能产生 `-Xms1024m -Xmx256m`，JVM 启动失败。

```go
// U-12：JAVA_OPTS="-Xms1024m -Xmx2048m"，连续 scale-down 10 次
// stepMB=256, minHeapMB=256
// 期望：当 newXmxMB 即将小于 Xms 值时，controller 应停在 Xms 值而非继续下调
// 当前代码：minHeapMB 是下限，但不感知 Xms——需确认 minHeapMB >= Xms 的运维约定
// 建议：在文档中注明 minHeapMB 必须 >= JAVA_OPTS 中所有 -Xms 值
```

### AT-04 CR 删除后 Deployment 孤儿处理

**场景**：删除 JvmAutoTuner CR，Deployment 的 JAVA_OPTS 保留最后一次 AutoTuner 写入的值，不回滚到初始值。

**L1 Kind 测试**：

1. AutoTuner 将 Xmx 从 1024m 调整到 1280m
2. 删除 CR：`kubectl delete jat <name>`
3. 验证 Deployment 的 JAVA_OPTS 仍为 `-Xmx1280m`（预期行为，控制器不回滚）
4. 验证 controller 不再 reconcile 该 Deployment（IsNotFound 返回 nil）

**通过标准**：CR 删除后不报错，Deployment 保留最后值，日志中无 panic 或无限重试。

---

## L9：遗漏盲区与振荡场景

### GC-05 SoftReference 缓存振荡（AutoTuner ping-pong）

**场景**：order-service 用 `SoftReference` 做本地缓存，GC 压力大时缓存被批量清除，heap 骤降到 35%（触发 scale-down），缓存重建后 heap 又升到 83%（触发 scale-up），形成反复 patch Deployment 的振荡。

**测试层**：L2 Perf + L3 MAT

Prometheus 观察（heap 振荡曲线）：
```promql
jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"}
```

MAT OQL 查找 SoftReference 持有的对象：
```sql
SELECT * FROM java.lang.ref.SoftReference s WHERE s.referent != null
```

**与 AutoTuner 的关联**：若 `ScaleDownThreshold=40, ScaleUpThreshold=80`，阈值间距只有40%，SoftRef 缓存清除后一次 reconcile 触发 scale-down，缓存重建后下一次 reconcile 触发 scale-up，产生不必要的 rolling update。**建议**：`ScaleDownThreshold ≤ 30`，或在 CR spec 中加 `stabilizationWindow`（当前未实现）。

---

### GC-06 Dead Loop：CPU 100%，Heap 稳定，AutoTuner 保持 no-op

**场景**：order-service 某个处理线程进入死循环（如无限重试、spinlock），CPU 飙升但对象分配率正常，heap used% 维持在 55%，AutoTuner 不应触发。

**测试层**：L0（验证 in-band → no-op）+ L2（确认 AutoTuner 在 CPU 告警时不误操作）

L0 已由 `TestNoOp_HeapInBand` 覆盖逻辑等价场景。L2 需在压测时故意让一个线程 spin，验证：

```promql
# CPU 高但 heap 稳定
process_cpu_usage{job="order-service"}        # 期望接近 1.0
jvm_memory_used_bytes{area="heap"} / ...      # 期望稳定在 50-60%
```

AutoTuner CR `LastAction` 必须为 `no-op`。死循环的根因是线程问题，不是 JVM 堆大小，调大 `-Xmx` 无效且有害。

---

### GC-07 Direct Memory OOM（AutoTuner 感知盲区）

**场景**：使用 Netty 或 NIO 的服务（如 Kafka producer 内部）耗尽堆外直接内存，抛出 `OutOfMemoryError: Direct buffer memory`，但 `jvm_memory_used_bytes{area="heap"}` 完全正常，AutoTuner 不触发。

**测试层**：L3 审计 + 启动参数检查

```bash
# 检查是否配置了 Direct Memory 上限
kubectl exec -n oms-prod <pod> -- jcmd 1 VM.flags | grep -i direct
```

MAT OQL 查找直接内存持有者：
```sql
SELECT * FROM java.nio.DirectByteBuffer
```

**结论**：Direct Memory OOM 必须由 `-XX:MaxDirectMemorySize` + JVM 监控（`jvm_buffer_memory_used_bytes`）管控，不在 AutoTuner 职责范围内。需在 Deployment 模板中显式配置：
```yaml
JAVA_OPTS: "-Xmx1024m -XX:MaxDirectMemorySize=256m"
```

---

### GC-08 StackOverflowError（AutoTuner 感知盲区）

**场景**：深度递归（如处理嵌套 JSON、树形结构）导致 `StackOverflowError`，与 heap 无关，AutoTuner 无法感知。调大 `-Xmx` 无效，需调整 `-Xss`（线程栈大小）。

**L0 代码审计**：确认 `extractXmx` 只匹配 `-Xmx`，不会误改 `-Xss`：
```go
// TestReplaceXmx/xmx_in_middle 已验证 -Xss256k 不被触碰
```

**建议**：若业务存在深递归，在 JAVA_OPTS 中加 `-Xss512k`，AutoTuner 不会影响该值。

---

### AT-05 ReconcilePeriod="0s" 导致悄悄停止轮询（L0）

**场景**：`reconcilePeriod: "0s"` 合法 Go duration，`parsePeriod("0s")` 返回 0，`ctrl.Result{RequeueAfter: 0}` 等价于不定时 requeue，AutoTuner 只在 CR 被修改时才 reconcile，**永远不再主动轮询 Prometheus**。

**L0 测试**（已在 controller test 文件中）：见 `TestParsePeriod_ZeroDuration`。

**建议修复**：`parsePeriod` 中对 `d == 0` 的情况回退到 `defaultReconcilePeriod`：
```go
if d <= 0 {
    return defaultReconcilePeriod
}
```

---

### KP-04 Consumer Group 被外部删除 → Lag 归零 → HPA 误缩容

**场景**：运维误执行 `kafka-consumer-groups.sh --delete --group inventory-service`，`KafkaConsumerLagMetrics` 的 `adminClient.listConsumerGroupOffsets` 返回空，`oms_kafka_consumer_lag` 变为 0，Prometheus Adapter 认为 Lag 已消除，HPA 将 replicas 缩到 minReplicas，但实际上消息仍在积压。

**测试方法（L2）**：
```bash
# 删除 consumer group（模拟误操作）
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --delete --group inventory-service

# 观察
# oms_kafka_consumer_lag → 归零（假象）
# oms_kafka_consumer_lag_up → 0（标记不可用）— 正确行为
# HPA desired replicas → 是否因此缩容？
```

**通过标准**：`oms_kafka_consumer_lag_up` 归零时，HPA 告警规则不应基于 `lag=0` 触发缩容，应等待 `lag_up=1` 恢复后再评估。建议在 HPA ScaleDown 策略中设置 `stabilizationWindowSeconds ≥ 300`。

---

### DB-03 InventoryOutboxEvent 轮询无复合索引（慢查询）

**背景**：`InventoryOutboxEvent` 实体有 `status` 和 `createdAt` 字段，但代码中无 `@Index(columnList="status,created_at")` 注解。Outbox 轮询查询（如 `WHERE status='PENDING' ORDER BY created_at`）在数据量大时变为全表扫描。

**测试方法（L2 本地 DB）**：
```sql
EXPLAIN ANALYZE
SELECT * FROM inventory_outbox_event
WHERE status = 'PENDING'
ORDER BY created_at
LIMIT 100;
-- 期望：Index Scan
-- 警告：Seq Scan 表示缺少索引
```

写入 50 万条 `PUBLISHED` 记录后重新执行，观察 `seq_scan` 次数和执行时间。若无索引，扫描时间随数据量线性增长，触发 DB 连接池排队 → Kafka consumer 线程阻塞 → Lag 上升。

**建议**：在 `InventoryOutboxEvent` 上加：
```java
@Table(indexes = @Index(columnList = "status,next_attempt_at"))
```

---

### RS-01 Redis Seckill DLQ 满 → 库存 Quota 不一致

**背景**：`RedisSeckillStreamConsumer` 在消息超过 `maxDeliveryAttempts=8` 次后写入 DLQ，同时调用 `inventoryService.releaseSeckillQuota(sku, qty)` 释放库存。若 DLQ 长度超过 `dlqMaxLength=10000`（TRIM），后续失败消息的 quota 仍被释放，但 DLQ 记录被丢弃，无法审计。

**测试方法（L2 本地）**：

```bash
# 向 stream 写入 10001 条必然失败的消息（如 sku 不存在）
# 观察：
redis-cli XLEN 'oms:seckill:dlq:{SKU-TEST}'   # 期望不超过 10000（TRIM 生效）
redis-cli HGET 'oms:seckill:attempts:{SKU-TEST}' <msg-id>  # 期望空（已清理）
```

Prometheus 观察：
```promql
oms_seckill_dlq_depth{sku="SKU-TEST"}          # 不应无限增长
oms_seckill_stream_pending{sku="SKU-TEST"}     # 应归零（消息已处理）
```

**通过标准**：DLQ 深度受 `dlqMaxLength` 约束；每次进入 DLQ 时 `oms_seckill_stream.dead_letter` 计数器增加；超出 DLQ 上限的记录需有告警（当前无）——建议加 Prometheus alerting rule：`oms_seckill_dlq_depth > dlqMaxLength * 0.9`。

---

### TH-01 线程死锁（互相等待锁）

**场景**：order-service 两个线程交叉获取 Lock-A 和 Lock-B，循环等待，所有业务线程 BLOCKED，QPS 归零，但 heap used% 维持正常，AutoTuner 不应触发。

**测试层**：L0（逻辑等价由 `TestNoOp_HeapInBand` 覆盖）+ L2（`jstack` 验证）

L2 注入：
```bash
kubectl exec -n oms-prod <pod> -- jcmd 1 Thread.print | grep -A5 "deadlock"
```

Prometheus 观察（线程全 BLOCKED，CPU ≈ 0，heap 稳定）：
```promql
jvm_threads_states_threads{state="blocked"}   # 期望 > 总线程数 80%
process_cpu_usage{job="order-service"}         # 期望接近 0
jvm_memory_used_bytes{area="heap"} / ...       # 稳定，no-op
```

**通过标准**：AutoTuner `LastAction = no-op`；jstack 含 `Found 1 deadlock`；告警规则需加 `jvm_threads_deadlocked > 0`（调大 -Xmx 无法解锁死锁）。

---

### TH-02 线程池耗尽（无界队列 → OOM）

**场景**：DB 慢查询（索引缺失，结合 DB-01）占满所有 HikariCP 线程，Tomcat 请求入无界 TaskQueue，队列堆积触发 Heap OOM。链路：慢查询 → 连接池满 → 请求线程排队 → 队列无界 → heap 上升 → OOM。

**测试层**：L2 Perf + L3 MAT

Prometheus：
```promql
hikaricp_connections_active{pool="HikariPool-1"}   # = maximumPoolSize
hikaricp_connections_pending{pool="HikariPool-1"}  # 持续增长
jvm_threads_states_threads{state="waiting"}        # Tomcat 线程等 DB 连接
jvm_memory_used_bytes{area="heap"} / ...           # 队列堆积后上升
```

MAT OQL（定位队列堆积对象）：
```sql
SELECT COUNT(*) FROM org.apache.tomcat.util.threads.TaskQueue
SELECT * FROM java.util.concurrent.LinkedBlockingDeque
```

**与 AutoTuner 的关联**：若队列堆积导致 heap 越过 scaleUpThreshold，AutoTuner 会触发 scale-up，但调大 -Xmx 只是延迟 OOM，根因是无界队列 + 连接池满。**连续 scale-up ≥ 3 次且 Kafka Lag 未下降，应告警：根因可能是连接池/队列耗尽，而非堆不足**。

---

### TH-03 软引用/弱引用 Reference Queue 积压

**场景**：`ConcurrentHashMap<String, SoftReference<OrderDTO>>` 本地缓存在 GC 压力不足时不被清理，Reference Queue 积压，GC 效率下降。

**测试层**：L3 MAT

MAT OQL：
```sql
-- 仍持有 referent 的 SoftReference（GC 未清理）
SELECT s, s.referent.@retainedHeapSize FROM java.lang.ref.SoftReference s
  WHERE s.referent != null

-- WeakReference 积压（ThreadLocal 泄漏常见模式）
SELECT * FROM java.lang.ref.WeakReference s WHERE s.referent != null
```

结合 GC 日志观察 `[SoftReference, N refs, Xs]` 中 N 与 GC pause 的相关性。

**建议**：调低 `-XX:SoftRefLRUPolicyMSPerMB`（默认 1000ms/MB → 200ms/MB），缩短软引用存活时间。AutoTuner 不响应此场景，分析报告中记录即可。

---

## L10：TCP / 网络层故障

> **逻辑**：TCP 层故障不影响 JVM Heap，但会导致 Prometheus scrape 失败（AutoTuner 收到 no-data）、Kafka AdminClient 超时（Lag 陈旧）和 Kubernetes 健康检测失败（Pod 重启）。这一层验证 AutoTuner 在网络故障下的鲁棒性。

### NET-01 文件描述符耗尽（TIME_WAIT 风暴）

**场景**：高频短连接导致大量 socket 处于 `TIME_WAIT`，文件描述符耗尽（`too many open files`），AutoTuner 向 Prometheus 的 HTTP 请求被拒绝。

**注入方式（L1 Kind）**：
```yaml
# securityContext 限制 fd 上限
securityContext:
  sysctls:
    - name: net.core.somaxconn
      value: "32"
```

```bash
# 宿主机观察 TIME_WAIT 数量
ss -s | grep TIME-WAIT
```

**与 AutoTuner 关联**：fd 耗尽时 HTTP 请求失败（`dial tcp: too many open files`），由 L0 `TestPrometheus503_NoOpAndRequeue` 覆盖逻辑路径（503 → no-op + RequeueAfter=30s）。L2 实际触发，验证日志含该错误且进程不崩溃。

---

### NET-02 半开连接（SYN 等待超时）

**场景**：`iptables DROP` 模拟网络分区，TCP SYN 指数退避（3s→6s→12s→24s，共 ~75s），超过 liveness probe `timeoutSeconds`，触发 Pod 重启。

**注入与恢复（L1 Kind）**：
```bash
# 注入：阻断 8080 入流量
kubectl exec -n jvm-autotuner-e2e <node-pod> -- \
  iptables -A INPUT -p tcp --dport 8080 -j DROP

sleep 30   # 等待 liveness 触发重启

# 恢复
kubectl exec -n jvm-autotuner-e2e <node-pod> -- \
  iptables -D INPUT -p tcp --dport 8080 -j DROP
```

观察：
```bash
kubectl get events -n jvm-autotuner-e2e --field-selector reason=Killing
kubectl describe pod <pod> | grep -A5 "Liveness probe failed"
```

**通过标准**：Pod 重启后 AutoTuner 正确读取新 Pod 的 JAVA_OPTS（AT-04 逻辑路径，无缓存污染）。

---

### NET-03 K8s 控制平面完整握手链路（取证记录）

> 不是自动化测试，而是控制平面恢复时 TCP/TLS 握手链路的完整取证，用于架构评审和技术深度说明。

**从物理机启动到 `kubectl` 返回结果的完整协议栈时间线**：

```
阶段 1 — 故障注入
  Kubelet inotify 感知 manifests/kube-apiserver.yaml 变更
  → containerd Stop + Create gRPC 请求
  → 新 API-Server 连接旧 etcd IP → 连接失败 → CrashLoopBackOff

阶段 2 — 故障扩散
  Kubelet 心跳（每 10s）PATCH node.status → 超时
  Scheduler / Controller-Manager → CrashLoopBackOff
  kubectl get nodes → connection refused
```

```
阶段 3 — 手动修复（关键协议栈事件）
  ① TCP 三次握手（RTT ~0.5ms）
       SYN  ─────────────────────→ etcd:2379
       SYN-ACK  ←───────────────────
       ACK  ─────────────────────→
       内核 TCP 状态机：SYN_SENT → ESTABLISHED

  ② TLS 1.3 握手（1-RTT，~1ms）
       ClientHello（cipher suites, random, SNI）  ──→
       ←── ServerHello + Certificate + EncryptedExtensions + Finished
       Client Finished  ──────────────────────────→
       此后所有 gRPC 帧均 AES-256-GCM 加密

  ③ mTLS 双向证书验证（~0.5ms）
       API-Server → 出示 apiserver-etcd-client.crt
       etcd       → 出示 etcd/server.crt
       双方验证 cluster CA（etcd/ca.crt）签名

  ④ gRPC / HTTP2 SETTINGS 帧交换（流控协商）
       SETTINGS（max_concurrent_streams, initial_window_size）↔

  ⑤ etcd Raft 线性一致读（quorum 确认）
       API-Server → /health linearizable read
       etcd Leader 确认 ≥ 2/3 节点响应 → cluster_id 校验匹配
       → API-Server 加载 /registry 集群状态，6443 端口开始监听

  Kubelet 心跳成功 PATCH node.status → etcd
  kubectl get nodes → LIST → Ready
```

```
阶段 4 — Scheduler 资源修复（Cgroup 前置分配）
  Kubelet 感知 kube-scheduler.yaml 变更（cpu: 1000m → 100m）
  Linux Cgroups 分配成功 → Scheduler 进程启动
  Scheduler → API-Server 6443（重复阶段 3 的 TCP/TLS 握手流程）
  → Leader Election（etcd 分布式锁）→ 注册为调度器领导者
  kubectl get pod -n kube-system → 全部 1/1 Running
```

**记忆图谱（递进依赖）**：
```
systemd → Kubelet → inotify 监听 manifests/
  → 拉起 API-Server（静态 Pod）
  → TCP 三次握手 + TLS 1.3 + mTLS + gRPC → etcd
  → Raft quorum 确认 → 加载集群状态
  → API-Server 6443 监听
  → Kubelet 心跳成功 → Node Ready
  → Scheduler TCP+TLS → API-Server → Leader Election
  → 全部 Running
```

**一句话点睛**：总开关是 **Kubelet**，核心桥梁是 **API-Server ↔ etcd 的 mTLS+gRPC 连接**，最后一公里是 **Cgroup 资源前置分配**。

---

## 阻断标准

L0/L1 任一失败、发生 OOM、Kafka Lag 持续增长、Ready 前接流量、错误率超 SLO、MAT 发现未解释的内存泄漏，均**阻断发布/容量结论**。

没有真实时间序列证据或 MAT 分析报告时，容量结论只能标记"**未验证**"。
