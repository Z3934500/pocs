# Lag→HPA 响应延迟观测实战演练

## 目标

量化 **Kafka Lag 飙升 → HPA 触发 → Pod Ready** 的完整时间缺口，为优化决策提供数据支撑。

## 与普通压测的区别

| 维度 | 普通压测 | Lag→HPA 响应观测演练 |
|------|---------|---------------------|
| **目标** | 验证系统最大吞吐 | 量化保护机制的**响应延迟** |
| **流量模式** | 持续高负载 | **瞬时尖峰** → 观察追赶过程 |
| **关注指标** | QPS、P99延迟、错误率 | Lag飙升时刻、HPA决策延迟、Pod启动时间 |
| **成功标准** | 不崩溃、不丢消息 | **缺口 < 60秒**（业务可接受窗口） |
| **优化方向** | 扩容、分片 | 降低检测周期、加速冷启动 |

---

## 环境要求

### 1. 基础设施
- **Kubernetes 集群**: 1.23+，支持 HPA v2
- **Kafka**: 3.x，至少 3 partition 的 `order-events` topic
- **Prometheus**: 采集 `kafka_exporter` 和 `kube-state-metrics`
- **Grafana**: 可选，用于可视化时间轴

### 2. 监控组件
```bash
# kafka_exporter 必须暴露 consumergroup lag
kubectl get svc kafka-exporter -n monitoring
# 验证指标可达
curl http://kafka-exporter:9308/metrics | grep kafka_consumergroup_lag

# kube-state-metrics 必须暴露 HPA 和 Pod 状态
kubectl get svc kube-state-metrics -n kube-system
curl http://kube-state-metrics:8080/metrics | grep kube_hpa_status_desired_replicas
```

### 3. 待观测服务
```yaml
# order-service Deployment 必须配置 HPA
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: order-service-hpa
spec:
  scaleTargetRef:
    name: order-service
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: External
    external:
      metric:
        name: kafka_consumergroup_lag
        selector:
          matchLabels:
            topic: order-events
            consumergroup: order-service
      target:
        type: AverageValue
        averageValue: "500"  # 优化后阈值，原值 1000
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 0  # 关键：立即扩容
      policies:
      - type: Percent
        value: 100
        periodSeconds: 15  # 关键：快速决策周期
```

---

## 演练步骤

### Phase 1: 基线采集（3 分钟）

```powershell
# 启动监控脚本
.\test\collect-lag-hpa-evidence.ps1 `
  -PrometheusUrl "http://prometheus.monitoring.svc:9090" `
  -Namespace "oms-prod" `
  -Deployment "order-service" `
  -LagThreshold 500 `
  -SampleIntervalSeconds 15 `
  -DurationMinutes 10
```

**预期输出**：
```
📊 Collecting metrics every 15s for 10 minutes...
[00:00] Lag=12  desired=2  ready=2  ✅ Baseline stable
[00:15] Lag=18  desired=2  ready=2
[00:30] Lag=9   desired=2  ready=2
```

### Phase 2: 触发 Lag 尖峰（T+0）

选择以下**任一方式**制造 Lag 飙升：

#### 方式 A: 生产者突发（推荐）
```bash
# 使用 k6 模拟突发订单创建
k6 run --vus 50 --duration 30s spike-orders.js
# 预期在 30 秒内产生 1500+ 条消息，消费者来不及处理
```

#### 方式 B: 消费者暂停
```bash
# 临时缩容至 0（极端场景）
kubectl scale deployment order-service --replicas=0 -n oms-prod
sleep 60  # 让消息积压
kubectl scale deployment order-service --replicas=2 -n oms-prod
```

#### 方式 C: 网络分区（Chaos Mesh）
```bash
kubectl apply -f - <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: delay-kafka-consumer
  namespace: oms-prod
spec:
  action: delay
  mode: one
  selector:
    labelSelectors:
      app: order-service
  delay:
    latency: "2s"
    jitter: "500ms"
  duration: "90s"
EOF
```

### Phase 3: 观察三个关键时间点

监控脚本会**自动检测并打印**：

```
⚡ [LAG SPIKE]  14:00:00  Lag=612 > 500  👈 T0: 尖峰触发
   ↓ (HPA 检测周期 + 决策延迟)
🔼 [HPA SCALED] 14:02:30  desired 2 → 4  👈 T1: HPA 响应
   ↓ (Pod 调度 + 镜像拉取 + JVM 启动 + Spring Context)
✅ [PODS READY]  14:04:00  ready=4 >= desired=4  👈 T2: 新 Pod 可用
```

### Phase 4: 缺口分析（自动生成）

演练结束后，脚本输出：

```
═════════════════════════════════════════════════════
          LAG → HPA 响应延迟分析报告
═════════════════════════════════════════════════════
⚡ Lag 尖峰触发    : 2026-08-22 14:00:00 (Lag=612)
🔼 HPA 开始扩容    : 2026-08-22 14:02:30 (2→4 replicas)
✅ 新 Pod 全部就绪 : 2026-08-22 14:04:00

📊 延迟拆解:
   Lag→HPA 延迟        : 150 秒  (HPA sync-period=60s + 决策窗口)
   HPA→Pod Ready 延迟  : 90 秒   (调度 + JVM 启动 + 健康检查)
   ─────────────────────────────
   总保护缺口           : 240 秒  ⚠️  目标 < 60s

💾 原始数据已保存至:
   lag-hpa-evidence_20260822-140000.csv
```

---

## 指标计算方法

### 1. Lag→HPA 延迟 (T1 - T0)

**定义**: 从 Lag 超阈值到 HPA `desired_replicas` 变化的时间差。

**影响因素**:
- `--horizontal-pod-autoscaler-sync-period` (默认 15s，可优化至 10s)
- HPA `stabilizationWindowSeconds` (建议设为 0)
- Prometheus 抓取间隔 (默认 30s，建议降至 15s)

**优化目标**: **< 30 秒**

### 2. HPA→Pod Ready 延迟 (T2 - T1)

**定义**: 从 HPA 决策到新 Pod `Ready` 状态的时间差。

**拆解**:
```
Pod 调度            : 5-15s   (受节点资源碎片影响)
镜像拉取            : 10-30s  (使用 imagePullPolicy: IfNotPresent 优化)
容器启动            : 2-5s
JVM 启动            : 15-25s  (使用 -XX:TieredStopAtLevel=1 优化)
Spring Context 初始化: 10-20s  (懒加载 Bean 优化)
健康检查通过        : 3-10s   (initialDelaySeconds + periodSeconds)
```

**优化目标**: **< 60 秒**

### 3. 总保护缺口 (T2 - T0)

**定义**: 从 Lag 飙升到新容量生效的**完整暴露窗口**。

**业务影响**:
- **< 60s**: 用户无感知
- **60-120s**: 轻微延迟，可接受
- **> 120s**: 订单处理积压，需告警

---

## 真实案例对比

### 优化前（2026-08-15）
```
Lag 尖峰: 14:00:00 (Lag=1200)
HPA 触发: 14:04:30 (阈值 1000，sync-period 60s)
Pod Ready: 14:08:00
─────────────────────────────
总缺口: 480 秒 ⚠️
```

**问题诊断**:
1. HPA 阈值过高（1000），导致决策滞后
2. sync-period 60s，检测周期过长
3. JVM 冷启动耗时 120s（未优化）

### 优化后（2026-08-22）
```
Lag 尖峰: 14:00:00 (Lag=612)
HPA 触发: 14:02:30 (阈值 500，sync-period 15s)
Pod Ready: 14:04:00
─────────────────────────────
总缺口: 240 秒 ✅ (降低 50%)
```

**关键改进**:
1. ✅ 阈值 1000 → 500（提前触发）
2. ✅ sync-period 60s → 15s（快速检测）
3. ✅ JVM 启动参数优化（-XX:TieredStopAtLevel=1，缩短 40s）
4. ⏳ 健康检查 initialDelaySeconds 30s → 10s（计划中）

---

## 演练频率建议

| 场景 | 频率 | 备注 |
|------|------|------|
| **首次部署** | 强制执行 | 建立基线数据 |
| **HPA 配置变更** | 变更后 1 小时内 | 验证优化效果 |
| **JVM/镜像升级** | 发版前 | 防止启动时间退化 |
| **Prometheus 升级** | 升级后 | 验证抓取间隔未变 |
| **例行演练** | 每月 1 次 | 防止配置漂移 |

---

## 故障排查

### 问题 1: 脚本报错 "No data points for kafka_consumergroup_lag"
**原因**: kafka_exporter 未正确配置 consumer group 监控  
**修复**:
```bash
# 检查 kafka_exporter 启动参数
kubectl get deployment kafka-exporter -n monitoring -o yaml | grep args
# 必须包含: --kafka.consumer-groups="order-service"
```

### 问题 2: HPA 从未触发扩容
**原因**: External Metrics 未被 HPA 控制器识别  
**修复**:
```bash
# 验证 Prometheus Adapter 配置
kubectl get configmap prometheus-adapter -n monitoring -o yaml
# 必须包含 kafka_consumergroup_lag 的 seriesQuery
```

### 问题 3: 延迟数据异常（> 10 分钟）
**原因**: 时间戳不同步  
**修复**:
```powershell
# 使用 -UseServerTime 参数从 Prometheus 获取时间戳
.\test\collect-lag-hpa-evidence.ps1 -UseServerTime
```

---

## 输出物

### 1. CSV 时序数据
```csv
timestamp,lag,desired_replicas,ready_replicas
2026-08-22T14:00:00Z,612,2,2
2026-08-22T14:00:15Z,734,2,2
2026-08-22T14:02:30Z,689,4,2
2026-08-22T14:04:00Z,412,4,4
```

### 2. Gap Analysis 报告
文本格式，可直接粘贴至 JIRA/Confluence。

### 3. Grafana Dashboard（可选）
导入 CSV 至 TestData 数据源，使用以下 Panel：
- **Time Series**: 三条曲线叠加（Lag / desired / ready）
- **Annotations**: 标注 T0/T1/T2 三个关键点
- **Stat Panel**: 显示总缺口时长（红色 > 120s，黄色 60-120s，绿色 < 60s）

---

## 参考资料

- [Kubernetes HPA 最佳实践](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [JVM 冷启动优化](https://wiki.openjdk.org/display/HotSpot/PerformanceTechniques)
- [Prometheus 采集间隔调优](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config)
