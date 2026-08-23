# JVM AutoTuner 混沌测试 Runbook

> 执行环境：Kind 集群 `oms-e2e`。每个场景末尾都标注了等价的自动化 Go 测试函数名。

---

## 前置条件

```powershell
kind create cluster --name oms-e2e
$env:KUBECONFIG = "$(kind get kubeconfig --name oms-e2e)"
# 部署 CRD + controller + order-service
kubectl apply -f config/crd/jvmautotuner.yaml
kubectl apply -f test/fixtures/kind/namespace.yaml
kubectl apply -f test/fixtures/kind/rbac.yaml
kubectl apply -f test/fixtures/kind/order-service.yaml
kubectl apply -f config/sample/jvmautotuner-order-service.yaml
```

---

## CH-01 Prometheus 不可用

**自动化等价**：`TestPrometheus503_NoOpAndRequeue`

**注入**：`test/chaos/inject-prometheus-down.ps1`

**观察**：
```powershell
kubectl get jat -n jvm-autotuner-e2e -w
# LastAction 应保持不变，不出现 scale-up/scale-down
kubectl logs -n jvm-autotuner-e2e deploy/jvm-autotuner-controller --tail=20
# 期望日志：Prometheus query failed; will retry in 30s
```

**通过标准**：JAVA_OPTS 未变；CR status.LastAction 保持旧值；controller 未 panic。

**恢复**：`kubectl rollout restart deploy/prometheus -n monitoring`

---

## CH-02 Prometheus 慢响应（超 HTTPClient 超时）

**自动化等价**：`TestPrometheusTimeout_NoOpAndRequeue`

**注入**（需要 Toxiproxy 或 tc）：
```bash
# Kind 节点内执行（替代方法：用 tc 加延迟）
kubectl exec -n monitoring deploy/prometheus -- \
  sh -c "sleep 60" &   # 占用 Prometheus 主进程（仅用于演示）
# 生产级注入：Toxiproxy latency toxic，见 test/fixtures/redis/toxiproxy.yaml 模式
```

**观察**：同 CH-01。RequeueAfter=30s，JAVA_OPTS 不变。

**通过标准**：日志含 `context deadline exceeded` 或 `i/o timeout`；RequeueAfter=30s。

---

## CH-03 Prometheus 返回脏数据（NaN / Inf / 非法 JSON）

**自动化等价**：`TestPrometheusNaN_DocumentedNoOp`, `TestPrometheusInf_NoPanic`, `TestPrometheusMalformedJSON_NoOpAndRequeue`

**注入**（mock sidecar 替换 Prometheus endpoint）：
```powershell
# 临时将 CR 中的 prometheusURL 指向一个返回脏数据的 stub
kubectl patch jat order-service-gc-tuner -n jvm-autotuner-e2e \
  --type=merge -p '{"spec":{"prometheusURL":"http://chaos-stub.jvm-autotuner-e2e:9999"}}'

# 部署返回 NaN 的 stub（单命令）
kubectl run chaos-stub -n jvm-autotuner-e2e \
  --image=hashicorp/http-echo --port=9999 \
  -- -listen=:9999 \
  -text='{"status":"success","data":{"result":[{"value":["0","NaN"]}]}}'
kubectl expose pod chaos-stub -n jvm-autotuner-e2e --port=9999
```

**通过标准**：NaN 时 controller 不 patch，无 panic；+Inf 时 controller 不 panic（记录 gap，待加 math.IsInf guard）。

**恢复**：
```powershell
kubectl patch jat order-service-gc-tuner -n jvm-autotuner-e2e \
  --type=merge -p '{"spec":{"prometheusURL":"http://prometheus-operated.monitoring.svc.cluster.local:9090"}}'
kubectl delete pod chaos-stub -n jvm-autotuner-e2e
```

---

## CH-04 目标 Deployment 运行中被删除

**自动化等价**：`TestDeploymentNotFound_ReturnsError`

**注入**：
```powershell
kubectl delete deploy order-service -n jvm-autotuner-e2e
```

**观察**：
```powershell
kubectl logs -n jvm-autotuner-e2e deploy/jvm-autotuner-controller --tail=10
# 期望日志：get Deployment jvm-autotuner-e2e/order-service: ... not found
kubectl get events -n jvm-autotuner-e2e --sort-by=.lastTimestamp | tail -5
# 期望：controller reconcile error，exponential backoff 事件
```

**通过标准**：controller 返回 error（触发 backoff 重试），不 panic，不静默忽略。

**恢复**：`kubectl apply -f test/fixtures/kind/order-service.yaml`

---

## CH-05 containerName 与 Deployment 不匹配

**自动化等价**：`TestContainerNameMismatch_ReturnsError`

**注入**：
```powershell
kubectl patch jat order-service-gc-tuner -n jvm-autotuner-e2e \
  --type=merge -p '{"spec":{"containerName":"nonexistent"}}'
```

**通过标准**：日志含 `container "nonexistent" not found`；JAVA_OPTS 未变。

**恢复**：`kubectl patch jat order-service-gc-tuner -n jvm-autotuner-e2e --type=merge -p '{"spec":{"containerName":"order-service"}}'`

---

## CH-06 RBAC 禁止 patch Deployment

**注入**：`test/chaos/inject-rbac-deny.ps1`

**观察**：
```powershell
kubectl logs -n jvm-autotuner-e2e deploy/jvm-autotuner-controller --tail=10
# 期望：deployments.apps "order-service" is forbidden
kubectl get deploy order-service -n jvm-autotuner-e2e \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS")].value}'
# 期望：JAVA_OPTS 未变
```

**通过标准**：Forbidden 错误触发 backoff；JAVA_OPTS 不被修改。

**恢复**：`kubectl apply -f test/fixtures/kind/rbac.yaml`

---

## CH-07 并发写入导致 409 Conflict

**自动化等价**：`TestPatchConflict_ReturnsError`

**注入**（在 controller reconcile 窗口内并发 patch）：
```powershell
# 在一个终端持续触发 reconcile（改 CR annotation）
while ($true) {
  kubectl annotate jat order-service-gc-tuner -n jvm-autotuner-e2e \
    chaos-ts="$(Get-Date -Format o)" --overwrite
  Start-Sleep 1
}
# 在另一个终端并发修改 Deployment
while ($true) {
  kubectl patch deploy order-service -n jvm-autotuner-e2e \
    --type=merge -p '{"spec":{"template":{"metadata":{"annotations":{"chaos-ts":"'"$(Get-Date -UFormat %s)"'"}}}}}'
  Start-Sleep 1
}
```

**观察**：`kubectl logs ... | grep -i conflict`

**通过标准**：出现 `409 Conflict`；controller 触发 backoff 重试后最终收敛；JAVA_OPTS 最终正确。

---

## CH-08 Controller Pod 运行中被 Kill

**注入**：`test/chaos/inject-controller-kill.ps1`

**观察**：
```powershell
kubectl get pods -n jvm-autotuner-e2e -w
# 观察 controller pod 重启 → Running
kubectl get jat order-service-gc-tuner -n jvm-autotuner-e2e
# 重启后 CurrentXmxMB 应从 Deployment 重新读取（不缓存旧值）
```

**通过标准**：Pod 重启后 reconcile 正确继续；CR status 与 Deployment JAVA_OPTS 一致。

---

## CH-09 网络分区（Prometheus 端口被 DROP）

**注入**：`test/chaos/inject-network-partition.ps1`

**通过标准**：同 CH-01；日志含 `connection refused` 或 `no route to host`；RequeueAfter=30s。

---

## CH-10 MinHeapMB == MaxHeapMB（零宽调优带）

**自动化等价**：`TestMinMaxHeapEqual_AlwaysNoOp`

**注入**：
```powershell
kubectl patch jat order-service-gc-tuner -n jvm-autotuner-e2e \
  --type=merge -p '{"spec":{"minHeapMB":1024,"maxHeapMB":1024}}'
```

**通过标准**：无论 heap 百分比如何，JAVA_OPTS 中 Xmx 保持不变。

**恢复**：`kubectl patch jat order-service-gc-tuner -n jvm-autotuner-e2e --type=merge -p '{"spec":{"minHeapMB":512,"maxHeapMB":4096}}'`

---

## 故障方向总表

| ID | 组件 | 故障模式 | 预期行为 | 自动化 Go 测试 | 手动脚本 |
|---|---|---|---|---|---|
| F-01 | Prometheus | HTTP 超时 | no-op, requeue 30s | TestPrometheusTimeout_NoOpAndRequeue | CH-02 |
| F-02 | Prometheus | 非法 JSON | no-op, requeue 30s | TestPrometheusMalformedJSON_NoOpAndRequeue | CH-03 |
| F-03 | Prometheus | NaN 返回值 | no-op（IEEE 754 比较均为 false）| TestPrometheusNaN_DocumentedNoOp | CH-03 |
| F-04 | Prometheus | +Inf 返回值 | 不 panic（gap：待加 guard）| TestPrometheusInf_NoPanic | CH-03 |
| F-05 | Prometheus | 503/空结果 | no-op, requeue 30s | TestPrometheus503_NoOpAndRequeue | CH-01 |
| F-06 | K8s Deployment | Deployment 不存在 | 返回 error，backoff 重试 | TestDeploymentNotFound_ReturnsError | CH-04 |
| F-07 | K8s Deployment | containerName 不匹配 | 返回 error | TestContainerNameMismatch_ReturnsError | CH-05 |
| F-08 | K8s Deployment | patch 409 Conflict | 返回 Conflict error | TestPatchConflict_ReturnsError | CH-07 |
| F-09 | RBAC | patch Forbidden | 返回 error，JAVA_OPTS 不变 | —（fake client 不模拟 RBAC）| CH-06 |
| F-10 | CR Spec | min==max | 永久 no-op | TestMinMaxHeapEqual_AlwaysNoOp | CH-10 |
| F-11 | CR Spec | threshold 边界（恰好等于）| no-op（严格 >/<）| TestHeapExactlyAtScaleUpThreshold_NoOp | — |
| F-12 | Controller | Pod crash 重启 | 重启后从 Deployment 重读 | TestOOMKilled_AutoTunerReadsLiveDeployment | CH-08 |
| F-13 | 网络 | 端口 DROP | no-op, requeue 30s | TestPrometheusTimeout_NoOpAndRequeue | CH-09 |
