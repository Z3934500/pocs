# Code Analysis Report — jvm-autotuner

- **Generated**: 2026-08-21
- **Source directory**: `oms-oltp-poc/go-operator/jvm-autotuner`
- **Files analyzed**: 9 source files

---

## Phase 0 — File Inventory

| # | Path | Language | Lines | Layer |
|---|------|----------|-------|-------|
| 1 | `cmd/main.go` | Go 1.22 | 87 | Script/Entry-point |
| 2 | `internal/controller/jvmautotuner_controller.go` | Go 1.22 | 320 | Service |
| 3 | `internal/controller/jvmautotuner_controller_test.go` | Go 1.22 | 730 | Service (test) |
| 4 | `api/v1alpha1/jvmautotuner_types.go` | Go 1.22 | 101 | Model/Entity |
| 5 | `api/v1alpha1/groupversion_info.go` | Go 1.22 | 19 | Infrastructure |
| 6 | `api/v1alpha1/zz_generated.deepcopy.go` | Go 1.22 | 101 | Infrastructure (generated) |
| 7 | `config/crd/jvmautotuner.yaml` | YAML | 133 | Config |
| 8 | `config/rbac/role.yaml` | YAML | 55 | Config |
| 9 | `config/sample/jvmautotuner-order-service.yaml` | YAML | 39 | Config |

**Build system detected:**
- `go.mod` — Go module `github.com/OWNER/jvm-autotuner`, Go 1.22
- Direct deps: `k8s.io/api v0.29.3`, `k8s.io/apimachinery v0.29.3`, `k8s.io/client-go v0.29.3`, `sigs.k8s.io/controller-runtime v0.17.3`
- Kubernetes CRD toolchain: `controller-gen` (kubebuilder annotations present; `make manifests` regenerates YAML)

---

## Phase 1 — Architecture Map

```
jvm-autotuner/
├── cmd/
│   └── main.go                    [Script/Entry-point]  ← Manager bootstrap, flag parsing, scheme registration
├── api/
│   └── v1alpha1/
│       ├── groupversion_info.go   [Infrastructure]      ← GVK registration (jvm.oms.io/v1alpha1)
│       ├── jvmautotuner_types.go  [Model/Entity]        ← CRD Go types: Spec / Status structs
│       └── zz_generated.deepcopy.go [Infrastructure]   ← controller-gen deep-copy (DO NOT EDIT)
├── internal/
│   └── controller/
│       ├── jvmautotuner_controller.go  [Service]        ← Core reconcile loop + Prometheus query + patch logic
│       └── jvmautotuner_controller_test.go [Service]    ← Unit + acceptance tests with fake client + httptest stub
├── config/
│   ├── crd/jvmautotuner.yaml      [Config]              ← OpenAPIv3 CRD schema for kubectl apply
│   ├── rbac/role.yaml             [Config]              ← ServiceAccount + ClusterRole + ClusterRoleBinding
│   └── sample/jvmautotuner-order-service.yaml [Config] ← Example CR for oms-prod order-service
├── go.mod                         [Config]              ← Module declaration + pinned deps
└── go.sum                         [Config]              ← Dependency checksum lock file
```

---

## Phase 2 — Layer Diagram

```
Kubernetes API Server
  │   (watch JvmAutoTuner CRs)
  ▼
[cmd/main.go]
  │  registers scheme, instantiates Manager + HTTPClient
  ▼
[controller-runtime Manager]
  │  enqueues reconcile requests on CR create/update/delete
  ▼
[jvmautotuner_controller.go — Reconcile()]
  │
  ├─── 1. Get JvmAutoTuner CR ──────────────── Kubernetes API
  ├─── 2. Get target Deployment ────────────── Kubernetes API
  ├─── 3. extractXmx() ─────────────────────── parse JAVA_OPTS env var (regex)
  ├─── 4. queryHeapUsagePct()
  │         └── queryPrometheus() ──────────── HTTP GET → Prometheus /api/v1/query
  ├─── 5. Decide: scale-up / scale-down / no-op
  ├─── 6. patchJavaOpts() (conditional) ────── Strategic-merge-patch → Kubernetes API
  └─── 7. Status().Update() ────────────────── Kubernetes API
            │
            └── RequeueAfter(reconcilePeriod) → back to Manager queue

[api/v1alpha1/jvmautotuner_types.go]
  └── defines JvmAutoTuner CRD schema used by all layers above
```

---

## Phase 3 — Cross-Cutting Concerns

| Concern | Where | Notes |
|---------|-------|-------|
| **幂等性** | `controller.go:112` | 仅在 `newXmxMB != currentXmxMB` 时才发 Patch；Status 每轮都更新 |
| **错误处理** | `controller.go:57-131` | Prometheus 失败返回 `RequeueAfter:30s` 而非传播错误；其他错误包装后返回令 Manager 退避重试 |
| **可观测性** | `controller.go:95,104,107,117` | zap 结构化日志记录每次 reconcile 的 heapPct、currentXmxMB、action、newJavaOpts |
| **CR 状态血缘** | `controller.go:121-128` | `Status.LastAction` / `HeapUsagePct` / `CurrentXmxMB` / `LastTunedAt` 每轮写回 CR |
| **Schema 安全** | `config/crd/jvmautotuner.yaml:46-97` | OpenAPIv3 validates required fields, integer min/max (scaleUpThreshold 1-99, minHeapMB ≥128, stepMB ≥64) |
| **依赖隔离** | `go.mod` | 所有 k8s/controller-runtime 依赖固定 minor 版本；间接依赖在 go.sum 中锁定 checksum |
| **RBAC 最小权限** | `config/rbac/role.yaml` | Deployment 仅允许 `get/list/watch/patch`，禁止 `delete`；CR status 独立 subresource |
| **Leader Election** | `cmd/main.go:43` | 默认关闭，通过 `--leader-elect` flag 启用；使用 ConfigMap + Lease 机制 |
| **HTTP 超时** | `cmd/main.go:66` | HTTPClient 固定 10s 超时，防止 Prometheus 慢查询阻塞 reconcile goroutine |
| **PII** | 无 | 不处理用户数据；所有操作对象为 JVM 内存指标和 Kubernetes 资源 |

---

## Phase 4 — Key Data Models

### JvmAutoTunerSpec（期望状态）

| 字段 | 类型 | 分类 | 说明 |
|------|------|------|------|
| `TargetDeployment` | string | identity | 被调优的 Deployment 名 |
| `TargetNamespace` | string | identity | 目标 NS，省略则同 CR NS |
| `ContainerName` | string | identity | 携带 JAVA_OPTS 的容器名 |
| `PrometheusURL` | string | infrastructure | Prometheus API base URL |
| `HeapQuery` | string | metrics | PromQL：heap used bytes |
| `MaxHeapQuery` | string | metrics (optional) | PromQL：heap committed bytes |
| `ScaleUpThreshold` | int32 | governance | heap% 上界，触发 Xmx 增大 |
| `ScaleDownThreshold` | int32 | governance | heap% 下界，触发 Xmx 减小 |
| `MinHeapMB` | int32 | governance | -Xmx 下限（MB） |
| `MaxHeapMB` | int32 | governance | -Xmx 上限（MB） |
| `StepMB` | int32 | governance | 每次调整步长（MB） |
| `ReconcilePeriod` | string | governance | Go duration，默认 "5m" |

### JvmAutoTunerStatus（观测状态）

| 字段 | 类型 | 分类 | 说明 |
|------|------|------|------|
| `CurrentXmxMB` | int32 | metrics | 控制器最近写入的 -Xmx 值 |
| `HeapUsagePct` | int32 | metrics | 上轮 reconcile 观测到的 heap% |
| `LastTunedAt` | *metav1.Time | governance | JAVA_OPTS 最后变更时间戳 |
| `LastAction` | string | governance | 上轮 reconcile 动作描述，如 "scale-up: 1024MB → 1280MB (heap=83%)" |
| `Conditions` | []metav1.Condition | governance | 标准 K8s condition 列表 |

**关键派生计算：**
- `heapPct = usedBytes / maxBytes × 100`
- `maxBytes` = `MaxHeapQuery` Prometheus 结果（失败时回退至 `MaxHeapMB × 1024 × 1024`），或当 `MaxHeapQuery` 为空时使用 `currentXmxMB × 1024 × 1024`
- `newXmxMB = min(currentXmxMB + StepMB, MaxHeapMB)` （scale-up）
- `newXmxMB = max(currentXmxMB - StepMB, MinHeapMB)` （scale-down）
- 初始化缺省值：`currentXmxMB = (MinHeapMB + MaxHeapMB) / 2`

---

## Phase 5 — Design Patterns

| Pattern | Location | Notes |
|---------|----------|-------|
| **Kubernetes Operator / Reconciler** | `controller.go:52` | 标准 controller-runtime reconcile 模式：observe → decide → act → requeue |
| **Level-based Control Loop** | `controller.go:98-109` | 不记录历史事件，每轮根据当前状态决策；天然幂等 |
| **Strategic Merge Patch** | `controller.go:246-283` | 只 patch 目标 container 的 JAVA_OPTS，其余字段不触碰 |
| **Threshold Band Controller** | `controller.go:101-108` | 双阈值死区（dead-band）防止 scale-up/down 频繁振荡 |
| **Null Object / Default** | `controller.go:82-86, 286-298` | 缺少 -Xmx 时初始化为中值；ReconcilePeriod 为空或非法时回退到默认值 |
| **Façade (HTTP Client injection)** | `cmd/main.go:64-68` | HTTPClient 作为字段注入，测试时可替换为 httptest.Server |
| **Fake Client Testing** | `controller_test.go:267+` | 使用 `sigs.k8s.io/controller-runtime/pkg/client/fake` 替换真实 API Server |

---

## Phase 6 — 逐文件分析

### 1. `cmd/main.go`

#### A. Syntax Profile
- Go 1.22；使用标准 `flag` 包、`os.Exit`
- 依赖：`sigs.k8s.io/controller-runtime`、`k8s.io/apimachinery`、`go.uber.org/zap`（通过 controller-runtime 封装）

#### B. Structural Skeleton
```go
var scheme = runtime.NewScheme()
var setupLog = ctrl.Log.WithName("setup")

func init()   // 注册 clientgo、appsv1、jvmv1alpha1 到 scheme
func main()   // 解析 flags → 构建 Manager → 注册 Controller → 添加健康检查 → Start
```

#### C. Algorithm Profile
| Method | Pseudocode |
|--------|-----------|
| `main()` | parse flags (metricsAddr, probeAddr, leaderElect) → set zap logger → ctrl.NewManager(kubeconfig, options) → if err exit(1) → register JvmAutoTunerReconciler with HTTPClient{Timeout:10s} → AddHealthzCheck("healthz") → AddReadyzCheck("readyz") → mgr.Start(SignalHandler) → if err exit(1) |

#### D. Execution Pipeline
| Stage | Participation |
|-------|--------------|
| Edit | 通过 CLI flags 或环境变量配置，无配置文件 |
| Compile | `go build ./cmd/main.go` 产出二进制 |
| Link | 依赖 `api/v1alpha1`、`internal/controller`、controller-runtime |
| Load | 进程启动时执行 init()，注册 scheme |
| Execute | 启动 Manager 事件循环，监听 OS 信号，优雅退出 |

#### E. Framework Layer
`Script/Entry-point` — 仅做组件装配和进程生命周期管理，不含业务逻辑。

---

### 2. `internal/controller/jvmautotuner_controller.go`

#### A. Syntax Profile
- Go 1.22；`regexp.MustCompile`、`json.RawMessage`、`context`、`net/http`
- 依赖：`k8s.io/api/apps/v1`、`k8s.io/apimachinery`、`sigs.k8s.io/controller-runtime`

#### B. Structural Skeleton
```go
const defaultReconcilePeriod = 5 * time.Minute
const javaOptsEnvVar         = "JAVA_OPTS"
const xmxRegexp              = `-Xmx(\d+)[mM]`

type JvmAutoTunerReconciler struct {
    client.Client
    HTTPClient *http.Client
}
type prometheusResponse struct { Status string; Data struct{ Result []struct{ Value []json.RawMessage } } }

func (r *JvmAutoTunerReconciler) Reconcile(ctx, req) (ctrl.Result, error)
func (r *JvmAutoTunerReconciler) queryHeapUsagePct(tuner, currentXmxMB) (float64, error)
func (r *JvmAutoTunerReconciler) queryPrometheus(baseURL, query string) (float64, error)
func (r *JvmAutoTunerReconciler) patchJavaOpts(ctx, deploy, containerIdx, newJavaOpts) error
func extractXmx(deploy, containerName) (int32, string, int)
func replaceXmx(javaOpts string, newXmxMB int32) string
func parsePeriod(s string) time.Duration
func min32(a, b int32) int32
func max32(a, b int32) int32
func (r *JvmAutoTunerReconciler) SetupWithManager(mgr) error
```

#### C. Algorithm Profile
| Method | Pseudocode |
|--------|-----------|
| `Reconcile` | get CR → if NotFound return nil → resolve targetNS → get Deployment → extractXmx → if xmx==0 init midpoint → queryHeapUsagePct → decide action (up/down/no-op) → if changed patchJavaOpts → Status.Update → return RequeueAfter(reconcilePeriod) |
| `queryHeapUsagePct` | queryPrometheus(heapQuery)→usedBytes → if maxHeapQuery≠"" queryPrometheus(maxHeapQuery)→maxBytes (fallback MaxHeapMB on error/zero) else maxBytes=currentXmxMB×1MB → if maxBytes==0 error → return usedBytes/maxBytes×100 |
| `queryPrometheus` | build URL with query-escaped PromQL → HTTPClient.Get → ReadAll body → json.Unmarshal → check status=="success" && len(result)>0 → parse values[1] as float64 |
| `extractXmx` | iterate containers; match by name → iterate env; match JAVA_OPTS → regex FindStringSubmatch(`-Xmx(\d+)[mM]`) → return (MB, rawValue, containerIdx); -1 if container not found |
| `replaceXmx` | if existing `-Xmx\d+[mM]` → ReplaceAll with `-Xmx{n}m`; else if empty return `-Xmx{n}m`; else TrimSpace+append |
| `patchJavaOpts` | build minimal strategic-merge-patch JSON targeting container by name → r.Patch(StrategicMergePatchType) |
| `parsePeriod` | ParseDuration(s) → if err or d≤0 return defaultReconcilePeriod |

#### D. Execution Pipeline
| Stage | Participation |
|-------|--------------|
| Edit | CR spec 字段驱动行为；无代码配置文件 |
| Compile | 编译进 operator 二进制 |
| Link | 依赖 `api/v1alpha1` types，controller-runtime Client interface |
| Load | Manager.Start() 后由 controller-runtime 注册并启动 watcher |
| Execute | 每次 CR 事件或 RequeueAfter 触发 Reconcile；产出 Deployment patch + CR status 更新 |

#### E. Framework Layer
`Service` — 封装完整的业务逻辑（JVM heap 调优策略），是项目核心。

---

### 3. `api/v1alpha1/jvmautotuner_types.go`

#### A. Syntax Profile
- Go 1.22；kubebuilder marker annotations (`+kubebuilder:object:root=true` 等)
- 依赖：`k8s.io/apimachinery/pkg/apis/meta/v1`

#### B. Structural Skeleton
```go
type JvmAutoTunerSpec struct {
    TargetDeployment, TargetNamespace, ContainerName string
    PrometheusURL, HeapQuery, MaxHeapQuery           string
    ScaleUpThreshold, ScaleDownThreshold             int32
    MinHeapMB, MaxHeapMB, StepMB                     int32
    ReconcilePeriod                                  string
}
type JvmAutoTunerStatus struct {
    CurrentXmxMB, HeapUsagePct int32
    LastTunedAt                *metav1.Time
    LastAction                 string
    Conditions                 []metav1.Condition
}
type JvmAutoTuner struct { metav1.TypeMeta; metav1.ObjectMeta; Spec; Status }
type JvmAutoTunerList struct { metav1.TypeMeta; metav1.ListMeta; Items []JvmAutoTuner }
func init()  // SchemeBuilder.Register
```

#### D. Execution Pipeline
| Stage | Participation |
|-------|--------------|
| Edit | 手动编辑后运行 `make manifests` 重新生成 CRD YAML 和 deepcopy |
| Compile | 编译进 operator 二进制和 CRD schema |
| Link | 被 controller、main、test 引用 |
| Load | init() 在 package import 时注册到 SchemeBuilder |
| Execute | 运行时作为 Kubernetes object 在 API server 中存储和检索 |

#### E. Framework Layer
`Model/Entity` — 纯数据结构定义，描述 CRD 的 spec/status contract。

---

### 4. `api/v1alpha1/groupversion_info.go`

#### A. Syntax Profile
- Go 1.22；无外部依赖除 `k8s.io/apimachinery` 和 `controller-runtime/pkg/scheme`

#### B. Structural Skeleton
```go
var GroupVersion = schema.GroupVersion{Group: "jvm.oms.io", Version: "v1alpha1"}
var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
var AddToScheme = SchemeBuilder.AddToScheme
```

#### E. Framework Layer
`Infrastructure` — 注册 GVK 到 Kubernetes scheme，无业务逻辑。

---

### 5. `api/v1alpha1/zz_generated.deepcopy.go`

#### A. Syntax Profile
- 由 `controller-gen` 自动生成，`//go:build !ignore_autogenerated`
- 实现 `runtime.Object` 接口所需的 `DeepCopyObject()` 方法

#### E. Framework Layer
`Infrastructure` — 代码生成产物，不手动编辑。

---

### 6. `config/crd/jvmautotuner.yaml`

CRD 定义，group `jvm.oms.io`，shortName `jat`，scope `Namespaced`。  
OpenAPIv3 schema 校验所有 spec 必填字段及数值范围约束，status subresource 独立。  
`Layer: Config`

---

### 7. `config/rbac/role.yaml`

三个资源：ServiceAccount `jvm-autotuner-controller`（namespace: `jvm-autotuner-system`）、ClusterRole、ClusterRoleBinding。  
最小权限：Deployment 只允许 patch，不允许 delete；Leader Election 使用 ConfigMap + Lease。  
`Layer: Config`

---

### 8. `config/sample/jvmautotuner-order-service.yaml`

示例 CR，目标 `oms-prod/order-service`，thresholds 40/80，step 256MB，范围 512–4096MB，period 5m。  
`Layer: Config`

---

### 9. `internal/controller/jvmautotuner_controller_test.go`

#### A. Syntax Profile
- Go 1.22 `testing` 包；`net/http/httptest`；`sync/atomic`；`controller-runtime/pkg/client/fake`
- Table-driven tests for pure functions；integration-style tests for full Reconcile path

#### B. Structural Skeleton
```go
type promStub struct { srv *httptest.Server; statusCode atomic.Int32; body atomic.Value }
func newScheme(t)                // 注册 appsv1 + jvmv1alpha1
func newPromStub(code, body)     // 启动 httptest.Server
func heapResponse(usedBytes)     // 构造 Prometheus JSON 响应
func makeDeployment(...)         // 构造测试用 Deployment
func makeTuner(...)              // 构造测试用 JvmAutoTuner CR
func reconcileOnce(...)          // 执行单次 Reconcile
func getJavaOpts(...)            // 从 fake client 读取 JAVA_OPTS
// 14 个 Test 函数覆盖：scale-up/down、no-op、max cap、min floor、
// 无 Xmx 初始化、多容器隔离、Prometheus 503/空数据/恢复、
// 幂等性、Xms 不被修改、CR 删除、parsePeriod 边界、振荡文档化、dead loop、OOMKill
```

#### E. Framework Layer
`Service (test)` — 与 controller 同 package，可访问内部函数（extractXmx、replaceXmx、parsePeriod）进行白盒测试。

---

## Phase 7 — AI Reconstruction Prompt

```
PROJECT: jvm-autotuner
PURPOSE: A Kubernetes Operator that automatically adjusts the -Xmx JVM heap flag in a
         target Deployment's JAVA_OPTS environment variable, driven by live Prometheus
         heap metrics, keeping heap utilisation between a configurable band.

TECH STACK:
  Language:       Go 1.22
  Framework:      sigs.k8s.io/controller-runtime v0.17.3
  Kubernetes:     k8s.io/api, k8s.io/apimachinery, k8s.io/client-go v0.29.3
  CRD tooling:    controller-gen (kubebuilder annotations)
  Metrics source: Prometheus HTTP API /api/v1/query (instant query, PromQL)
  Logging:        go.uber.org/zap via controller-runtime zap wrapper
  Testing:        Go standard testing + controller-runtime fake client + net/http/httptest

RUNTIME PREREQUISITES:
  - Running Kubernetes cluster with CRD installed (kubectl apply -f config/crd/)
  - RBAC applied (kubectl apply -f config/rbac/role.yaml)
  - Prometheus accessible from operator pod at configured prometheusURL
  - Target Deployment must have a container with JAVA_OPTS env var (or the controller
    will initialise it at midpoint of min/max and append -Xmx)

FILE MANIFEST:
  cmd/main.go
  internal/controller/jvmautotuner_controller.go
  api/v1alpha1/jvmautotuner_types.go
  api/v1alpha1/groupversion_info.go
  api/v1alpha1/zz_generated.deepcopy.go   (generated — run controller-gen)

PER-FILE SPECIFICATIONS:

  cmd/main.go:
    - Parse flags: --metrics-bind-address (default :8080), --health-probe-bind-address
      (default :8081), --leader-elect (default false)
    - Register scheme: clientgoscheme + appsv1 + jvmv1alpha1
    - Create ctrl.Manager with LeaderElectionID="jvm-autotuner.oms.io"
    - Instantiate JvmAutoTunerReconciler{Client: mgr.GetClient(),
      HTTPClient: &http.Client{Timeout: 10s}}
    - Add /healthz and /readyz Ping checks; call mgr.Start(SignalHandler)

  api/v1alpha1/groupversion_info.go:
    - GroupVersion = schema.GroupVersion{Group:"jvm.oms.io", Version:"v1alpha1"}
    - Export SchemeBuilder and AddToScheme

  api/v1alpha1/jvmautotuner_types.go:
    - JvmAutoTunerSpec: TargetDeployment(req), TargetNamespace(opt), ContainerName(req),
      PrometheusURL(req), HeapQuery(req), MaxHeapQuery(opt), ScaleUpThreshold int32(req),
      ScaleDownThreshold int32(req), MinHeapMB int32(req), MaxHeapMB int32(req),
      StepMB int32(req), ReconcilePeriod string(opt)
    - JvmAutoTunerStatus: CurrentXmxMB, HeapUsagePct int32; LastTunedAt *metav1.Time;
      LastAction string; Conditions []metav1.Condition
    - Kubebuilder markers: object:root=true, subresource:status, resource:shortName=jat
    - additionalPrinterColumns: Target, HeapPct, XmxMB, LastAction, Age
    - Register both JvmAutoTuner and JvmAutoTunerList in init()

  api/v1alpha1/zz_generated.deepcopy.go:
    - DeepCopyInto/DeepCopy/DeepCopyObject for JvmAutoTuner, JvmAutoTunerList,
      JvmAutoTunerSpec, JvmAutoTunerStatus
    - Properly deep-copies LastTunedAt (*metav1.Time) and Conditions ([]metav1.Condition)

  internal/controller/jvmautotuner_controller.go:
    Constants: defaultReconcilePeriod=5m, javaOptsEnvVar="JAVA_OPTS",
               xmxRegexp=`-Xmx(\d+)[mM]`
    Reconcile(ctx, req):
      1. r.Get CR; if NotFound return nil
      2. targetNS = spec.TargetNamespace ?? cr.Namespace
      3. r.Get Deployment
      4. extractXmx(deploy, containerName) → (currentXmxMB, javaOpts, containerIdx)
         if containerIdx==-1 return error
         if currentXmxMB==0 → currentXmxMB = (min+max)/2
      5. queryHeapUsagePct → heapPct; if err return RequeueAfter:30s (no error propagation)
      6. if heapPct > ScaleUpThreshold && current < max  → newXmx = min(current+step, max)
         if heapPct < ScaleDownThreshold && current > min → newXmx = max(current-step, min)
      7. if newXmx != current: patchJavaOpts(ctx, deploy, containerIdx, replaceXmx(...))
      8. Status.Update: CurrentXmxMB=newXmx, HeapUsagePct=int32(heapPct),
                        LastTunedAt=now, LastAction=action string
      9. return RequeueAfter(parsePeriod(spec.ReconcilePeriod))
    queryHeapUsagePct: query heapQuery → usedBytes; optionally query maxHeapQuery → maxBytes
      (fallback: MaxHeapMB*1MB on error; or currentXmxMB*1MB if no maxHeapQuery)
      return usedBytes/maxBytes*100
    queryPrometheus(baseURL, query): GET baseURL+"/api/v1/query?query="+escape(query)
      → parse prometheusResponse{Status,Data.Result[].Value[ts,"val"]}
      → return ParseFloat(values[1])
    extractXmx(deploy, name): scan containers by name; scan env for JAVA_OPTS;
      regex match -Xmx(\d+)[mM]; return (MB, rawValue, idx); 0,"",idx if no -Xmx; 0,"",-1 if no container
    replaceXmx(javaOpts, newMB): regex replace -Xmx\d+[mM] → -Xmx{n}m; append if absent
    patchJavaOpts: strategic-merge-patch containers[name=X].env[JAVA_OPTS]=newValue
    parsePeriod(s): ParseDuration; fallback defaultReconcilePeriod if empty/invalid/≤0
    SetupWithManager: ctrl.NewControllerManagedBy(mgr).For(&JvmAutoTuner{}).Complete(r)

STARTUP SEQUENCE:
  # Local dev (requires KUBECONFIG and a running cluster)
  kubectl apply -f config/crd/jvmautotuner.yaml
  kubectl apply -f config/rbac/role.yaml
  go run ./cmd/main.go --metrics-bind-address=:8080 --health-probe-bind-address=:8081

  # Apply a sample CR
  kubectl apply -f config/sample/jvmautotuner-order-service.yaml

  # Run unit tests (no cluster needed)
  go test ./internal/controller/... -v

VERIFICATION STEPS:
  # Check CR status
  kubectl get jat -A
  kubectl describe jat order-service-gc-tuner -n oms-prod

  # Verify JAVA_OPTS was patched
  kubectl get deployment order-service -n oms-prod \
    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS")].value}'

  # Watch controller logs
  kubectl logs -f deployment/jvm-autotuner-controller -n jvm-autotuner-system

KNOWN CONSTRAINTS / GOTCHAS:
  1. ScaleUpThreshold and ScaleDownThreshold must be far apart (≥ 40%) to avoid
     oscillation: two consecutive crossings produce two Deployment patches, each
     triggering a rolling update.
  2. parsePeriod rejects 0s and negative durations (falls back to 5m) to prevent
     RequeueAfter=0 silently stopping the polling loop.
  3. The controller reads -Xmx from the live Deployment on every reconcile — it never
     trusts Status.CurrentXmxMB — so external changes (OOMKill resets, manual patches)
     are picked up automatically.
  4. patchJavaOpts uses StrategicMergePatchType: containers list is merged by name,
     so only the target container's JAVA_OPTS is touched; sidecar containers are safe.
  5. Module path uses placeholder "github.com/OWNER/jvm-autotuner" — replace OWNER
     with the real GitHub org before publishing.
  6. Leader election is disabled by default; enable with --leader-elect in production
     HA deployments.
  7. HTTPClient timeout is hardcoded to 10s in main.go; if Prometheus queries exceed
     this the controller falls back to 30s requeue without patching.
```

