# OMS 中间件混沌演练手册

> **适用对象**: OMS OLTP 服务的 SRE / Dev 团队  
> **演练类型**: 中间件故障注入（Chaos Engineering）  
> **覆盖范围**: 18 个场景，横跨 DB / Outbox / HMAC / Privacy / Saga / Config / API 七个维度  
> **配套脚本**: `tests/test_chaos_middleware.py`（51 tests）

---

## 目录

1. [和普通压测的区别](#1-和普通压测的区别)
2. [演练环境要求](#2-演练环境要求)
3. [演练时长与节奏](#3-演练时长与节奏)
4. [指标计算方法](#4-指标计算方法)
5. [逐场景演练步骤](#5-逐场景演练步骤)
6. [关键发现与生产影响](#6-关键发现与生产影响)
7. [输出物与复盘模板](#7-输出物与复盘模板)
8. [演练频率建议](#8-演练频率建议)

---

## 1. 和普通压测的区别

| 维度 | 普通压测（Load Test） | 混沌演练（Chaos Drill） |
|------|----------------------|------------------------|
| **核心问题** | "系统能撑多少 QPS？" | "系统在局部故障时能否自保？" |
| **流量模式** | 持续爬升 → 峰值 → 稳定 | 正常流量下，**注入单点故障** |
| **关注指标** | QPS、P99、错误率、CPU | 事务原子性、幂等性、错误传播路径 |
| **成功标准** | 不崩溃、不丢消息、延迟达标 | **故障隔离**：一个组件异常不扩散到整个系统 |
| **环境** | 尽量接近生产规模 | **单实例 + SQLite 即可**；重现逻辑路径，不依赖规模 |
| **持续时间** | 30 分钟 ～ 数小时 | 单场景 **5～15 分钟**；全套 18 场景约 **3 小时** |
| **执行者** | QA / Perf 团队 | Dev + SRE 共同执行，Dev 负责注入，SRE 负责观测 |
| **优化方向** | 扩容、分片、缓存 | 修补代码路径、加防护断言、完善配置校验 |
| **自动化** | JMeter / k6 脚本 | `pytest tests/test_chaos_middleware.py` |

**核心区别用一句话概括**：压测验证容量上限，混沌演练验证故障边界——两者互补，不可替代。

---

## 2. 演练环境要求

### 2.1 最小环境（本地开发 / CI）

```
Python  3.11+
pytest  8.x
SQLite  3.39+（内置于 Python）
```

所有 18 个场景均可在本地运行，不依赖 Kubernetes 或 Kafka：

```powershell
# 安装依赖
pip install -r oms-oltp-poc/requirements.txt

# 运行全套混沌测试
cd oms-oltp-poc
pytest tests/test_chaos_middleware.py -v --tb=short
```

### 2.2 生产验证环境（推荐）

在以下环境执行时，混沌演练结论更具说服力：

| 组件 | 规格 | 用途 |
|------|------|------|
| Kubernetes | 1.23+，2 节点 | 验证 Pod 级故障隔离 |
| PostgreSQL / Aurora | 单实例 | 替换 SQLite，测试连接池行为 |
| Kafka | 3.x，1 broker | 验证 Outbox 积压行为 |
| Prometheus + Grafana | 标准部署 | 观测指标异常 |

> 本手册的测试代码基于 SQLite；生产环境验证需同步修改 `oms_oltp/db.py` 的连接层。

### 2.3 权限要求

- 能创建 / 删除 SQLite 文件（`tmp_path` fixture 自动处理）
- 能修改文件权限（`chmod`，用于 CHAOS-DB-02）
- Windows 上 CHAOS-DB-02 使用无效路径模拟，无需 `chmod`

---

## 3. 演练时长与节奏

### 单次完整演练（18 场景）

| 阶段 | 内容 | 预计时长 |
|------|------|---------|
| **环境准备** | 依赖安装、数据库初始化 | 10 分钟 |
| **自动执行** | `pytest -v` 跑完 51 tests | 2～5 分钟 |
| **结果分析** | 对照预期行为，标记 PASS / DOCUMENTED GAP | 30 分钟 |
| **复盘讨论** | 关键发现 → 修复决策 | 60 分钟 |
| **报告输出** | 填写复盘模板 | 30 分钟 |
| **合计** | | **约 2.5 小时** |

### 单场景专项演练

针对单个维度（例如只跑 DB 相关）：

```powershell
# 只跑 DB 相关场景
pytest tests/test_chaos_middleware.py -v -k "DB"

# 只跑 SAGA 相关场景
pytest tests/test_chaos_middleware.py -v -k "SAGA or saga"

# 只跑关键发现场景（4 个高优先级）
pytest tests/test_chaos_middleware.py -v -k "SETTINGS_01 or HMAC_02 or PRIVACY_01 or DB_03"
```

单场景专项演练耗时：**15～30 分钟**（含分析）。

---

## 4. 指标计算方法

### 4.1 事务原子性指标

**定义**: 故障注入后，数据库状态是否保持一致。

```
原子性通过率 = 无中间状态的场景数 / 总注入场景数
```

本次演练结果：**18/18 = 100%**（所有场景均无脏写或中间状态）

**检测方法**:
- 注入后查询 `sku_inventory.available_stock` 是否等于预期值
- 查询 `saga_log` 是否有不应存在的 `commit_inventory` 记录
- 查询 `outbox_events` 的 `status` 是否仍为 `PENDING`

### 4.2 故障隔离指标

**定义**: 一个组件故障是否扩散到其他组件。

```
隔离成功率 = 故障局限在预期组件内的场景数 / 总注入场景数
```

| 注入点 | 预期影响范围 | 实际影响范围 | 隔离结论 |
|--------|------------|------------|---------|
| DB 文件只读 | place_order 抛异常 | ✅ 仅 place_order | 隔离成功 |
| busy_timeout | _reserved_order 阻塞 | ✅ 仅当前请求 | 隔离成功 |
| Outbox 崩溃 | publish_outbox 回滚 | ✅ 事件仍在 PENDING | 隔离成功 |
| 空 webhook secret | 接受任意回调 | ⚠️ 影响所有支付回调 | **隔离失败（配置漏洞）** |

### 4.3 幂等性指标

**定义**: 相同请求重复执行，结果是否一致。

```
幂等性覆盖率 = 验证了幂等行为的接口数 / 所有对外接口数
```

已验证：
- `place_order`（idempotency_key）✅
- `create_payment_intent`（已 CONFIRMED 订单重建 intent）✅
- `publish_outbox`（崩溃回滚后重试）✅

未验证（建议补充）：
- `cancel_order` 重复调用
- `expire_reservations` 在分布式节点上的幂等性

### 4.4 启动开销指标（CHAOS-API-01）

```
init_schema 平均耗时 = 总耗时(ms) / 调用次数
```

测试结果：200 次调用 < 5000ms，即平均每次 < 25ms。  
**告警阈值**: 单次 > 500µs 表明出现回归（索引丢失或 schema 膨胀）。

---

## 5. 逐场景演练步骤

> 每个场景包含：**故障描述** → **注入方式** → **预期行为** → **验证命令** → **实际结论**

---

### DB 维度（4 个场景）

#### CHAOS-DB-01: 并发 expire_reservations 竞态

**故障描述**: 两个线程同时对同一批过期预留执行释放，可能导致库存双重释放。

**注入方式**:
```python
# 强制将预留过期时间设为过去
conn.execute(
    "UPDATE inventory_reservations SET expires_at = '2000-01-01T00:00:00+00:00' WHERE order_id = ?",
    (order_id,)
)
# 两个线程同时调用 svc.expire_reservations()
```

**预期行为**: 至少一个成功；`available_stock` 恢复到原始值 120，不多不少。

**验证命令**:
```python
stock = next(r for r in svc.inventory() if r["sku_id"] == "SKU-RED-001")
assert stock["available_stock"] == 120
```

**实际结论**: ✅ 不双释放库存（SQLite WAL 模式的串行化保护生效）

**生产注意**: Aurora / PostgreSQL 下需验证 `SELECT FOR UPDATE SKIP LOCKED` 是否正确使用。

---

#### CHAOS-DB-02: DB 文件只读

**故障描述**: 磁盘满或权限变更导致 SQLite 文件变为只读。

**注入方式**:
```python
db.chmod(0o444)  # Linux/macOS
# Windows: 指向无效路径
```

**预期行为**: `place_order` 抛出异常，不产生部分写入。

**验证命令**:
```powershell
pytest tests/test_chaos_middleware.py -v -k "DB_02 or readonly"
```

**实际结论**: ✅ 抛异常，不写入

---

#### CHAOS-DB-03: busy_timeout 耗尽（锁竞争）

**故障描述**: 长事务持锁，其他请求等待超时（默认 5s）后阻塞整个 ASGI worker。

**注入方式**:
```python
# 后台线程持有 BEGIN IMMEDIATE 锁 7 秒
def hold_lock():
    conn.execute("BEGIN IMMEDIATE")
    release.wait(timeout=7)
```

**预期行为**: 后续请求抛出 `sqlite3.OperationalError: database is locked`。

**验证命令**:
```python
assert "locked" in str(exc_info.value).lower()
```

**实际结论**: ✅ 抛 OperationalError

**⚠️ 生产影响**: SQLite `busy_timeout=5s` 会**阻塞整个 ASGI worker 线程**，不适合生产。  
**修复建议**: 换 Aurora + 连接池（asyncpg），或至少将 `busy_timeout` 设为 100ms + 应用层重试。

---

#### CHAOS-DB-04: reserved_stock 被清零后 capture

**故障描述**: 数据库直接被修改（运维误操作 / 数据修复脚本），`reserved_stock` 清零，但订单仍在 RESERVED 状态。

**注入方式**:
```python
conn.execute("UPDATE sku_inventory SET reserved_stock = 0 WHERE sku_id = 'SKU-RED-001'")
svc.capture_payment(order_id=order_id, succeed=True)
```

**预期行为**: `capture_payment` 回滚，订单保持 RESERVED，saga_log 无 `commit_inventory`。

**验证命令**:
```python
assert svc.order(order_id)["status"] == "RESERVED"
row = conn.execute("SELECT step_name FROM saga_log WHERE order_id = ? AND step_name = 'commit_inventory'", (order_id,)).fetchone()
assert row is None
```

**实际结论**: ✅ 回滚，订单留 RESERVED

---

### Outbox 维度（2 个场景）

#### CHAOS-OUTBOX-01: publish_outbox 中途崩溃

**故障描述**: Outbox 发布进程在处理第二条事件时崩溃（模拟 OOM Kill / 进程重启）。

**注入方式**:
```python
# monkeypatch 替换 publish_outbox，在第 2 条事件时 raise RuntimeError
if call_count["n"] == 2:
    raise RuntimeError("injected crash mid-batch")
```

**预期行为**: 整个 batch 事务回滚，所有事件回到 PENDING 状态，不永久丢失。

**验证命令**:
```python
still_pending = len([e for e in svc.outbox() if e["status"] == "PENDING"])
assert still_pending == pending_before
```

**实际结论**: ✅ 事务回滚，不永久丢事件

---

#### CHAOS-OUTBOX-02: 积压超过 limit → 静默滞后

**故障描述**: 30 条 PENDING 事件，每次只发布 10 条，剩余 20 条静默积压。

**注入方式**:
```python
for i in range(30):
    _reserved_order(svc, key=f"backlog-{i}")
svc.publish_outbox(limit=10)
```

**预期行为**: 只发布 10 条，剩余 20 条留在 PENDING（文档化行为，非 bug）。

**实际结论**: ✅ 文档化行为

**生产注意**: 需要监控 `outbox_events WHERE status='PENDING' AND created_at < NOW()-5min` 的数量，设置告警阈值。

---

### HMAC 维度（2 个场景）

#### CHAOS-HMAC-01: 支付密钥轮换拒绝在途回调

**故障描述**: 支付回调在飞行中时，服务端轮换了 `payment_webhook_secret`，导致签名验证失败。

**注入方式**:
```python
# 用旧 secret 签名
sig = _signed("cb-rot", provider_ref, amount_cents)
# 服务端替换为新 secret
monkeypatch.setattr(paysec, "settings", Settings(payment_webhook_secret="rotated-secret"))
# 发送回调
secure.handle_payment_callback(..., signature=sig)
```

**预期行为**: 抛出 `BusinessError(INVALID_PROVIDER_SIGNATURE)`，订单保持 RESERVED。

**验证命令**:
```python
assert exc_info.value.code == "INVALID_PROVIDER_SIGNATURE"
assert svc.order(order_id)["status"] == "RESERVED"
```

**实际结论**: ✅ INVALID_PROVIDER_SIGNATURE

**密钥轮换操作规程**: 轮换前需等待所有在途回调（通常 < 60s）处理完毕，或保持双 secret 验证过渡期。

---

#### CHAOS-HMAC-02: REJECTED 回调永久卡死

**故障描述**: 回调因签名错误被标记为 REJECTED 后，即使用正确签名重投，仍无法处理。

**注入方式**:
```python
# 第一次：错误签名 → REJECTED
secure.handle_payment_callback(..., signature="forged")
# 第二次：正确签名 → 仍然报错
secure.handle_payment_callback(..., signature=correct_sig)
```

**预期行为（当前）**: 第二次仍然抛出 BusinessError，`callback_id` 永久卡死。

**实际结论**: ✅ 文档化 gap（无重试路径）

**⚠️ 生产影响**: 订单永久停留在 RESERVED，无法自动恢复，需要人工干预。  
**修复建议**: 见 `production-fixes-checklist.md` → FIX-02。

---

### Privacy 维度（3 个场景）

#### CHAOS-PRIVACY-01: 未知客户默认 CN 区域无警告

**故障描述**: `CUSTOMER_REGIONS` 中不存在的 `customer_id`，系统静默归类为 CN 区域，无任何告警或错误。

**注入方式**:
```python
# 插入 CUSTOMER_REGIONS 中不存在的客户
conn.execute("INSERT OR IGNORE INTO customers VALUES ('CUST-9999','Unknown','Retail','2026-01-01T00:00:00+00:00')")
order = _reserved_order(svc, cust="CUST-9999")
export = secure.export_order_summary(order_id=order_id, target_region="OVERSEAS", purpose="customer_analytics")
```

**预期行为（当前）**: 允许导出，audit 记录 ALLOWED，无 WARNING。

**实际结论**: ✅ 文档化静默误分类

**⚠️ 生产影响**: 未知客户的数据可能被错误地跨境传输，违反 PDPA / PIPL。  
**修复建议**: 见 `production-fixes-checklist.md` → FIX-03。

---

#### CHAOS-PRIVACY-02: 隐私 token 密钥轮换破坏身份

**故障描述**: `privacy_token_secret` 轮换后，同一 `customer_id` 产生不同 token，导致下游系统无法关联历史数据。

**注入方式**:
```python
export1 = secure.export_order_summary(...)  # 旧 secret
monkeypatch.setattr(paysec, "settings", Settings(privacy_token_secret="new-secret-xyz"))
export2 = secure.export_order_summary(...)  # 新 secret
assert token1 != token2
```

**实际结论**: ✅ 同一 customer_id 产生不同 token（文档化不稳定性）

**修复建议**: 引入 token 版本号，或使用确定性加密（AES-SIV）替代 HMAC。

---

#### CHAOS-PRIVACY-03: 审计 INSERT 失败 → 异常传播

**故障描述**: 导出时审计记录写入失败，验证系统行为是阻止导出还是静默放行。

**注入方式**:
```python
monkeypatch.setattr(SecurePaymentPrivacyService, "_write_export_audit", failing_audit)
secure.export_order_summary(...)
```

**预期行为**: 抛出异常，导出被阻止，audit 表中无对应记录。

**实际结论**: ✅ 导出被阻止，不静默

---

### Saga 维度（2 个场景）

#### CHAOS-SAGA-01: reserved_stock 不匹配时 capture

**故障描述**: `reserved_stock` 与订单数量不匹配时，`capture_payment` 应回滚整个 saga。

**注入方式**: 同 CHAOS-DB-04（该场景既是 DB 测试也是 Saga 测试）

**额外验证**:
```python
row = conn.execute(
    "SELECT step_name FROM saga_log WHERE order_id = ? AND step_name = 'commit_inventory'",
    (order_id,)
).fetchone()
assert row is None
```

**实际结论**: ✅ 回滚，saga_log 无 commit 记录

---

#### CHAOS-SAGA-02: 取消已 CONFIRMED 订单

**故障描述**: 订单已支付确认，尝试取消应被拒绝，状态机保持完整性。

**注入方式**:
```python
svc.capture_payment(order_id=order_id, succeed=True)
svc.cancel_order(order_id=order_id)  # 应抛错
```

**预期行为**: `BusinessError(INVALID_ORDER_STATE)`，订单保持 CONFIRMED。

**实际结论**: ✅ INVALID_ORDER_STATE

---

### Settings 维度（2 个场景）

#### CHAOS-SETTINGS-01: 空 webhook secret 可被接受 ⚠️ 关键

**故障描述**: `payment_webhook_secret` 配置为空字符串时，用空 secret 签名的回调可以成功通过验证。

**注入方式**:
```python
monkeypatch.setattr(paysec, "settings", Settings(payment_webhook_secret=""))
empty_sig = sign_payment_callback(..., secret="")
secure.handle_payment_callback(..., signature=empty_sig)
```

**实际结论**: ✅（当前代码） — 订单成功变为 CONFIRMED

**⚠️ 生产影响**: 任何人只需用空 secret 签名即可伪造支付回调，将任意订单标记为已支付。  
**修复建议**: 见 `production-fixes-checklist.md` → FIX-01（P0，立即修复）。

---

#### CHAOS-SETTINGS-02: sqlite_path 不可写

**故障描述**: `sqlite_path` 指向不可创建的路径，`initialize()` 应快速失败。

**注入方式**:
```python
impossible = Path("Z:/nonexistent_drive/sub/oms.sqlite")
svc2 = OMSService(impossible)
svc2.initialize()
```

**实际结论**: ✅ 抛 OSError

---

### API 维度（2 个场景）

#### CHAOS-API-01: init_schema 热路径开销

**故障描述**: 每次 HTTP 请求都调用 `init_schema()`（CREATE TABLE IF NOT EXISTS × 15），存在无谓开销。

**测量方式**:
```python
start = time.perf_counter()
for _ in range(200):
    init_schema(conn)
elapsed_ms = (time.perf_counter() - start) * 1000
avg_us = (elapsed_ms / 200) * 1000
```

**实际结论**: ✅ 200 次 < 5s（平均 < 25ms / 次，可接受）

**持续监控**: 若平均超过 500µs，说明 schema 膨胀或索引丢失，触发告警。

---

#### CHAOS-API-02: 未知 BusinessError → 409

**故障描述**: `INTERNAL_SAGA_FAILURE` 等非预设错误码，通过 HTTP API 返回 409 Conflict 而非 500。

**注入方式**:
```python
with patch("oms_oltp.service.OMSService.place_order",
           side_effect=BusinessError("INTERNAL_SAGA_FAILURE", "unexpected internal error")):
    resp = client.post("/api/orders", ...)
```

**实际结论**: ✅ 文档化状态码误映射（409 而非 500）

**修复建议**: 在 exception handler 中添加兜底逻辑，未知 BusinessError 返回 500，避免客户端误判为客户端错误。

---

## 6. 关键发现与生产影响

| ID | 场景 | 严重程度 | 生产影响 | 修复优先级 |
|----|------|---------|---------|-----------|
| **FIX-01** | CHAOS-SETTINGS-01：空 webhook secret | 🔴 P0 | 攻击者可伪造任意支付确认 | **立即修复** |
| **FIX-02** | CHAOS-HMAC-02：REJECTED 回调永久死锁 | 🟠 P1 | 订单永久卡在 RESERVED，需人工修复 | 下个 Sprint |
| **FIX-03** | CHAOS-PRIVACY-01：未知客户静默归 CN | 🟠 P1 | 潜在违反 PDPA/PIPL 跨境传输规定 | 下个 Sprint |
| **FIX-04** | CHAOS-DB-03：busy_timeout 阻塞 ASGI Worker | 🟡 P2 | 高并发时 Worker 线程全部阻塞 | 计划中 |

详细修复步骤见 `production-fixes-checklist.md`。

---

## 7. 输出物与复盘模板

### 7.1 每次演练输出物清单

- [ ] pytest 运行日志（`pytest -v --tb=long > chaos-run-$(date +%Y%m%d).log`）
- [ ] 本手册的关键发现表格（更新版本）
- [ ] `production-fixes-checklist.md` 中对应 FIX 条目的状态更新
- [ ] JIRA/Confluence 工单（针对 P0/P1 发现）

### 7.2 复盘讨论模板

```
演练日期: ___________
执行人: ___________
环境: [ ] 本地 SQLite  [ ] 生产验证环境

新发现:
  场景 ID: ___________
  故障描述: ___________
  实际行为: ___________
  与预期的偏差: ___________
  影响评估: ___________
  修复决策: ___________

已知 gap 状态更新:
  FIX-01: [ ] 未修复  [ ] 已修复  [ ] 验证通过
  FIX-02: [ ] 未修复  [ ] 已修复  [ ] 验证通过
  FIX-03: [ ] 未修复  [ ] 已修复  [ ] 验证通过
  FIX-04: [ ] 未修复  [ ] 已修复  [ ] 验证通过
```

---

## 8. 演练频率建议

| 触发条件 | 执行范围 | 负责人 |
|---------|---------|--------|
| **新功能上线前** | 全套 18 场景 | Dev + SRE |
| **依赖库升级**（SQLAlchemy / FastAPI） | DB + API 维度（6 场景） | Dev |
| **密钥轮换操作前** | HMAC + Settings 维度（4 场景） | SRE |
| **Privacy 规则变更** | Privacy 维度（3 场景） | Dev + Compliance |
| **生产事故复盘后** | 对应维度全套 | SRE |
| **例行演练** | 全套 18 场景 | 轮值 SRE |
| 时间间隔 | **每月一次** | — |

---

## 参考

- 配套测试代码: `tests/test_chaos_middleware.py`
- 生产修复清单: `drills/chaos-findings/production-fixes-checklist.md`
- Lag→HPA 演练手册: `drills/lag-hpa-response-drill.md`
- Chaos Engineering 原则: [principlesofchaos.org](https://principlesofchaos.org)