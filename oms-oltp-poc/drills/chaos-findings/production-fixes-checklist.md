# OMS 生产修复清单 — 混沌演练关键发现

> **来源**: `tests/test_chaos_middleware.py` 18 场景混沌演练  
> **最后更新**: 2026-08-22  
> **状态说明**: 🔴 未修复 / 🟡 进行中 / ✅ 已验证

---

## FIX-01 — 空 webhook secret 被接受（P0，立即修复）

**来源场景**: CHAOS-SETTINGS-01  
**严重程度**: 🔴 P0 — 安全漏洞  
**当前状态**: 🔴 未修复

### 问题描述

`payment_webhook_secret` 配置为空字符串时，用空 secret 签名的回调可以通过 HMAC 验证，
订单被标记为 CONFIRMED。任何能发送 HTTP 请求的攻击者，只需用空 secret 签名即可伪造支付确认。

```python
# 当前行为：以下代码成功将订单变为 CONFIRMED
monkeypatch.setattr(paysec, "settings", Settings(payment_webhook_secret=""))
empty_sig = sign_payment_callback(..., secret="")
secure.handle_payment_callback(..., signature=empty_sig)
# → order["status"] == "CONFIRMED"  ⚠️
```

### 根因

`sign_payment_callback` 使用 `hmac.new(secret.encode(), ...)` — 空字符串合法，HMAC 不拒绝空 key。
验证层只检查签名是否匹配，不检查 secret 本身是否有效。

### 修复步骤

**Step 1**: 在应用启动时（`lifespan` 或 `__init__`）校验 secret 非空

```python
# src/oms_oltp/config.py 或 src/oms_oltp/api.py lifespan
from oms_oltp.config import settings

def validate_startup_config():
    if not settings.payment_webhook_secret:
        raise RuntimeError(
            "FATAL: payment_webhook_secret is empty. "
            "Set OMS_PAYMENT_WEBHOOK_SECRET env var before starting."
        )
    if len(settings.payment_webhook_secret) < 32:
        raise RuntimeError(
            "FATAL: payment_webhook_secret must be at least 32 characters."
        )
```

**Step 2**: 在 `handle_payment_callback` 入口处增加防御性检查

```python
# src/oms_oltp/payment_security.py
def handle_payment_callback(self, ...):
    if not settings.payment_webhook_secret:
        raise BusinessError(
            "MISCONFIGURED_WEBHOOK_SECRET",
            "payment_webhook_secret is not configured"
        )
    # ... 现有验证逻辑
```

**Step 3**: 在 CI 环境变量配置中强制设置非空值，防止测试环境用空 secret 上线

```yaml
# .github/workflows/deploy.yml
- name: Validate secrets
  run: |
    if [ -z "$OMS_PAYMENT_WEBHOOK_SECRET" ]; then
      echo "ERROR: OMS_PAYMENT_WEBHOOK_SECRET is not set"
      exit 1
    fi
```

**Step 4**: 添加回归测试

```python
def test_empty_webhook_secret_is_rejected_at_startup():
    with pytest.raises(RuntimeError, match="payment_webhook_secret is empty"):
        validate_startup_config()  # 在 Settings(payment_webhook_secret="") 环境下
```

### 验证方法

```powershell
# 本地验证
$env:OMS_PAYMENT_WEBHOOK_SECRET = ""
python -c "from oms_oltp.api import app"
# 预期: RuntimeError: FATAL: payment_webhook_secret is empty
```

### 预期修复效果

修复后，CHAOS-SETTINGS-01 测试预期行为从 "CONFIRMED（漏洞）" 变为 "RuntimeError（防护）"，
需同步更新测试断言。

---

## FIX-02 — REJECTED 回调永久死锁，无重试路径（P1）

**来源场景**: CHAOS-HMAC-02  
**严重程度**: 🟠 P1 — 业务影响（订单卡死）  
**当前状态**: 🔴 未修复

### 问题描述

支付回调因签名错误（如密钥轮换期间的在途请求）被标记为 `REJECTED` 后，
即使支付机构用正确签名重投，`callback_id` 查询到 REJECTED 状态就直接拒绝，
订单永久停留在 RESERVED，无自动恢复路径。

```
支付机构重投流程（当前）:第一次: 错误签名 → callback_id 标记 REJECTED
  第二次: 正确签名 → 查到 REJECTED → 仍然抛 BusinessError
  结果: 订单永久 RESERVED，需人工干预
```

### 根因

`payment_intents` 或 `payment_callbacks` 表中，`callback_id` 的状态一旦变为 REJECTED，
后续查询走幂等性短路逻辑，直接返回错误，不重新验证签名。

### 修复步骤（两种方案，选其一）

#### 方案 A：允许 REJECTED 状态重试（推荐）

**Step 1**: 修改回调处理逻辑，REJECTED 状态的 callback_id 允许重新验证

```python
# src/oms_oltp/payment_security.py
def handle_payment_callback(self, callback_id, ...):
    existing = self._get_callback(callback_id)
    if existing:
        if existing["status"] == "CAPTURED":
            return {"order": self._svc.order(existing["order_id"])}  # 幂等返回
        elif existing["status"] == "REJECTED":
            # 允许重试：重新验证签名，不直接拒绝
            pass  # 继续走验证流程
    # ... 验证签名 ...
```

**Step 2**: 记录重试事件到 audit log

```python
if existing and existing["status"] == "REJECTED":
    logger.warning(
        "callback_id=%s retried after REJECTED status, re-validating",
        callback_id
    )
```

#### 方案 B：管理员手动解锁工具（短期应急）

```python
# src/oms_oltp/admin_tools.py
def unlock_rejected_callback(db_path: str, callback_id: str, reason: str):
    """
    管理员工具：将 REJECTED 的 callback 重置为 PENDING，允许重新处理。
    需要记录操作日志，双人审批。
    """
    with connect(db_path) as conn:
        with transaction(conn):
            conn.execute(
                "UPDATE payment_callbacks SET status = 'PENDING', rejected_reason = NULL "
                "WHERE callback_id = ? AND status = 'REJECTED'",
                (callback_id,)
            )
            conn.execute(
                "INSERT INTO admin_audit_log VALUES (?, 'unlock_callback', ?, ?)",
                (callback_id, reason, datetime.utcnow().isoformat())
            )
```

**Step 3**: 配套 SOP（无论选哪个方案）

```
处置 SOP — REJECTED 回调死锁:
1. 收到告警: order_status=RESERVED AND age > 30min
2. 查询: SELECT * FROM payment_callbacks WHERE callback_id = ?
3. 确认支付机构已成功扣款（在支付机构后台核查）
4. 若已扣款: 执行 unlock_rejected_callback，记录原因
5. 触发重新投递或手动调用 capture_payment
6. 验证订单状态变为 CONFIRMED
```

### 监控告警

```yaml
# Prometheus alert rule
- alert: OrderStuckInReserved
  expr: |
    count(oms_order_status{status="RESERVED"} and on(order_id) oms_order_age_seconds > 1800) > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Orders stuck in RESERVED for > 30 minutes"
```

---

## FIX-03 — 未知客户静默归 CN 区域（P1）

**来源场景**: CHAOS-PRIVACY-01  
**严重程度**: 🟠 P1 — 合规风险（PDPA / PIPL）  
**当前状态**: 🔴 未修复

### 问题描述

`CUSTOMER_REGIONS` 中不存在的 `customer_id`，系统静默默认分类为 CN 区域，
允许数据导出到 OVERSEAS 目标，可能违反《个人信息保护法》（PIPL）的跨境传输限制。

```python
# 当前行为：
export = secure.export_order_summary(
    order_id=order_id,
    target_region="OVERSEAS",  # 跨境！
    purpose="customer_analytics",
)
# CUST-9999 不在 CUSTOMER_REGIONS 中
# → 系统静默归 CN
# → audit 记录 ALLOWED（但实际地区未知）
# → 数据被允许导出 ⚠️
```

### 根因

地区解析逻辑中，`CUSTOMER_REGIONS.get(customer_id, "CN")` 的默认值 `"CN"` 无告警，
也不区分"已知中国用户"和"地区未知用户"。

### 修复步骤

**Step 1**: 将未知客户的地区标记为显式 UNKNOWN，不再静默默认

```python
# src/oms_oltp/payment_security.py
def _resolve_customer_region(self, customer_id: str) -> str:
    region = CUSTOMER_REGIONS.get(customer_id)
    if region is None:
        logger.warning(
            "customer_id=%s not found in CUSTOMER_REGIONS, defaulting to UNKNOWN",
            customer_id
        )
        return "UNKNOWN"  # 不再静默返回 "CN"
    return region
```

**Step 2**: 在 `export_order_summary` 中拒绝 UNKNOWN 地区的跨境请求

```python
def export_order_summary(self, order_id, target_region, purpose):
    customer_region = self._resolve_customer_region(order["customer_id"])
    
    if customer_region == "UNKNOWN":
        # 保守策略：未知地区不允许跨境导出
        if target_region != "CN":
            raise BusinessError(
                "CUSTOMER_REGION_UNKNOWN",
                f"Cannot export order {order_id} cross-border: "
                f"customer region is unknown. Requires manual review."
            )
        # CN 内部导出：允许但记录警告
        self._write_export_audit(..., decision="ALLOWED_WITH_WARNING", 
                                 reason="customer_region_unknown")
```

**Step 3**: 添加启动检查，检测 CUSTOMER_REGIONS 覆盖率

```python
def validate_customer_regions_coverage(db_path: str):
    """报告数据库中有订单但不在 CUSTOMER_REGIONS 中的客户"""
    with connect(db_path) as conn:
        unknown = conn.execute("""
            SELECT DISTINCT o.customer_id
            FROM orders o
            WHERE o.customer_id NOT IN (
                SELECT customer_id FROM customer_regions
            )
            LIMIT 100
        """).fetchall()if unknown:
        logger.warning(
            "CUSTOMER_REGIONS missing %d customer(s): %s",
            len(unknown),
            [r["customer_id"] for r in unknown]
        )
```

**Step 4**: 添加 Prometheus 指标

```python
# 每次静默归类时 +1
unknown_region_counter = Counter(
    "oms_customer_region_unknown_total",
    "Orders exported with unknown customer region"
)
```

### 告警规则

```yaml
- alert: CustomerRegionUnknownExport
  expr: increase(oms_customer_region_unknown_total[1h]) > 0
  labels:
    severity: warning
  annotations:
    summary: "Cross-border export attempted for customer with unknown region"
```

---

## FIX-04 — busy_timeout 阻塞 ASGI Worker（P2）

**来源场景**: CHAOS-DB-03  
**严重程度**: 🟡 P2 — 性能风险（高并发下 Worker 阻塞）  
**当前状态**: 🔴 未修复

### 问题描述

SQLite `busy_timeout=5000ms`（5秒）意味着在锁竞争时，ASGI worker 线程会同步阻塞最多 5 秒。
在 Uvicorn 默认配置（4 workers）下，4 个并发锁竞争请求即可耗尽全部 worker，
导致后续所有请求超时，服务完全不可用。

```
并发场景（4 workers, busy_timeout=5s）:
  Request 1: 持锁 → 正常处理
  Request 2: 等待锁 → worker 阻塞 5s
  Request 3: 等待锁 → worker 阻塞 5s
  Request 4: 等待锁 → worker 阻塞 5s
  Request 5: 无可用 worker → 502 Bad Gateway ⚠️
```

### 修复步骤

#### 短期（1-2天）: 降低 busy_timeout

```python
# src/oms_oltp/db.py
BUSY_TIMEOUT_MS = 100  # 从 5000 降至 100ms

def connect(db_path):
    conn = sqlite3.connect(db_path, timeout=BUSY_TIMEOUT_MS / 1000)
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    # ...
```

同时在应用层添加指数退避重试：

```python
# src/oms_oltp/service.py
import time, random

def _with_retry(fn, max_attempts=3, base_delay=0.05):
    for attempt in range(max_attempts):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.01)
                time.sleep(delay)
            else:
                raise
```

#### 中期（下个 Sprint）: 迁移至 Aurora PostgreSQL + asyncpg

```python
# 替换 sqlite3 连接层
# src/oms_oltp/db.py（Aurora 版本）
import asyncpg

async def get_pool():
    return await asyncpg.create_pool(
        dsn=settings.aurora_dsn,
        min_size=5,
        max_size=20,
        command_timeout=10,        # 单查询超时
        max_inactive_connection_lifetime=300,
    )
```

迁移检查清单：
- [ ] `SELECT FOR UPDATE SKIP LOCKED` 替换 SQLite 锁机制
- [ ] 连接池大小 = Uvicorn workers × 5（初始值）
- [ ] 开启 `pg_stat_activity` 监控，检测长事务
- [ ] 压测验证：100 并发下 P99 < 200ms

#### 短期应急监控

```yaml
- alert: OmsSQLiteLockContention
  expr: rate(oms_db_locked_errors_total[5m]) > 0.1
  labels:
    severity: warning
  annotations:
    summary: "SQLite lock contention detected — consider Aurora migration"
```

---

## 附录：修复验证命令

```powershell
# 在修复完成后，运行以下命令验证对应场景的行为已改变

# FIX-01 验证：空 secret 现在应该被拒绝（而非接受）
pytest tests/test_chaos_middleware.py -v -k "SETTINGS_01" -s

# FIX-02 验证：REJECTED 回调可以重试
pytest tests/test_chaos_middleware.py -v -k "HMAC_02" -s

# FIX-03 验证：未知客户跨境导出被拒绝
pytest tests/test_chaos_middleware.py -v -k "PRIVACY_01" -s

# FIX-04 验证：busy_timeout 降低后不再长时间阻塞
pytest tests/test_chaos_middleware.py -v -k "DB_03" -s

# 全套回归
pytest tests/test_chaos_middleware.py -v --tb=short
```

---

## 修复状态跟踪

| Fix ID | 场景 | 优先级 | 负责人 | 目标日期 | 当前状态 |
|--------|------|--------|--------|---------|---------|
| FIX-01 | CHAOS-SETTINGS-01 空 webhook secret | 🔴 P0 | — | 立即 | 🔴 未修复 |
| FIX-02 | CHAOS-HMAC-02 REJECTED 死锁 | 🟠 P1 | — | Sprint+1 | 🔴 未修复 |
| FIX-03 | CHAOS-PRIVACY-01 未知客户归 CN | 🟠 P1 | — | Sprint+1 | 🔴 未修复 |
| FIX-04 | CHAOS-DB-03 busy_timeout 阻塞 | 🟡 P2 | — | 计划中 | 🔴 未修复 |