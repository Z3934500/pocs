# Test Organization Analysis

## 现有4个测试文件的分类

### 1. **test_docs.py** (保持在根目录)
**性质**: Meta-level test (元测试)
- 验证文档链接的有效性
- 检查测试数量的声明是否准确
- 验证git跟踪状态
- **不适合归入任何分类**，因为它测试的是"测试本身"和"文档"

**建议**: 保持在 `tests/test_docs.py`

---

### 2. **test_pipeline.py** → `tests/integration/test_olap_pipeline.py`
**性质**: Integration test (集成测试)

**理由**:
- 测试完整的数据管道 (Bronze → Silver → Gold)
- 依赖真实的数据库连接 (`connect()`)
- 测试 CDC 幂等性（需要文件系统）
- 测试 Batch Importer + Realtime Stream 协同
- 需要真实的 DuckDB/SQLite 数据库

**测试内容**:
```python
- CdcIdempotencyTest: CDC重复事件去重
- CcePipelineTest: 完整pipeline构建Gold特征
- PipelineCliTest: CLI命令行测试
```

**迁移建议**:
```bash
mv tests/test_pipeline.py tests/integration/test_olap_pipeline.py
```

---

### 3. **test_layers.py** (保持在根目录)
**性质**: Architecture test (架构测试)

**理由**:
- 验证Layer依赖关系 (L0/L1/L2)
- 检查模块导入的分层约束
- 验证文件夹命名规范
- 属于**静态架构检查**，不是功能测试

**建议**: 保持在 `tests/test_layers.py`

---

### 4. **test_oltp.py** → `tests/integration/test_oltp_system.py`
**性质**: Integration test (集成测试)

**理由**:
- 测试状态机完整流程
- 测试 Outbox Pattern（需要真实 SQLite）
- 测试 Settlement Calendar
- 测试 Risk Evaluator
- **虽然用了 FakeZSetStore，但核心是测试组件集成**

**测试内容**:
```python
- StateMachineTest: 状态机生命周期
- AuditTrailTest: 审计追踪
- RiskSeamTest: 风险评估集成
- SettlementCalendarTest: 结算日历
- OutboxTest: Outbox模式 + SQLite
- StorePortTest: 存储适配器
- BoundaryTest: 架构边界检查（可以拆分）
```

**迁移建议**:
```bash
# 主体移到 integration/
mv tests/test_oltp.py tests/integration/test_oltp_system.py

# BoundaryTest 可以拆分到单独文件（可选）
# 因为它是架构检查，类似 test_layers.py
```

---

## 最终项目结构

```
tests/
├── conftest.py                          # ✅ 新增：共享fixtures
├── README.md                            # ✅ 新增：测试文档
│
├── test_docs.py                         # 📌 保留：文档一致性检查
├── test_layers.py                       # 📌 保留：架构分层检查
│
├── unit/                                # ✅ 新增：单元测试
│   ├── test_realtime_stream.py         #    Stream Job核心逻辑
│   └── test_online_store.py            #    OnlineStore抽象
│
├── integration/                         # ✅ 混合：集成测试
│   ├── test_kafka_redis.py             #    ✅ 新增：Kafka+Redis集成
│   ├── test_olap_pipeline.py           #    🔄 重命名：test_pipeline.py
│   └── test_oltp_system.py             #    🔄 重命名：test_oltp.py
│
└── e2e/                                 # ✅ 新增：端到端测试
    └── test_stream_pipeline.py         #    完整流水线E2E
```

---

## 分类逻辑说明

### 📌 根目录保留 (2个)
- **test_docs.py**: 元测试，检查测试自身和文档
- **test_layers.py**: 架构约束，静态依赖检查

### 🔄 迁移到 integration/ (2个)
- **test_pipeline.py → test_olap_pipeline.py**: 完整OLAP管道测试
- **test_oltp.py → test_oltp_system.py**: OLTP系统集成测试

### ✅ 新增目录
- **unit/**: 纯逻辑单元测试（无外部依赖）
- **integration/**: 需要外部服务（DB/Kafka/Redis）
- **e2e/**: 完整流程端到端测试

---

## 为什么这样分类？

### 1. **隔离原则**
- **元测试** (docs, layers) 不是功能测试 → 根目录
- **单元测试** 纯逻辑 → unit/
- **集成测试** 需要外部服务 → integration/
- **端到端测试** 完整用户场景 → e2e/

### 2. **运行速度**
```bash
pytest tests/test_docs.py tests/test_layers.py    # < 2s (静态检查)
pytest tests/unit/                                 # < 5s (纯内存)
pytest tests/integration/                          # ~30s (需要DB/Kafka)
pytest tests/e2e/                                  # ~90s (完整流程)
```

### 3. **职责清晰**
- **test_docs.py**: "测试的测试"（守护文档一致性）
- **test_layers.py**: "架构守护"（守护依赖分层）
- **test_olap_pipeline.py**: "数据流集成"（OLAP管道）
- **test_oltp_system.py**: "事务系统集成"（OLTP状态机）
- **test_realtime_stream.py**: "流处理逻辑"（FeatureProcessor）
- **test_stream_pipeline.py**: "端到端场景"（Kafka→Stream Job→Redis）

---

## 实施建议

### Option 1: 完全重组（推荐）
```bash
cd c:/Users/z3934/pocs/cce-feature-platform/tests

# 迁移文件
mv test_pipeline.py integration/test_olap_pipeline.py
mv test_oltp.py integration/test_oltp_system.py

# 验证
pytest --collect-only  # 检查所有测试能否被发现
```

### Option 2: 保守方案（保留兼容性）
```bash
# 保持原文件不动，在新目录添加软链接或import
# 但这会让结构混乱，不推荐
```

---

## CI/CD 配置更新

```yaml
# .github/workflows/test.yml

jobs:
  meta-tests:
    name: Meta & Architecture Tests
    steps:
      - run: pytest tests/test_docs.py tests/test_layers.py -v

  unit-tests:
    name: Unit Tests
    steps:
      - run: pytest tests/unit/ -v --cov

  integration-tests:
    name: Integration Tests
    needs: unit-tests
    services: [kafka, redis, postgres]
    steps:
      - run: pytest tests/integration/ -v

  e2e-tests:
    name: E2E Tests
    needs: integration-tests
    steps:
      - run: docker-compose -f deploy/local/docker-compose.yml up -d
      - run: pytest tests/e2e/ -v --run-e2e
```

---

## 总结

| 文件 | 原位置 | 新位置 | 分类 | 理由 |
|------|--------|--------|------|------|
| test_docs.py | tests/ | **保留** | Meta | 测试的测试 |
| test_layers.py | tests/ | **保留** | Architecture | 架构约束 |
| test_pipeline.py | tests/ | **integration/** | Integration | 完整管道 |
| test_oltp.py | tests/ | **integration/** | Integration | 系统集成 |
| (新增) | - | unit/ | Unit | 纯逻辑 |
| (新增) | - | e2e/ | E2E | 完整场景 |

**核心原则**: 按测试目的（meta/arch/unit/integration/e2e）分类，而非按测试对象（olap/oltp）分类。
