# JVM/OMS 可观测性运行手册

## 常态检查

每个服务的 Actuator 暴露：

```text
/actuator/health
/actuator/metrics
/actuator/prometheus
```

重点查询：

- `jvm_memory_used_bytes` / `jvm_memory_max_bytes`
- `jvm_gc_pause_seconds`
- `jvm_threads_live_threads` / `jvm_threads_peak_threads`
- `http_server_requests_seconds`
- `oms_outbox_oldest_age_seconds`
- `oms_seckill_stream_length` / `oms_seckill_dlq_depth`
- `oms_reservation_expiration_total` / `oms_reservation_expiration_duration`

生产环境应把这些指标接入 Prometheus/ADOT 后，再按 service、route 和受控业务维度配置告警。不要把用户 ID、订单 ID 作为 Prometheus label。

## JFR 深度诊断

在 `inventory-oms-poc` 目录执行：

```powershell
.\observability\jfr\start-jfr.ps1 -ProcessId <PID>
# 复现或等待问题窗口
.\observability\jfr\stop-jfr.ps1 -ProcessId <PID>
```

脚本默认使用 `profile` 配置、15 分钟最大时长和 256 MB 文件上限。用 JDK Mission Control 查看 GC Pause、Java Monitor Block、线程、Socket/文件 I/O 和热点方法；诊断文件不得上传包含敏感业务数据的公共位置。

## KRaft 本地验证

```powershell
docker compose -f .\docker-compose.kraft.yml up -d
```

应用连接仍使用 `localhost:9092`。现有 `docker-compose.yml` 继续保留 ZooKeeper 版本，用于兼容性对比。
