# test/collect-lag-hpa-evidence.ps1
# Grafana 混合视图证据采集脚本
# 用途：在压测过程中采集 kafka_consumer_lag / hpa_desired_replicas / pod_ready_status
#       三条曲线的时序数据，重现 "Lag飙升 → HPA触发 → Pod Ready" 的时间轴缺口分析
#
# 使用方法：
#   .\test\collect-lag-hpa-evidence.ps1 `
#     -PrometheusUrl http://prometheus:9090 `
#     -Namespace oms-prod `
#     -Deployment order-service `
#     -LagThreshold 500 `
#     -SampleIntervalSeconds 15 `
#     -DurationMinutes 10

param(
    [Parameter(Mandatory=$true)][string]$PrometheusUrl,
    [string]$Namespace     = "oms-prod",
    [string]$Deployment    = "order-service",
    [string]$HpaName       = "order-service",
    [string]$ConsumerGroup = "order-service",
    [string]$KafkaTopic    = "inventory.reserve",
    [int]$LagThreshold          = 500,   # 新阈值；原始值 1000 作为注释保留
    [int]$SampleIntervalSeconds = 15,
    [int]$DurationMinutes       = 10,
    [string]$Output = "test/artifacts/lag-hpa-evidence"
)

$ErrorActionPreference = "Stop"
$root   = Split-Path -Parent $PSScriptRoot
$outDir = Join-Path $root $Output
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$stamp  = Get-Date -Format "yyyyMMdd-HHmmss"
$outFile = Join-Path $outDir "$stamp-timeline.csv"
$summaryFile = Join-Path $outDir "$stamp-gap-analysis.txt"

# ── Prometheus 即时查询辅助函数 ─────────────────────────────────────────────
function Query-Prom([string]$q) {
    $enc = [uri]::EscapeDataString($q)
    $url = "$($PrometheusUrl.TrimEnd('/'))/api/v1/query?query=$enc"
    try {
        $resp = Invoke-RestMethod -Uri $url -TimeoutSec 5
        if ($resp.status -eq "success" -and $resp.data.result.Count -gt 0) {
            return [double]$resp.data.result[0].value[1]
        }
    } catch { }
    return $null
}

# ── PromQL 查询定义 ─────────────────────────────────────────────────────────
# Lag: sum of consumer lag across all partitions for this group+topic
$qLag = "sum(kafka_consumergroup_lag{group=`"$ConsumerGroup`",topic=`"$KafkaTopic`"})"
# 若使用 oms 自定义指标则换成：
# $qLag = "oms_kafka_consumer_lag{group=`"$ConsumerGroup`",topic=`"$KafkaTopic`"}"

$qHpaDesired  = "kube_hpa_status_desired_replicas{namespace=`"$Namespace`",horizontalpodautoscaler=`"$HpaName`"}"
$qHpaCurrent  = "kube_hpa_status_current_replicas{namespace=`"$Namespace`",horizontalpodautoscaler=`"$HpaName`"}"
$qPodReady    = "sum(kube_pod_status_ready{namespace=`"$Namespace`",pod=~`"$Deployment-.*`",condition=`"true`"})"
$qDeployAvail = "kube_deployment_status_replicas_available{namespace=`"$Namespace`",deployment=`"$Deployment`"}"
$qJvmHeapPct  = "avg(jvm_memory_used_bytes{area=`"heap`",namespace=`"$Namespace`",pod=~`"$Deployment-.*`"} / jvm_memory_max_bytes{area=`"heap`",namespace=`"$Namespace`",pod=~`"$Deployment-.*`"}) * 100"

# ── CSV ヘッダ ───────────────────────────────────────────────────────────────
"timestamp,lag,hpa_desired,hpa_current,pods_ready,deploy_available,jvm_heap_pct" | Out-File $outFile

# ── 关键时间点记录（用于缺口分析）──────────────────────────────────────────
$lagSpikeTime    = $null   # Lag 首次超过阈值的时间
$hpaTriggerTime  = $null   # hpa_desired 首次增加的时间
$podReadyTime    = $null   # pods_ready 回到期望值的时间
$prevHpaDesired  = $null
$prevLag         = $null

Write-Host "[$stamp] 开始采集：每 ${SampleIntervalSeconds}s 采样，持续 ${DurationMinutes}min" -ForegroundColor Cyan
Write-Host "    Prometheus : $PrometheusUrl"
Write-Host "    Namespace  : $Namespace / Deployment: $Deployment"
Write-Host "    Lag阈值    : $LagThreshold（原始值 1000，已调优至 $LagThreshold）"
Write-Host "    输出       : $outDir"
Write-Host ""

$endTime   = (Get-Date).AddMinutes($DurationMinutes)
$sampleNum = 0

while ((Get-Date) -lt $endTime) {
    $now = Get-Date
    $ts  = $now.ToString("o")
    $sampleNum++

    $lag      = Query-Prom $qLag
    $hpaDes   = Query-Prom $qHpaDesired
    $hpaCur   = Query-Prom $qHpaCurrent
    $podReady = Query-Prom $qPodReady
    $depAvail = Query-Prom $qDeployAvail
    $heapPct  = Query-Prom $qJvmHeapPct

    "$ts,$lag,$hpaDes,$hpaCur,$podReady,$depAvail,$heapPct" | Add-Content $outFile

    # ── 关键事件检测 ──────────────────────────────────────────────────────────
    if ($lag -ne $null -and $lagSpikeTime -eq $null -and $lag -gt $LagThreshold) {
        $lagSpikeTime = $now
        Write-Host "  ⚡ [LAG SPIKE]  $($now.ToString('HH:mm:ss'))  Lag=$lag > $LagThreshold" -ForegroundColor Yellow
    }

    if ($hpaDes -ne $null -and $prevHpaDesired -ne $null -and $hpaTriggerTime -eq $null) {
        if ($hpaDes -gt $prevHpaDesired) {
            $hpaTriggerTime = $now
            Write-Host "  🔼 [HPA SCALED] $($now.ToString('HH:mm:ss'))  desired $prevHpaDesired → $hpaDes" -ForegroundColor Cyan
        }
    }

    if ($hpaTriggerTime -ne $null -and $podReadyTime -eq $null -and $podReady -ne $null) {
        if ($hpaDes -ne $null -and $podReady -ge $hpaDes) {
            $podReadyTime = $now
            Write-Host "  ✅ [PODS READY]  $($now.ToString('HH:mm:ss'))  ready=$podReady >= desired=$hpaDes" -ForegroundColor Green
        }
    }

    $prevHpaDesired = $hpaDes
    $prevLag        = $lag

    $status = "lag={0,6}  hpa={1}/{2}  pods_ready={3}  heap={4:F0}%" -f `
        ($lag ?? "-"), ($hpaCur ?? "-"), ($hpaDes ?? "-"), ($podReady ?? "-"), ($heapPct ?? 0)
    Write-Host "  [$($now.ToString('HH:mm:ss'))] $status"

    Start-Sleep $SampleIntervalSeconds
}

# ── 缺口分析报告 ─────────────────────────────────────────────────────────────
$report = @"
=== Lag → HPA → Pod Ready 缺口分析 ===
采集时间   : $stamp
Namespace  : $Namespace / $Deployment
Lag 阈值   : $LagThreshold（原阈值 1000）

关键时间点
----------
Lag 首次超阈值  : $(if ($lagSpikeTime)   { $lagSpikeTime.ToString('HH:mm:ss')   } else { "未观察到" })
HPA 触发扩容    : $(if ($hpaTriggerTime) { $hpaTriggerTime.ToString('HH:mm:ss') } else { "未观察到" })
新 Pod Ready    : $(if ($podReadyTime)   { $podReadyTime.ToString('HH:mm:ss')   } else { "未观察到" })

缺口计算
--------
$(
    if ($lagSpikeTime -and $hpaTriggerTime) {
        $d1 = ($hpaTriggerTime - $lagSpikeTime).TotalSeconds
        "Lag→HPA 延迟        : {0:F0} 秒  (目标 < 90s，sync-period 调低后)" -f $d1
    } else { "Lag→HPA 延迟        : 未采集到完整数据" }
)
$(
    if ($hpaTriggerTime -and $podReadyTime) {
        $d2 = ($podReadyTime - $hpaTriggerTime).TotalSeconds
        "HPA→Pod Ready 延迟  : {0:F0} 秒  (JVM启动 + Spring Context 初始化)" -f $d2
    } else { "HPA→Pod Ready 延迟  : 未采集到完整数据" }
)
$(
    if ($lagSpikeTime -and $podReadyTime) {
        $total = ($podReadyTime - $lagSpikeTime).TotalSeconds
        "总保护缺口           : {0:F0} 秒  (目标 < 60s；优化前约 240s)" -f $total
    } else { "总保护缺口           : 未采集到完整数据" }
)

优化前后对比（参考基线）
------------------------
指标                    优化前      优化后（目标）
Lag 触发阈值            1000        $LagThreshold
HPA sync-period         默认15s     调低后
Lag→HPA 响应时间        约 2.5 min  < 90 s
总缺口（Lag→Ready）     约 4 min    < 60 s

原始 CSV 时序数据  : $outFile
"@

$report | Out-File $summaryFile
Write-Host ""
Write-Host $report -ForegroundColor White
Write-Host "`n[完成] 缺口分析报告: $summaryFile" -ForegroundColor Green
