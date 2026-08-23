#!/usr/bin/env bash
# test/collect-hpa-evidence.sh — Linux/CI 版本
set -euo pipefail

PROMETHEUS_URL=${PROMETHEUS_URL:-http://prometheus:9090}
NAMESPACE=${NAMESPACE:-oms-prod}
OUT_DIR=${OUT_DIR:-./evidence}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUT="$OUT_DIR/$TIMESTAMP"
mkdir -p "$OUT"

echo "Collecting HPA evidence → $OUT"

kubectl get hpa -n "$NAMESPACE" -o yaml > "$OUT/hpa.yaml"
kubectl get deployment -n "$NAMESPACE" -o yaml > "$OUT/deployments.yaml"
kubectl get pods -n "$NAMESPACE" -o wide > "$OUT/pods.txt"
kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp > "$OUT/events.txt"

prom_query() {
  local name=$1 query=$2
  curl -sG "$PROMETHEUS_URL/api/v1/query" \
    --data-urlencode "query=$query" \
    -o "$OUT/${name}.json"
  echo "  $name.json"
}

prom_query "kafka_lag"       "oms_kafka_consumer_lag"
prom_query "hpa_desired"     "kube_hpa_status_desired_replicas{namespace=\"$NAMESPACE\"}"
prom_query "pod_ready"       "kube_pod_status_ready{namespace=\"$NAMESPACE\",condition=\"true\"}"
prom_query "pod_created"     "kube_pod_created{namespace=\"$NAMESPACE\"}"
prom_query "jvm_heap_used"   "jvm_memory_used_bytes{area=\"heap\",namespace=\"$NAMESPACE\"}"
prom_query "jvm_gc_pause"    "jvm_gc_pause_seconds_sum{namespace=\"$NAMESPACE\"}"

echo "Done. Evidence in $OUT"
