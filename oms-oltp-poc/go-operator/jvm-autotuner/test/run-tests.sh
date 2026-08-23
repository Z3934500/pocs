#!/usr/bin/env bash
# test/run-tests.sh — Linux/CI 入口，对应 run-tests.ps1
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
MODULE_DIR="$REPO_ROOT/oms-oltp-poc/go-operator/jvm-autotuner"

E2E=${E2E:-0}
E2E_KIND_CLUSTER=${E2E_KIND_CLUSTER:-jvm-autotuner-e2e}
LOG_DIR=${LOG_DIR:-$SCRIPT_DIR/../logs}

mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
UNIT_LOG="$LOG_DIR/unit_${TIMESTAMP}.log"
E2E_LOG="$LOG_DIR/e2e_${TIMESTAMP}.log"

echo "=== JVM AutoTuner Tests ===" | tee "$UNIT_LOG"
echo "Module: $MODULE_DIR"

cd "$MODULE_DIR"

echo "--- L0 Unit/Package Tests ---" | tee -a "$UNIT_LOG"
go test ./internal/... -count=1 -v 2>&1 | tee -a "$UNIT_LOG"

if [[ "$E2E" == "1" ]]; then
  echo "--- L1 Kind E2E ---" | tee "$E2E_LOG"
  export E2E_KIND=1
  export KUBECONFIG
  KUBECONFIG=$(kind get kubeconfig --name "$E2E_KIND_CLUSTER" 2>/dev/null || echo "")
  if [[ -z "$KUBECONFIG" ]]; then
    echo "ERROR: Kind cluster '$E2E_KIND_CLUSTER' not found. Start it first:" | tee -a "$E2E_LOG"
    echo "  kind create cluster --name $E2E_KIND_CLUSTER" | tee -a "$E2E_LOG"
    exit 1
  fi
  go test -tags=e2e ./e2e -v -count=1 -timeout 3m 2>&1 | tee -a "$E2E_LOG"
fi

echo ""
echo "Logs written to $LOG_DIR"
