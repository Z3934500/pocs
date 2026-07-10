#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/pocs}"
SERVICE_USER="${SERVICE_USER:-knowledge}"
SERVICE_NAME="knowledge-cockpit"
PORT="${PORT:-8088}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root or with sudo: sudo APP_DIR=${APP_DIR} bash knowledge-cockpit/deploy/linux/install_linux.sh"
  exit 1
fi

if [[ ! -f "knowledge-cockpit/server.py" ]]; then
  echo "Run this from the repository root that contains knowledge-cockpit/server.py"
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required. Install it with your OS package manager first."
  exit 1
fi

if ! id "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd --system --home-dir "${APP_DIR}" --shell /sbin/nologin "${SERVICE_USER}"
fi

mkdir -p "${APP_DIR}"
rsync -a --delete \
  --exclude '.git' \
  --exclude '**/__pycache__' \
  --exclude '**/.pytest_cache' \
  ./ "${APP_DIR}/"

chown -R "${SERVICE_USER}:${SERVICE_USER}" "${APP_DIR}"

if [[ ! -f /etc/knowledge-cockpit.env ]]; then
  install -m 600 knowledge-cockpit/deploy/linux/knowledge-cockpit.env.example /etc/knowledge-cockpit.env
  echo "Created /etc/knowledge-cockpit.env. Edit OPENAI_API_KEY before using AI KB."
fi

install -m 644 knowledge-cockpit/deploy/linux/knowledge-cockpit.service /etc/systemd/system/${SERVICE_NAME}.service

systemctl daemon-reload
systemctl enable ${SERVICE_NAME}
systemctl restart ${SERVICE_NAME}

cat <<MSG

Service installed.

Check status:
  systemctl status ${SERVICE_NAME} --no-pager

Check local health:
  curl http://127.0.0.1:${PORT}/knowledge-cockpit/api/health

Next:
  1. Add Nginx locations from:
     ${APP_DIR}/knowledge-cockpit/deploy/linux/nginx-knowledge-cockpit.locations.conf
  2. Point your domain, load balancer or Cloudflare Tunnel to local Nginx on port 80.

MSG
