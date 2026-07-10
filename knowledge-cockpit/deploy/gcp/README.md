# GCP Deployment Runbook

This runbook deploys Knowledge Cockpit full mode on Google Cloud. The recommended PoC path is a Compute Engine Linux VM running Nginx + systemd + the Python backend. Static-only mode can use Cloud Storage + Cloud CDN.

Full mode provides:

- HTTPS PWA at `/knowledge-cockpit/`
- phone remote control through shared sessions
- AI KB through server-side OpenAI API calls
- no OpenAI key in browser code

## Target Architecture

```text
Phone / laptop browser
  -> https://your-own-domain.example/knowledge-cockpit/
  -> Cloud DNS / external DNS
  -> External HTTP(S) Load Balancer or Cloudflare Tunnel
  -> Nginx on Compute Engine
  -> systemd service: knowledge-cockpit
  -> Python server.py on 127.0.0.1:8088
  -> OpenAI Responses API
```

## GCP Checklist

| Area | Recommended setting |
| --- | --- |
| Region / zone | One region and one zone for PoC; add managed instance group across zones for higher availability |
| Network | Default VPC is acceptable for a PoC; use a dedicated VPC/subnet for cleaner demos |
| Instance | `e2-small` or `e2-medium` is enough for the cockpit; no GPU required |
| Firewall | Allow SSH/IAP only for admin; allow 80/443 only if not using Cloudflare Tunnel |
| TLS | Google-managed certificate on HTTP(S) Load Balancer, or Cloudflare-managed TLS if using Tunnel |
| Secrets | `/etc/knowledge-cockpit.env`; do not store API keys in startup scripts or repo files |

## Shared Linux Templates

The cloud-specific runbook reuses common Linux service templates:

```text
knowledge-cockpit/deploy/linux/knowledge-cockpit.env.example
knowledge-cockpit/deploy/linux/knowledge-cockpit.service
knowledge-cockpit/deploy/linux/nginx-knowledge-cockpit.locations.conf
knowledge-cockpit/deploy/linux/cloudflared-knowledge-cockpit.example.yml
knowledge-cockpit/deploy/linux/install_linux.sh
```

## 1. Provision Compute Engine

Recommended PoC shape:

```text
OS: Ubuntu LTS or Debian
Machine type: e2-small or e2-medium
Disk: 20-30 GB balanced persistent disk
Access: SSH through IAP if possible
Outbound: HTTPS to OpenAI and package repositories
```

Install packages:

```bash
sudo apt-get update
sudo apt-get install -y git python3 nginx rsync
```

## 2. Pull The Repo

```bash
sudo mkdir -p /opt
cd /opt
sudo git clone https://github.com/<your-org-or-user>/pocs.git pocs
sudo chown -R "$USER:$USER" /opt/pocs
cd /opt/pocs
```

## 3. Install The Service

```bash
sudo APP_DIR=/opt/pocs bash knowledge-cockpit/deploy/linux/install_linux.sh
```

Edit secrets:

```bash
sudo nano /etc/knowledge-cockpit.env
sudo systemctl restart knowledge-cockpit
```

Local health:

```bash
curl http://127.0.0.1:8088/knowledge-cockpit/api/health
```

## 4. Configure Nginx

Add the shared Nginx locations into your site server block:

```text
/opt/pocs/knowledge-cockpit/deploy/linux/nginx-knowledge-cockpit.locations.conf
```

Then:

```bash
sudo nginx -t
sudo systemctl reload nginx
curl http://localhost/knowledge-cockpit/api/health
```

## 5. Choose Public Exposure

| Option | Best for | Notes |
| --- | --- | --- |
| Cloudflare Tunnel | Fast demo, no inbound 80/443 on VM | Public hostname points to `http://localhost:80` |
| External HTTP(S) Load Balancer | GCP-native production path | Use managed certificate and backend service |
| Cloud Run | Containerized future path | Requires a container image and stateless session persistence design |
| Cloud Storage + Cloud CDN | Static mode only | No AI KB or remote control backend |

## 6. Verify

```text
https://your-own-domain.example/knowledge-cockpit/
```

```bash
curl https://your-own-domain.example/knowledge-cockpit/api/health
```

## Update

```bash
cd /opt/pocs
git pull
sudo chown -R knowledge:knowledge /opt/pocs
sudo systemctl restart knowledge-cockpit
sudo nginx -t
sudo systemctl reload nginx
```

## GCP Notes

- Prefer IAP for SSH instead of exposing SSH broadly.
- Use Cloud Logging Ops Agent if you want VM logs and metrics in Cloud Logging.
- The backend listens only on `127.0.0.1:8088`; do not open that port publicly.
- For production, move from one VM to a managed instance group or Cloud Run-compatible container design.
