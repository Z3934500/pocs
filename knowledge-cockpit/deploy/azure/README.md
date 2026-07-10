# Azure Deployment Runbook

This runbook deploys Knowledge Cockpit full mode on Azure. The recommended PoC path is an Azure Linux VM running Nginx + systemd + the Python backend. Static-only mode can use Azure Storage Static Website + Azure CDN or Front Door.

Full mode provides:

- HTTPS PWA at `/knowledge-cockpit/`
- phone remote control through shared sessions
- AI KB through server-side OpenAI API calls
- no OpenAI key in browser code

## Target Architecture

```text
Phone / laptop browser
  -> https://your-own-domain.example/knowledge-cockpit/
  -> Azure DNS / external DNS
  -> Azure Application Gateway / Front Door, or Cloudflare Tunnel
  -> Nginx on Azure Linux VM
  -> systemd service: knowledge-cockpit
  -> Python server.py on 127.0.0.1:8088
  -> OpenAI Responses API
```

## Azure Checklist

| Area | Recommended setting |
| --- | --- |
| Region / zone | One region for PoC; use availability zones or VM scale set for higher availability |
| Network | VNet with a public subnet for direct ingress or private VM with Cloudflare Tunnel |
| Instance | `B2s`/`B2ms` is enough for the cockpit; no GPU required |
| NSG | Allow SSH only from your IP or through Bastion; allow 80/443 only if not using Cloudflare Tunnel |
| TLS | Application Gateway/Front Door certificate, or Cloudflare-managed TLS if using Tunnel |
| Secrets | `/etc/knowledge-cockpit.env`; do not store API keys in ARM/Bicep parameters committed to git |

## Shared Linux Templates

The cloud-specific runbook reuses common Linux service templates:

```text
knowledge-cockpit/deploy/linux/knowledge-cockpit.env.example
knowledge-cockpit/deploy/linux/knowledge-cockpit.service
knowledge-cockpit/deploy/linux/nginx-knowledge-cockpit.locations.conf
knowledge-cockpit/deploy/linux/cloudflared-knowledge-cockpit.example.yml
knowledge-cockpit/deploy/linux/install_linux.sh
```

## 1. Provision Azure Linux VM

Recommended PoC shape:

```text
OS: Ubuntu LTS
Size: B2s or B2ms
Disk: 30 GB Standard SSD or Premium SSD
Access: SSH restricted by NSG or Azure Bastion
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
| Application Gateway | Azure-native production path | Use backend pool on VM port 80 and managed TLS |
| Azure Front Door | Global edge entry point | Useful if multiple origins or global routing are needed |
| Azure Storage Static Website | Static mode only | No AI KB or remote control backend |

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

## Azure Notes

- Prefer Azure Bastion or restricted NSG rules for SSH.
- Use Azure Monitor Agent if you want VM logs and metrics in Azure Monitor.
- The backend listens only on `127.0.0.1:8088`; do not open that port publicly.
- For production, move from one VM to an image/scale-set pattern or a containerized App Service/Container Apps design.
