# AWS Deployment Runbook

This runbook deploys Knowledge Cockpit full mode on AWS. The recommended low-friction path is an EC2 Linux VM running Nginx + systemd + the Python backend. Static-only mode can use S3 + CloudFront.

Full mode provides:

- HTTPS PWA at `/knowledge-cockpit/`
- phone remote control through shared sessions
- AI KB through server-side OpenAI API calls
- no OpenAI key in browser code

## Target Architecture

```text
Phone / laptop browser
  -> https://your-own-domain.example/knowledge-cockpit/
  -> Route 53 / external DNS
  -> CloudFront or ALB, or Cloudflare Tunnel
  -> Nginx on EC2
  -> systemd service: knowledge-cockpit
  -> Python server.py on 127.0.0.1:8088
  -> OpenAI Responses API
```

## AWS Checklist

| Area | Recommended setting |
| --- | --- |
| Region / AZ | One region; start with one EC2 instance for PoC, move to two AZs behind ALB for higher availability |
| VPC | Public subnet for direct ALB/HTTPS path, or private subnet if using SSM Session Manager plus Cloudflare Tunnel |
| Instance | `t3.small`/`t3.medium` is enough for a lightweight cockpit; no GPU required |
| Security group | Allow SSH only from your IP or use SSM; allow 80/443 only if not using Cloudflare Tunnel |
| TLS | CloudFront/ALB ACM certificate, or Cloudflare-managed TLS if using Tunnel |
| Secrets | `/etc/knowledge-cockpit.env`; never commit `OPENAI_API_KEY` |

## Shared Linux Templates

The cloud-specific runbook reuses common Linux service templates:

```text
knowledge-cockpit/deploy/linux/knowledge-cockpit.env.example
knowledge-cockpit/deploy/linux/knowledge-cockpit.service
knowledge-cockpit/deploy/linux/nginx-knowledge-cockpit.locations.conf
knowledge-cockpit/deploy/linux/cloudflared-knowledge-cockpit.example.yml
knowledge-cockpit/deploy/linux/install_linux.sh
```

## 1. Provision EC2

Recommended PoC shape:

```text
Amazon Linux 2023 or Ubuntu LTS
Instance type: t3.small or t3.medium
Storage: 20-30 GB gp3
Inbound: SSH from your IP, or no SSH if using SSM
Outbound: HTTPS to OpenAI and package repositories
```

Install packages:

```bash
if command -v dnf >/dev/null 2>&1; then
  sudo dnf install -y git python3 nginx rsync
else
  sudo apt-get update
  sudo apt-get install -y git python3 nginx rsync
fi
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
| Cloudflare Tunnel | Fast demo, no inbound 80/443 on EC2 | Public hostname points to `http://localhost:80` |
| ALB + ACM | AWS-native production path | Put EC2 in target group on port 80; TLS cert in ACM |
| CloudFront + ALB | Global edge cache/TLS | Useful if you also serve static assets through CloudFront |
| S3 + CloudFront | Static mode only | No AI KB or remote control backend |

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

## AWS Notes

- Use IAM roles and SSM Session Manager if you want to avoid public SSH.
- Use CloudWatch Agent if you want VM-level logs and metrics.
- The backend listens only on `127.0.0.1:8088`; do not open that port publicly.
- For production, move from one EC2 instance to an AMI/Launch Template behind ALB.
