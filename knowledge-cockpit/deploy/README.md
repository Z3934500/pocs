# Knowledge Cockpit Deployment Runbooks

Use these runbooks to deploy Knowledge Cockpit in static mode or full mode.

## Full Mode Cloud Runbooks

| Cloud | Runbook | Recommended PoC shape |
| --- | --- | --- |
| AWS | `aws/README.md` | EC2 + Nginx + systemd, with ALB/CloudFront or Cloudflare Tunnel |
| GCP | `gcp/README.md` | Compute Engine + Nginx + systemd, with External HTTP(S) Load Balancer or Cloudflare Tunnel |
| Azure | `azure/README.md` | Azure Linux VM + Nginx + systemd, with Application Gateway/Front Door or Cloudflare Tunnel |

## Shared Linux Templates

These files are reused by the cloud-specific VM runbooks:

```text
linux/knowledge-cockpit.env.example
linux/knowledge-cockpit.service
linux/nginx-knowledge-cockpit.locations.conf
linux/cloudflared-knowledge-cockpit.example.yml
linux/install_linux.sh
```

## Legacy Reference

The original Alibaba Cloud ECS runbook remains available for comparison:

```text
alicloud/README.md
```

New cloud demos should usually start with AWS, GCP or Azure.
