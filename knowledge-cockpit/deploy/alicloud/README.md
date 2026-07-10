# Legacy Alibaba Cloud ECS Deployment Runbook

This legacy runbook deploys Knowledge Cockpit full mode on an existing Alibaba Cloud ECS server behind `your-own-domain.example` using Cloudflare Tunnel. For new multi-cloud demos, prefer the AWS, GCP or Azure runbooks under `knowledge-cockpit/deploy/`.

Full mode provides:

- HTTPS PWA at `/knowledge-cockpit/`
- phone remote control through shared sessions
- AI KB through server-side OpenAI API calls
- no OpenAI key in browser code

## Target Architecture

```text
Phone / laptop browser
  -> https://your-own-domain.example/knowledge-cockpit/
  -> Cloudflare DNS / Cloudflare Tunnel
  -> cloudflared service on Alibaba Cloud ECS
  -> local Nginx on ECS
  -> systemd service: knowledge-cockpit
  -> Python server.py on 127.0.0.1:8088
  -> OpenAI Responses API
```

The public hostname does not depend on the ECS public IP. `cloudflared` creates an outbound tunnel from ECS to Cloudflare, so changing the ECS public IP normally does not require a DNS update.

## Files In This Folder

| File | Purpose |
| --- | --- |
| `knowledge-cockpit.env.example` | Server-side environment variable template. Copy to `/etc/knowledge-cockpit.env`. |
| `knowledge-cockpit.service` | systemd service template. Runs the Python server on `127.0.0.1:8088`. |
| `nginx-knowledge-cockpit.locations.conf` | Nginx location snippets for your existing `your-own-domain.example` server block. |
| `cloudflared-knowledge-cockpit.example.yml` | Optional named-tunnel config example. Dashboard-managed tunnels usually do not need this file. |
| `install_alicloud.sh` | Optional install helper for ECS Linux servers. Run from the repo root. |

## 1. Alibaba Cloud Checklist

In Alibaba Cloud and Cloudflare:

1. ECS instance is running.
2. `your-own-domain.example` is routed through Cloudflare Tunnel to this ECS instance; it does not depend on the ECS public IP.
3. `cloudflared` is installed as a service on ECS, or you are ready to install it with a Cloudflare Tunnel token.
4. SSH access is available to you.
5. Nginx is installed locally on ECS and can listen on `localhost:80` or port `80`.

With Cloudflare Tunnel, Alibaba Cloud Security Group does not need inbound `80` or `443` for this app. Keep SSH access restricted. The app backend itself listens only on `127.0.0.1:8088`, so port `8088` must not be opened publicly.

## 2. Upload Or Pull The Repo

On ECS, choose one path. This runbook uses `/opt/pocs`.

Repository: [your-org-or-user/pocs](https://github.com/<your-org-or-user>/pocs)

Using git:

```bash
sudo mkdir -p /opt
cd /opt
sudo git clone https://github.com/<your-org-or-user>/pocs.git pocs
sudo chown -R "$USER:$USER" /opt/pocs
```

Or upload the repo with `scp`/SFTP, then place it at:

```text
/opt/pocs
```

## 3. Configure Server Secrets

```bash
sudo cp /opt/pocs/knowledge-cockpit/deploy/alicloud/knowledge-cockpit.env.example /etc/knowledge-cockpit.env
sudo chmod 600 /etc/knowledge-cockpit.env
sudo nano /etc/knowledge-cockpit.env
```

Set:

```bash
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4.1-mini
OPENAI_RESPONSES_URL=https://api.openai.com/v1/responses
```

Do not commit the real key.

## 4. Install systemd Service

Create the service user:

```bash
sudo useradd --system --home-dir /opt/pocs --shell /sbin/nologin knowledge || true
sudo chown -R knowledge:knowledge /opt/pocs
```

Install the service:

```bash
sudo cp /opt/pocs/knowledge-cockpit/deploy/alicloud/knowledge-cockpit.service /etc/systemd/system/knowledge-cockpit.service
sudo systemctl daemon-reload
sudo systemctl enable knowledge-cockpit
sudo systemctl restart knowledge-cockpit
```

Check it:

```bash
sudo systemctl status knowledge-cockpit --no-pager
curl http://127.0.0.1:8088/knowledge-cockpit/api/health
```

Expected health response includes:

```json
{"ok": true, "chunks": 40, "openai_configured": true}
```

The exact chunk count may differ.

## 5. Configure Nginx

Open your existing Nginx config for `your-own-domain.example` and add the locations from:

```text
/opt/pocs/knowledge-cockpit/deploy/alicloud/nginx-knowledge-cockpit.locations.conf
```

The important routes are namespaced:

```text
/knowledge-cockpit/
/knowledge-cockpit/api/
```

This avoids colliding with an existing `/api` on your website.

Test and reload:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## 6. Configure Cloudflare Tunnel

If your tunnel is already created in the Cloudflare Zero Trust dashboard, add or confirm a public hostname route:

```text
Hostname: your-own-domain.example
Service:  http://localhost:80
```

This routes Cloudflare traffic to local Nginx. Keep this route pointed at Nginx instead of `127.0.0.1:8088` if the same domain also serves your existing website. Nginx then forwards only these app paths to the Python service:

```text
/knowledge-cockpit/
/knowledge-cockpit/api/
```

If `cloudflared` is not installed yet, create a Cloudflare Tunnel in the Cloudflare Zero Trust dashboard and copy the Linux service install command. It usually looks like:

```bash
sudo cloudflared service install <CLOUDFLARE_TUNNEL_TOKEN>
sudo systemctl enable cloudflared
sudo systemctl restart cloudflared
sudo systemctl status cloudflared --no-pager
```

Do not commit or paste the real tunnel token into this repo.

For a named-tunnel config-file setup, use this file as a reference:

```text
/opt/pocs/knowledge-cockpit/deploy/alicloud/cloudflared-knowledge-cockpit.example.yml
```

After the tunnel is running, confirm that the ECS server can reach the local app path through Nginx:

```bash
curl http://localhost/knowledge-cockpit/api/health
```

## 7. Verify Public URL

Open:

```text
https://your-own-domain.example/knowledge-cockpit/
```

Then test:

```bash
curl https://your-own-domain.example/knowledge-cockpit/api/health
```


## Private Presenter Notes

The public screen role hides presenter notes and AI KB. Use the phone in `Presenter` mode as your private notes device.

For cloud deployment, set a server-side PIN:

```bash
PRESENTER_PIN=change-me-before-demo
```

When `PRESENTER_PIN` is set, phone control and AI KB require the PIN. The screen can still read public sync state, but it will not show the private notes panel.
## 8. Phone Presenter Notes Demo

1. Open `https://your-own-domain.example/knowledge-cockpit/` on the presentation screen.
2. Open the same URL on your phone.
3. Use the same `Remote Session`, for example `demo`.
4. Screen role: `Screen`.
5. Phone role: `Remote`.
6. Tap a term or demo step on the phone. The screen should follow.

Because both devices use the cloud URL, they do not need to be on the same Wi-Fi.

## 9. AI KB Demo

1. Click `AI KB`.
2. Ask:

```text
What is the difference between OLTP sharding and OLAP partitioning?
```

3. Confirm the answer includes source files/headings.

## 10. Install On Phone

After the HTTPS URL works:

- iPhone Safari: Share -> Add to Home Screen.
- Android Chrome: menu -> Install app or Add to Home screen.

The installed app will still call the same cloud backend.

## 11. Update Deployment

After pushing changes to GitHub:

```bash
cd /opt/pocs
git pull
sudo chown -R knowledge:knowledge /opt/pocs
sudo systemctl restart knowledge-cockpit
sudo nginx -t
sudo systemctl reload nginx
```

No Cloudflare DNS change is needed after normal code updates. Restart `cloudflared` only if the tunnel service/config changed.

## 12. Troubleshooting

Service logs:

```bash
sudo journalctl -u knowledge-cockpit -f
```

Cloudflare Tunnel status and logs:

```bash
sudo systemctl status cloudflared --no-pager
sudo journalctl -u cloudflared -f
```

Local backend health:

```bash
curl http://127.0.0.1:8088/knowledge-cockpit/api/health
```

Public backend health:

```bash
curl https://your-own-domain.example/knowledge-cockpit/api/health
```

Common issues:

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Public page 404 | Nginx location missing or wrong prefix | Add `/knowledge-cockpit/` location and reload Nginx |
| AI KB says key missing | `/etc/knowledge-cockpit.env` not configured or service not restarted | Edit env file and `systemctl restart knowledge-cockpit` |
| Phone does not control screen | Different session id or backend API not reachable | Use same session and check `/knowledge-cockpit/api/health` |
| PWA install not shown | Not using HTTPS or browser install prompt hidden | Use HTTPS and browser menu Add to Home Screen |
| 502 from Nginx | Python service down | `systemctl status knowledge-cockpit` and check logs |
| Cloudflare 1033 / tunnel error | `cloudflared` is down, token is wrong, or public hostname route is missing | Check `systemctl status cloudflared`, Cloudflare Tunnel dashboard and public hostname route |
| Domain still works after ECS IP changes | Expected with Cloudflare Tunnel | No DNS update needed; keep `cloudflared` running on the new/active ECS host |

## 13. Optional One-Command Helper

From the repo root on ECS:

```bash
sudo APP_DIR=/opt/pocs bash knowledge-cockpit/deploy/alicloud/install_alicloud.sh
```

Then edit `/etc/knowledge-cockpit.env`, add the Nginx locations, confirm the Cloudflare Tunnel public hostname points to `http://localhost:80`, and reload services.