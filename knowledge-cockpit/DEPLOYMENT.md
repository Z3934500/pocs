# Deployment Steps

The cockpit supports two deployment modes.

- Static mode: no backend, no API key, installable PWA when served over HTTPS.
- Full mode: Python backend for phone remote control and AI knowledge-base answers.

## Option 1: Static PWA Under Your Website

Use this if you only need terminology cards, demo script, Q&A, graph and presenter notes.

1. Confirm the app works locally.

From the repository root, `pocs`, run:

```powershell
python -m http.server 8088
```

Then open:

```text
http://localhost:8088/knowledge-cockpit/
```

If you are already inside `pocs\knowledge-cockpit`, run the same command but open:

```text
http://localhost:8088/
```

2. Copy the whole folder to your website static directory:

```text
knowledge-cockpit/
  index.html
  style.css
  data.js
  app.js
  manifest.webmanifest
  sw.js
  assets/
```

3. Serve it at one of these paths:

```text
https://your-own-domain.example/knowledge/
https://your-own-domain.example/knowledge-cockpit/
```

4. If you use Nginx, a simple static location can look like this:

```nginx
location /knowledge-cockpit/ {
  alias /var/www/site/knowledge-cockpit/;
  try_files $uri $uri/ /knowledge-cockpit/index.html;
}
```

5. Reload Nginx:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

6. Install on phone:

- iPhone Safari: Share -> Add to Home Screen.
- Android Chrome: menu -> Install app or Add to Home screen.

## Option 2: Full Mode With Remote Control And AI KB

Use this if you want phone remote control and OpenAI-backed repo Q&A.

1. Put the repo on the server that hosts your website.

2. Set environment variables on the server:

```bash
export OPENAI_API_KEY="sk-..."
export OPENAI_MODEL="gpt-4.1-mini"
```

3. Start the cockpit server:

```bash
cd /var/www/pocs
python knowledge-cockpit/server.py --host 127.0.0.1 --port 8088
```

4. Put it behind Nginx:

```nginx
location ^~ /knowledge-cockpit/api/ {
  proxy_pass http://127.0.0.1:8088/knowledge-cockpit/api/;
  proxy_http_version 1.1;
  proxy_set_header Host $host;
  proxy_set_header X-Real-IP $remote_addr;
  proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
  proxy_set_header X-Forwarded-Proto $scheme;
  proxy_read_timeout 120s;
  proxy_send_timeout 120s;
  proxy_buffering off;
}

location ^~ /knowledge-cockpit/ {
  proxy_pass http://127.0.0.1:8088/knowledge-cockpit/;
  proxy_http_version 1.1;
  proxy_set_header Host $host;
  proxy_set_header X-Real-IP $remote_addr;
  proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
  proxy_set_header X-Forwarded-Proto $scheme;
  proxy_read_timeout 120s;
  proxy_send_timeout 120s;
}
```

5. Reload Nginx:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

6. Open the app:

```text
https://your-own-domain.example/knowledge-cockpit/
```

7. Phone presenter setup:

- Open the same URL on the screen and phone.
- Use the same `Remote Session`, such as `demo`.
- Keep the screen role as `Screen`.
- Set the phone role to `Remote`.
- Click terms or demo steps on the phone; the screen follows.

8. AI KB setup:

- Click `AI KB`.
- Ask a repo-specific question.
- The answer should include source file/headings.

## Cloud VM Recommended Paths

For full mode on a cloud Linux VM, use one of the cloud-specific runbooks:

| Cloud | Runbook | Typical PoC shape |
| --- | --- | --- |
| AWS | `knowledge-cockpit/deploy/aws/README.md` | EC2 + Nginx + systemd, optionally behind Cloudflare Tunnel, ALB or CloudFront |
| GCP | `knowledge-cockpit/deploy/gcp/README.md` | Compute Engine + Nginx + systemd, optionally behind Cloudflare Tunnel or External HTTP(S) Load Balancer |
| Azure | `knowledge-cockpit/deploy/azure/README.md` | Azure Linux VM + Nginx + systemd, optionally behind Cloudflare Tunnel, Application Gateway or Front Door |

All three cloud runbooks reuse shared Linux templates:

```text
knowledge-cockpit/deploy/linux/knowledge-cockpit.service
knowledge-cockpit/deploy/linux/knowledge-cockpit.env.example
knowledge-cockpit/deploy/linux/nginx-knowledge-cockpit.locations.conf
knowledge-cockpit/deploy/linux/cloudflared-knowledge-cockpit.example.yml
knowledge-cockpit/deploy/linux/install_linux.sh
```

The common production route is:

```text
https://your-own-domain.example/knowledge-cockpit/
  -> cloud DNS / load balancer / Cloudflare Tunnel
  -> local Nginx
  -> knowledge-cockpit systemd service on 127.0.0.1:8088
```

Legacy Alibaba Cloud ECS notes are still available at:

```text
knowledge-cockpit/deploy/alicloud/README.md
```

## Option 3: Local Network Demo

Use this for a room demo before deploying to HTTPS.

1. Start the full server on your laptop:

```powershell
$env:OPENAI_API_KEY="sk-..."
python knowledge-cockpit\server.py --host 0.0.0.0 --port 8088
```

2. Find your laptop LAN IP:

```powershell
ipconfig
```

3. On the big screen:

```text
http://localhost:8088/knowledge-cockpit/
```

4. On the phone, while on the same Wi-Fi:

```text
http://<laptop-lan-ip>:8088/knowledge-cockpit/
```

5. Use the same session id. Phone role = `Presenter`; screen role = `Screen`.

Local LAN HTTP is enough for control testing. PWA install is best tested on HTTPS.

## Option 4: GitHub Pages

This supports static mode only. It will not support AI KB or remote control because there is no backend.

Repository: [Z3934500/pocs](https://github.com/Z3934500/pocs)

1. Commit the `knowledge-cockpit/` folder.
2. In GitHub, open Settings -> Pages.
3. Select the branch that contains the repo.
4. Choose the repository root as the Pages source.
5. Visit:

```text
https://Z3934500.github.io/pocs/knowledge-cockpit/
```

## Private Presenter Notes

The public screen role hides presenter notes and AI KB. Use the phone in `Presenter` mode as your private notes device.

For cloud deployment, set a server-side PIN:

```bash
PRESENTER_PIN=change-me-before-demo
```

When `PRESENTER_PIN` is set, phone control and AI KB require the PIN. The screen can still read public sync state, but it will not show the private notes panel.
## Security Notes

- Do not put `OPENAI_API_KEY` in `data.js`, `app.js` or any browser file.
- Keep the API key in the server environment only.
- The included Python server only serves app assets and safe text/source files from the repo.
- If you expose this publicly, add auth at Nginx or application level before sharing broadly.

## Pre-Demo Checklist

1. Open the cockpit and search for `OLTP`.
2. Click `Demo Script` and step through the walkthrough.
3. Open the phone, set role to `Presenter`, and verify the screen follows.
4. Click `AI KB` and ask `What is the difference between OLTP sharding and OLAP partitioning?`.
5. Keep the app open in a browser tab before the live demo starts.
