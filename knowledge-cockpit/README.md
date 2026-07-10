# PoC Knowledge Cockpit

Presenter cockpit and lightweight knowledge-base app for the data-engineering PoC repository.

For the standalone enterprise knowledge-base automation PoC, see [Z3934500/KB](https://github.com/Z3934500/KB).

It can run in two modes:

- Static mode: Story Map, terminology cards, demo script, Q&A, concept graph and presenter notes.
- Full mode: everything in static mode, plus phone remote control, long-press voice input and AI knowledge-base answers backed by the repo docs.

## What It Contains

- Story Map: the main presentation structure, shown as `2 Original + 2 Refactor + 5 Extension`.
- Explore: searchable terminology cards with repo references and talk tracks.
- Demo Script: a controlled walkthrough for live presentations.
- Q&A: prepared answers for common architecture questions.
- Graph: a compact relationship map across OLTP, OLAP, governance, real-time and operations.
- AI KB: asks a local retrieval layer over repo docs, then calls the OpenAI Responses API from the server. On mobile, hold `Voice` to dictate the private question.
- Remote Session: lets a phone control the screen view by sharing the same session id.
- PWA metadata: installable on mobile when deployed over HTTPS.

## Static Local Run

This is enough for local demo without AI or phone sync.

If your current directory is the repository root, `pocs`, run:

```powershell
python -m http.server 8088
```

Then open:

```text
http://localhost:8088/knowledge-cockpit/
```

If your current directory is already `pocs\knowledge-cockpit`, run the same command but open the server root:

```powershell
python -m http.server 8088
```

```text
http://localhost:8088/
```

## Full Local Run: Remote + AI KB

Set your OpenAI key in the shell that starts the server:

```powershell
$env:OPENAI_API_KEY="sk-..."
$env:OPENAI_MODEL="gpt-4.1-mini"
python knowledge-cockpit\server.py --host 0.0.0.0 --port 8088
```

Open the screen view on your laptop:

```text
http://localhost:8088/knowledge-cockpit/
```

Open the remote view on your phone using the laptop LAN IP:

```text
http://<laptop-lan-ip>:8088/knowledge-cockpit/
```

Use the same `Remote Session` value on both devices, for example `demo`. Keep the laptop on `Screen`; set the phone to `Presenter`. Phone clicks will update the screen through the local server.


## Private Presenter Notes

The public screen role hides presenter notes and AI KB. Use the phone in `Presenter` mode as your private notes device.

For cloud deployment, set a server-side PIN:

```bash
PRESENTER_PIN=change-me-before-demo
```

When `PRESENTER_PIN` is set, phone control and AI KB require the PIN. The screen can still read public sync state, but it will not show the private notes panel.
## AI KB Notes

The browser never receives the OpenAI API key. The frontend calls:

```text
POST /api/chat
```

The Python server then:

1. Reads selected repo Markdown files.
2. Splits them into searchable chunks.
3. Retrieves the most relevant chunks for the question.
4. Sends only that context and the question to OpenAI.
5. Returns the answer plus source files/headings.

The default model can be changed with `OPENAI_MODEL`.

## Install On Phone

For a real phone install experience, deploy it under HTTPS, for example:

```text
https://your-own-domain.example/knowledge-cockpit/
```

Then:

- iPhone Safari: Share -> Add to Home Screen.
- Android Chrome: menu -> Install app or Add to Home screen.

Local HTTP over LAN is fine for remote-control testing, but mobile PWA installation is most reliable over HTTPS.

## Cloud Deployment Runbooks

For full remote-control + AI KB mode on a cloud Linux VM, use one of the cloud-specific runbooks:

| Cloud | Runbook | Shape |
| --- | --- | --- |
| AWS | `deploy/aws/README.md` | EC2 + Nginx + systemd, with ALB/CloudFront or Cloudflare Tunnel |
| GCP | `deploy/gcp/README.md` | Compute Engine + Nginx + systemd, with External HTTP(S) Load Balancer or Cloudflare Tunnel |
| Azure | `deploy/azure/README.md` | Azure Linux VM + Nginx + systemd, with Application Gateway/Front Door or Cloudflare Tunnel |

Shared Linux service templates live in:

```text
deploy/linux/
```

The legacy Alibaba Cloud ECS runbook remains available at:

```text
deploy/alicloud/README.md
```
## Deploy

See `DEPLOYMENT.md` for step-by-step deployment options.