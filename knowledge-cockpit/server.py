"""Local presenter server for Knowledge Cockpit.

Features:
- Serves the static cockpit app.
- Shares remote-control state across devices by session id.
- Answers questions with a small repo-local retrieval layer plus the OpenAI Responses API.

Keep OPENAI_API_KEY on the server side. Do not put it in browser code.
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


APP_ROOT = Path(__file__).resolve().parent
REPO_ROOT = APP_ROOT.parent

SAFE_REPO_EXTENSIONS = {".md", ".py", ".json", ".yaml", ".yml", ".tf", ".txt", ".toml"}

DOC_PATHS = [
    "README.md",
    "inventory-oms-poc/README.md",
    "oms-oltp-poc/README.md",
    "oms-oltp-poc/docs/OLTP_OLAP_HTAP_MAPPING.md",
    "data-governance-poc/README.md",
    "cce-feature-platform/README.md",
    "cce-feature-platform/docs/REALTIME_FEATURE_PLATFORM_480K.md",
    "cce-feature-platform/docs/BIG_DATA_EMR_DELTA_EXTENSION.md",
    "cce-feature-platform/docs/OPERATIONS_MATURITY_AND_COST.md",
    "oee-data-platform/README.md",
    "CICD_AND_DEPLOYMENT.md",
]


@dataclass(frozen=True)
class Chunk:
    path: str
    heading: str
    text: str


SESSIONS: dict[str, dict[str, Any]] = {}
CHUNKS: list[Chunk] = []


def load_chunks() -> list[Chunk]:
    chunks: list[Chunk] = []
    for relative in DOC_PATHS:
        path = REPO_ROOT / relative
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        sections = split_markdown_sections(text)
        for heading, section_text in sections:
            clean = normalize_text(section_text)
            if clean:
                chunks.append(Chunk(relative, heading, clean[:6000]))
    return chunks


def split_markdown_sections(text: str) -> list[tuple[str, str]]:
    sections: list[tuple[str, str]] = []
    current_heading = "Overview"
    current_lines: list[str] = []

    for line in text.splitlines():
        match = re.match(r"^(#{1,4})\s+(.+)$", line)
        if match and current_lines:
            sections.append((current_heading, "\n".join(current_lines)))
            current_heading = match.group(2).strip()
            current_lines = [line]
            continue
        if match:
            current_heading = match.group(2).strip()
        current_lines.append(line)

    if current_lines:
        sections.append((current_heading, "\n".join(current_lines)))
    return sections


def normalize_text(text: str) -> str:
    text = re.sub(r"```.*?```", " ", text, flags=re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"[#*_`>|-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def tokenize(text: str) -> set[str]:
    return {token.lower() for token in re.findall(r"[A-Za-z0-9_./+-]{2,}", text)}


def retrieve(question: str, limit: int = 5) -> list[Chunk]:
    query_tokens = tokenize(question)
    if not query_tokens:
        return CHUNKS[:limit]

    scored: list[tuple[float, Chunk]] = []
    for chunk in CHUNKS:
        chunk_tokens = tokenize(f"{chunk.heading} {chunk.path} {chunk.text}")
        overlap = query_tokens & chunk_tokens
        if not overlap:
            continue
        score = len(overlap) * 4
        score += sum(1 for token in overlap if token in chunk.heading.lower())
        score += min(len(chunk.text) / 1200, 3)
        scored.append((score, chunk))

    scored.sort(key=lambda item: item[0], reverse=True)
    return [chunk for _, chunk in scored[:limit]] or CHUNKS[:limit]


def build_prompt(question: str, chunks: list[Chunk]) -> str:
    context = "\n\n".join(
        f"Source: {chunk.path} | {chunk.heading}\n{chunk.text}" for chunk in chunks
    )
    return f"""You are a concise data-engineering demo assistant for this PoC repository.

Answer only from the provided repository context. If the context is insufficient, say what is missing.
Use practical architecture language. Prefer a short answer first, then bullets if useful.

Repository context:
{context}

Question:
{question}
"""


def call_openai(prompt: str) -> str:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured on the server.")

    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    url = os.environ.get("OPENAI_RESPONSES_URL", "https://api.openai.com/v1/responses")
    payload = {
        "model": model,
        "input": prompt,
        "max_output_tokens": 900,
    }

    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"OpenAI API error {exc.code}: {detail[:600]}") from exc

    return extract_response_text(json.loads(body))


def extract_response_text(response: dict[str, Any]) -> str:
    if isinstance(response.get("output_text"), str):
        return response["output_text"].strip()

    parts: list[str] = []
    for item in response.get("output", []):
        for content in item.get("content", []):
            if isinstance(content, dict):
                text = content.get("text")
                if isinstance(text, str):
                    parts.append(text)
    return "\n".join(parts).strip() or "No text response was returned."




def presenter_pin_required() -> bool:
    return bool(os.environ.get("PRESENTER_PIN", "").strip())


def presenter_pin_valid(payload: dict[str, Any]) -> bool:
    expected = os.environ.get("PRESENTER_PIN", "").strip()
    if not expected:
        return True
    return str(payload.get("pin") or "") == expected
def normalize_api_path(path: str) -> str:
    if path.startswith("/knowledge-cockpit/api/"):
        return path.removeprefix("/knowledge-cockpit")
    return path


class CockpitHandler(BaseHTTPRequestHandler):
    server_version = "KnowledgeCockpit/1.0"

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        api_path = normalize_api_path(parsed.path)
        if api_path == "/api/health":
            self.send_json(
                {
                    "ok": True,
                    "chunks": len(CHUNKS),
                    "openai_configured": bool(os.environ.get("OPENAI_API_KEY")),
                    "presenter_pin_required": presenter_pin_required(),
                    "model": os.environ.get("OPENAI_MODEL", "gpt-4.1-mini"),
                }
            )
            return
        if api_path == "/api/state":
            params = urllib.parse.parse_qs(parsed.query)
            session = params.get("session", ["demo"])[0] or "demo"
            self.send_json(get_session(session))
            return
        self.serve_static(parsed.path)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        api_path = normalize_api_path(parsed.path)
        if api_path == "/api/state":
            payload = self.read_json()
            if not presenter_pin_valid(payload):
                self.send_json({"error": "invalid presenter PIN"}, HTTPStatus.FORBIDDEN)
                return
            session = str(payload.get("session") or "demo")
            incoming_state = payload.get("state") or {}
            if not isinstance(incoming_state, dict):
                self.send_json({"error": "state must be an object"}, HTTPStatus.BAD_REQUEST)
                return
            current = get_session(session)
            current["revision"] += 1
            current["updated_at"] = time.time()
            current["state"].update(
                {
                    key: incoming_state[key]
                    for key in [
                        "view",
                        "selectedTermId",
                        "selectedStepId",
                        "category",
                        "query",
                    ]
                    if key in incoming_state
                }
            )
            SESSIONS[session] = current
            self.send_json(current)
            return

        if api_path == "/api/presenter-auth":
            payload = self.read_json()
            if not presenter_pin_valid(payload):
                self.send_json({"ok": False, "error": "invalid presenter PIN"}, HTTPStatus.FORBIDDEN)
                return
            self.send_json({"ok": True})
            return
        if api_path == "/api/chat":
            payload = self.read_json()
            if not presenter_pin_valid(payload):
                self.send_json({"error": "invalid presenter PIN"}, HTTPStatus.FORBIDDEN)
                return
            question = str(payload.get("question") or "").strip()
            if not question:
                self.send_json({"error": "question is required"}, HTTPStatus.BAD_REQUEST)
                return
            chunks = retrieve(question, limit=int(payload.get("top_k") or 5))
            try:
                answer = call_openai(build_prompt(question, chunks))
                self.send_json(
                    {
                        "answer": answer,
                        "sources": [
                            {"path": chunk.path, "heading": chunk.heading} for chunk in chunks
                        ],
                    }
                )
            except Exception as exc:  # noqa: BLE001 - return useful demo diagnostics
                self.send_json(
                    {
                        "error": str(exc),
                        "sources": [
                            {"path": chunk.path, "heading": chunk.heading} for chunk in chunks
                        ],
                    },
                    HTTPStatus.BAD_GATEWAY,
                )
            return

        self.send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)

    def read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}

    def serve_static(self, request_path: str) -> None:
        if request_path in {"", "/"}:
            file_path = APP_ROOT / "index.html"
        elif request_path.startswith("/knowledge-cockpit/"):
            file_path = APP_ROOT / urllib.parse.unquote(request_path.removeprefix("/knowledge-cockpit/"))
        else:
            file_path = REPO_ROOT / urllib.parse.unquote(request_path.lstrip("/"))

        try:
            resolved = file_path.resolve()
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        if resolved.is_dir():
            resolved = resolved / "index.html"

        app_root = APP_ROOT.resolve()
        repo_root = REPO_ROOT.resolve()
        is_app_asset = resolved == app_root or app_root in resolved.parents
        is_safe_repo_file = repo_root in resolved.parents and resolved.suffix in SAFE_REPO_EXTENSIONS
        if not (is_app_asset or is_safe_repo_file):
            self.send_error(HTTPStatus.FORBIDDEN)
            return

        if not resolved.exists() or not resolved.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        content_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
        content = resolved.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-cache" if resolved.suffix in {".html", ".js"} else "public, max-age=3600")
        self.end_headers()
        self.wfile.write(content)

    def send_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)


def get_session(session: str) -> dict[str, Any]:
    if session not in SESSIONS:
        SESSIONS[session] = {
            "session": session,
            "revision": 0,
            "updated_at": time.time(),
            "state": {
                "view": "explore",
                "selectedTermId": "oltp",
                "selectedStepId": "overview",
                "category": "All",
                "query": "",
            },
        }
    return SESSIONS[session]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Knowledge Cockpit presenter server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8088, type=int)
    args = parser.parse_args()

    global CHUNKS
    CHUNKS = load_chunks()

    server = ThreadingHTTPServer((args.host, args.port), CockpitHandler)
    print(f"Knowledge Cockpit: http://{args.host}:{args.port}/knowledge-cockpit/")
    print(f"RAG chunks loaded: {len(CHUNKS)}")
    print("Set OPENAI_API_KEY before using the AI KB tab.")
    server.serve_forever()


if __name__ == "__main__":
    main()
