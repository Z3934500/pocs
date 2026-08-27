"""Configuration layer — where deployment facts enter the process.

Layer 0: reads environment variables and resolves paths, and imports nothing
else from `cce_platform`. Every other layer receives its file locations and
runtime flags from here rather than reading `os.environ` itself, so the set of
knobs a deployment has is enumerable in one dataclass.

One honest exception to "pure configuration": `require_redis` defaults from
`runtime_env in {"staging", "production"}`. That is a deployment safety policy,
not a value read from the environment. It lives here because the alternative is
every caller re-deriving it, and a caller that forgets would silently degrade to
a per-process store where replicas diverge.
"""

from __future__ import annotations

from .config import Settings, load_settings, settings

__all__ = ["Settings", "load_settings", "settings"]
