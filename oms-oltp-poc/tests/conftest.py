from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture()
def tmp_path(request: pytest.FixtureRequest) -> Path:
    safe_name = request.node.name.replace("[", "_").replace("]", "_").replace(":", "_")
    path = Path(__file__).resolve().parents[1] / "data" / "test-runtime" / safe_name
    path.mkdir(parents=True, exist_ok=True)
    return path
