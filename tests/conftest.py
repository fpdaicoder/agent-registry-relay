"""Shared fixtures for registry and relay tests."""

from __future__ import annotations

import os
import sys
import uuid

import pytest


# ── install-mode fixtures ────────────────────────────────────────────────────


@pytest.fixture
def lite_app(tmp_path, monkeypatch):
    """Boot the registry API with an isolated runtime directory."""
    monkeypatch.setenv("A2X_REGISTRY_HOME", str(tmp_path))

    for n in list(sys.modules):
        if n.startswith("a2x_registry"):
            monkeypatch.delitem(sys.modules, n, raising=False)

    from a2x_registry.backend.app import app
    from a2x_registry.backend.startup import shutdown_registry, startup_registry

    startup_registry()

    from fastapi.testclient import TestClient
    yield TestClient(app)
    shutdown_registry()


# ── data helpers ─────────────────────────────────────────────────────────────


def make_agent_card(name: str = "agent-1") -> dict:
    """Build a minimal but valid A2A Agent Card for tests."""
    return {
        "name": name,
        "description": "tester",
        "url": "http://example.invalid",
        "version": "1.0",
        "protocolVersion": "0.0.1",
        "capabilities": {},
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "skills": [
            {"id": "s", "name": "s", "description": "s", "tags": ["t"]}
        ],
    }


@pytest.fixture
def agent_card():
    """Factory for building Agent Card dicts inside tests."""
    return make_agent_card


@pytest.fixture
def dataset(lite_app):
    """A fresh dataset per test, auto-deleted on teardown."""
    name = "ds_" + uuid.uuid4().hex[:8]
    r = lite_app.post("/api/datasets", json={"name": name})
    assert r.status_code == 200, r.text
    yield name
    r = lite_app.delete(f"/api/datasets/{name}")
    assert r.status_code == 200, r.text


# ── extra summary line ───────────────────────────────────────────────────────


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_sessionfinish(session, exitstatus):
    """Print a final ``N tests, N passed`` separator after pytest's summary.

    Uses ``tryfirst=True`` so this wrapper is outermost — its post-yield
    code runs AFTER pytest's built-in summary_stats() line, making this
    line the actual last line of pytest output (good for screenshots).
    """
    yield
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is None:
        return
    stats = reporter.stats
    passed = len(stats.get("passed", []))
    failed = len(stats.get("failed", []))
    skipped = len(stats.get("skipped", []))
    errors = len(stats.get("error", []))
    total = passed + failed + skipped + errors
    reporter.write_sep("=", f"{total} tests, {passed} passed", bold=True)
