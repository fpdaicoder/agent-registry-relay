"""Regression coverage for optional data-plane lifecycle ordering."""

from __future__ import annotations

from fastapi.testclient import TestClient


def test_app_metadata_uses_package_version() -> None:
    from a2x_registry import __version__
    from a2x_registry.backend.app import app

    assert app.title == "Agent Registry Relay"
    assert app.version == __version__


def test_optional_services_start_and_stop_in_dependency_order(monkeypatch) -> None:
    from a2x_registry.backend import app as app_module

    events: list[str] = []

    monkeypatch.setattr(
        app_module,
        "startup_registry",
        lambda: events.append("start-registry"),
    )
    monkeypatch.setattr(
        app_module,
        "shutdown_registry",
        lambda: events.append("stop-registry"),
    )

    def async_event(name: str):
        async def record() -> None:
            events.append(name)

        return record

    monkeypatch.setattr(app_module, "startup_relay", async_event("start-relay"))
    monkeypatch.setattr(app_module, "startup_tunnel", async_event("start-tunnel"))
    monkeypatch.setattr(
        app_module,
        "startup_artifact_relay",
        async_event("start-artifact-relay"),
    )
    monkeypatch.setattr(
        app_module,
        "shutdown_artifact_relay",
        async_event("stop-artifact-relay"),
    )
    monkeypatch.setattr(app_module, "shutdown_tunnel", async_event("stop-tunnel"))
    monkeypatch.setattr(app_module, "shutdown_relay", async_event("stop-relay"))
    with TestClient(app_module.app):
        pass

    assert events == [
        "start-registry",
        "start-relay",
        "start-tunnel",
        "start-artifact-relay",
        "stop-artifact-relay",
        "stop-tunnel",
        "stop-relay",
        "stop-registry",
    ]
