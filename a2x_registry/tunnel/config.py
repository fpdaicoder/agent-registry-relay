"""Environment-backed WebSocket tunnel configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


def _as_bool(value: str, name: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false, got {value!r}")


def _positive_int(value: str, name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


def _positive_float(value: str, name: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


@dataclass(frozen=True)
class TunnelConfig:
    enabled: bool = False
    auto_bind_registered_services: bool = False
    host: str = "0.0.0.0"
    port: int = 8001
    register_timeout_seconds: float = 10.0
    request_timeout_seconds: float = 300.0
    max_request_timeout_seconds: float = 300.0
    heartbeat_interval_seconds: float = 15.0
    device_timeout_seconds: float = 60.0
    max_message_bytes: int = 50 * 1024 * 1024
    max_devices: int = 1000
    max_pending_requests: int = 10000
    shared_token: str = ""

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "TunnelConfig":
        source = os.environ if env is None else env
        config = cls(
            enabled=_as_bool(
                source.get("A2X_TUNNEL_ENABLED", "false"),
                "A2X_TUNNEL_ENABLED",
            ),
            auto_bind_registered_services=_as_bool(
                source.get(
                    "A2X_TUNNEL_AUTO_BIND_REGISTERED_SERVICES",
                    "false",
                ),
                "A2X_TUNNEL_AUTO_BIND_REGISTERED_SERVICES",
            ),
            host=source.get("A2X_TUNNEL_HOST", "0.0.0.0").strip(),
            port=_positive_int(source.get("A2X_TUNNEL_PORT", "8001"), "A2X_TUNNEL_PORT"),
            register_timeout_seconds=_positive_float(
                source.get("A2X_TUNNEL_REGISTER_TIMEOUT_SECONDS", "10"),
                "A2X_TUNNEL_REGISTER_TIMEOUT_SECONDS",
            ),
            request_timeout_seconds=_positive_float(
                source.get("A2X_TUNNEL_REQUEST_TIMEOUT_SECONDS", "300"),
                "A2X_TUNNEL_REQUEST_TIMEOUT_SECONDS",
            ),
            max_request_timeout_seconds=_positive_float(
                source.get("A2X_TUNNEL_MAX_REQUEST_TIMEOUT_SECONDS", "300"),
                "A2X_TUNNEL_MAX_REQUEST_TIMEOUT_SECONDS",
            ),
            heartbeat_interval_seconds=_positive_float(
                source.get("A2X_TUNNEL_HEARTBEAT_INTERVAL_SECONDS", "15"),
                "A2X_TUNNEL_HEARTBEAT_INTERVAL_SECONDS",
            ),
            device_timeout_seconds=_positive_float(
                source.get("A2X_TUNNEL_DEVICE_TIMEOUT_SECONDS", "60"),
                "A2X_TUNNEL_DEVICE_TIMEOUT_SECONDS",
            ),
            max_message_bytes=_positive_int(
                source.get("A2X_TUNNEL_MAX_MESSAGE_BYTES", str(50 * 1024 * 1024)),
                "A2X_TUNNEL_MAX_MESSAGE_BYTES",
            ),
            max_devices=_positive_int(
                source.get("A2X_TUNNEL_MAX_DEVICES", "1000"),
                "A2X_TUNNEL_MAX_DEVICES",
            ),
            max_pending_requests=_positive_int(
                source.get("A2X_TUNNEL_MAX_PENDING_REQUESTS", "10000"),
                "A2X_TUNNEL_MAX_PENDING_REQUESTS",
            ),
            shared_token=source.get("A2X_TUNNEL_SHARED_TOKEN", ""),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.host:
            raise ValueError("A2X_TUNNEL_HOST must not be empty")
        if not 1 <= self.port <= 65535:
            raise ValueError("A2X_TUNNEL_PORT must be between 1 and 65535")
        if self.request_timeout_seconds > self.max_request_timeout_seconds:
            raise ValueError(
                "A2X_TUNNEL_REQUEST_TIMEOUT_SECONDS must not exceed "
                "A2X_TUNNEL_MAX_REQUEST_TIMEOUT_SECONDS"
            )
