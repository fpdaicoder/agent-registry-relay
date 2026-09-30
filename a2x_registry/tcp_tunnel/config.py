"""Environment-backed reverse TCP tunnel configuration."""

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
class TcpTunnelConfig:
    enabled: bool = False
    auto_bind_registered_services: bool = False
    host: str = "0.0.0.0"
    port: int = 8003
    proxy_host: str = "0.0.0.0"
    port_range_min: int = 10000
    port_range_max: int = 11000
    register_timeout_seconds: float = 10.0
    open_timeout_seconds: float = 15.0
    idle_timeout_seconds: float = 300.0
    buffer_bytes: int = 65536
    max_line_bytes: int = 1024 * 1024
    max_devices: int = 1000
    max_connections: int = 10000
    max_targets_per_device: int = 32
    shared_token: str = ""

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "TcpTunnelConfig":
        source = os.environ if env is None else env
        config = cls(
            enabled=_as_bool(
                source.get("A2X_TCP_TUNNEL_ENABLED", "false"),
                "A2X_TCP_TUNNEL_ENABLED",
            ),
            auto_bind_registered_services=_as_bool(
                source.get(
                    "A2X_TCP_TUNNEL_AUTO_BIND_REGISTERED_SERVICES",
                    "false",
                ),
                "A2X_TCP_TUNNEL_AUTO_BIND_REGISTERED_SERVICES",
            ),
            host=source.get("A2X_TCP_TUNNEL_HOST", "0.0.0.0").strip(),
            port=_positive_int(source.get("A2X_TCP_TUNNEL_PORT", "8003"), "A2X_TCP_TUNNEL_PORT"),
            proxy_host=source.get("A2X_TCP_TUNNEL_PROXY_HOST", "0.0.0.0").strip(),
            port_range_min=_positive_int(
                source.get("A2X_TCP_TUNNEL_PORT_RANGE_MIN", "10000"),
                "A2X_TCP_TUNNEL_PORT_RANGE_MIN",
            ),
            port_range_max=_positive_int(
                source.get("A2X_TCP_TUNNEL_PORT_RANGE_MAX", "11000"),
                "A2X_TCP_TUNNEL_PORT_RANGE_MAX",
            ),
            register_timeout_seconds=_positive_float(
                source.get("A2X_TCP_TUNNEL_REGISTER_TIMEOUT_SECONDS", "10"),
                "A2X_TCP_TUNNEL_REGISTER_TIMEOUT_SECONDS",
            ),
            open_timeout_seconds=_positive_float(
                source.get("A2X_TCP_TUNNEL_OPEN_TIMEOUT_SECONDS", "15"),
                "A2X_TCP_TUNNEL_OPEN_TIMEOUT_SECONDS",
            ),
            idle_timeout_seconds=_positive_float(
                source.get("A2X_TCP_TUNNEL_IDLE_TIMEOUT_SECONDS", "300"),
                "A2X_TCP_TUNNEL_IDLE_TIMEOUT_SECONDS",
            ),
            buffer_bytes=_positive_int(
                source.get("A2X_TCP_TUNNEL_BUFFER_BYTES", "65536"),
                "A2X_TCP_TUNNEL_BUFFER_BYTES",
            ),
            max_line_bytes=_positive_int(
                source.get("A2X_TCP_TUNNEL_MAX_LINE_BYTES", str(1024 * 1024)),
                "A2X_TCP_TUNNEL_MAX_LINE_BYTES",
            ),
            max_devices=_positive_int(
                source.get("A2X_TCP_TUNNEL_MAX_DEVICES", "1000"),
                "A2X_TCP_TUNNEL_MAX_DEVICES",
            ),
            max_connections=_positive_int(
                source.get("A2X_TCP_TUNNEL_MAX_CONNECTIONS", "10000"),
                "A2X_TCP_TUNNEL_MAX_CONNECTIONS",
            ),
            max_targets_per_device=_positive_int(
                source.get("A2X_TCP_TUNNEL_MAX_TARGETS_PER_DEVICE", "32"),
                "A2X_TCP_TUNNEL_MAX_TARGETS_PER_DEVICE",
            ),
            shared_token=source.get("A2X_TCP_TUNNEL_SHARED_TOKEN", ""),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.host:
            raise ValueError("A2X_TCP_TUNNEL_HOST must not be empty")
        if not self.proxy_host:
            raise ValueError("A2X_TCP_TUNNEL_PROXY_HOST must not be empty")
        if not 1 <= self.port <= 65535:
            raise ValueError("A2X_TCP_TUNNEL_PORT must be between 1 and 65535")
        if self.port_range_min > self.port_range_max:
            raise ValueError(
                "A2X_TCP_TUNNEL_PORT_RANGE_MIN must not exceed "
                "A2X_TCP_TUNNEL_PORT_RANGE_MAX"
            )
        if self.port_range_min <= self.port <= self.port_range_max:
            raise ValueError(
                "A2X_TCP_TUNNEL_PORT must stay outside the proxy port range"
            )
