"""Environment-backed relay configuration."""

from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlsplit


def _as_bool(value: str, name: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false, got {value!r}")


def _csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def normalize_origin(value: str) -> str:
    parsed = urlsplit(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"Invalid HTTP origin: {value!r}")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError(f"Origin must not contain credentials, query, or fragment: {value!r}")
    if parsed.path not in {"", "/"}:
        raise ValueError(f"Origin must not contain a path: {value!r}")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    return f"{parsed.scheme}://{host}:{port}"


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
class RelayConfig:
    enabled: bool = False
    public_base_url: str = ""
    allow_all_targets: bool = False
    allowed_origins: frozenset[str] = frozenset()
    allowed_cidrs: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = ()
    allowed_ports: frozenset[int] = frozenset()
    max_body_bytes: int = 8 * 1024 * 1024
    max_response_bytes: int = 16 * 1024 * 1024
    connect_timeout_seconds: float = 3.0
    read_timeout_seconds: float = 300.0
    max_inflight: int = 100
    forward_authorization: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "RelayConfig":
        source = os.environ if env is None else env
        enabled = _as_bool(source.get("A2X_RELAY_ENABLED", "false"), "A2X_RELAY_ENABLED")
        public_base_url = source.get("A2X_RELAY_PUBLIC_BASE_URL", "").rstrip("/")
        origins = frozenset(
            normalize_origin(value)
            for value in _csv(source.get("A2X_RELAY_ALLOWED_ORIGINS", ""))
        )
        cidrs = tuple(
            ipaddress.ip_network(value, strict=False)
            for value in _csv(source.get("A2X_RELAY_ALLOWED_CIDRS", ""))
        )
        ports = frozenset(
            _positive_int(value, "A2X_RELAY_ALLOWED_PORTS")
            for value in _csv(source.get("A2X_RELAY_ALLOWED_PORTS", ""))
        )
        config = cls(
            enabled=enabled,
            public_base_url=public_base_url,
            allow_all_targets=_as_bool(
                source.get("A2X_RELAY_ALLOW_ALL_TARGETS", "false"),
                "A2X_RELAY_ALLOW_ALL_TARGETS",
            ),
            allowed_origins=origins,
            allowed_cidrs=cidrs,
            allowed_ports=ports,
            max_body_bytes=_positive_int(
                source.get("A2X_RELAY_MAX_BODY_BYTES", str(8 * 1024 * 1024)),
                "A2X_RELAY_MAX_BODY_BYTES",
            ),
            max_response_bytes=_positive_int(
                source.get("A2X_RELAY_MAX_RESPONSE_BYTES", str(16 * 1024 * 1024)),
                "A2X_RELAY_MAX_RESPONSE_BYTES",
            ),
            connect_timeout_seconds=_positive_float(
                source.get("A2X_RELAY_CONNECT_TIMEOUT_SECONDS", "3"),
                "A2X_RELAY_CONNECT_TIMEOUT_SECONDS",
            ),
            read_timeout_seconds=_positive_float(
                source.get("A2X_RELAY_READ_TIMEOUT_SECONDS", "300"),
                "A2X_RELAY_READ_TIMEOUT_SECONDS",
            ),
            max_inflight=_positive_int(
                source.get("A2X_RELAY_MAX_INFLIGHT", "100"),
                "A2X_RELAY_MAX_INFLIGHT",
            ),
            forward_authorization=_as_bool(
                source.get("A2X_RELAY_FORWARD_AUTHORIZATION", "false"),
                "A2X_RELAY_FORWARD_AUTHORIZATION",
            ),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.enabled:
            return
        parsed = urlsplit(self.public_base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError(
                "A2X_RELAY_PUBLIC_BASE_URL must be an absolute HTTP(S) URL "
                "when relay is enabled"
            )
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError(
                "A2X_RELAY_PUBLIC_BASE_URL must not contain credentials, "
                "query, or fragment"
            )
        if (
            not self.allow_all_targets
            and not self.allowed_origins
            and not self.allowed_cidrs
        ):
            raise ValueError(
                "Relay requires A2X_RELAY_ALLOW_ALL_TARGETS=true, "
                "A2X_RELAY_ALLOWED_ORIGINS, or A2X_RELAY_ALLOWED_CIDRS"
            )
        if (
            not self.allow_all_targets
            and self.allowed_cidrs
            and not self.allowed_ports
        ):
            raise ValueError(
                "A2X_RELAY_ALLOWED_PORTS is required when CIDR targets are enabled"
            )
