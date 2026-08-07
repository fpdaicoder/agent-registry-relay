"""Environment-backed configuration for the standalone stream proxy."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlsplit


def _positive_int(value: str, name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


@dataclass(frozen=True)
class StreamProxyConfig:
    host: str = "127.0.0.1"
    port: int = 8002
    public_ws_base_url: str = "ws://127.0.0.1:8002"
    create_token: str = ""
    max_object_bytes: int = 256 * 1024 * 1024
    max_chunk_bytes: int = 1024 * 1024
    max_sessions: int = 128
    session_ttl_seconds: int = 900
    cleanup_interval_seconds: int = 15
    auth_timeout_seconds: int = 10
    reconnect_grace_seconds: int = 120

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
    ) -> "StreamProxyConfig":
        source = os.environ if env is None else env
        config = cls(
            host=source.get("A2X_STREAM_PROXY_HOST", "127.0.0.1").strip(),
            port=_positive_int(
                source.get("A2X_STREAM_PROXY_PORT", "8002"),
                "A2X_STREAM_PROXY_PORT",
            ),
            public_ws_base_url=source.get(
                "A2X_STREAM_PROXY_PUBLIC_WS_BASE_URL",
                "ws://127.0.0.1:8002",
            ).rstrip("/"),
            create_token=source.get("A2X_STREAM_PROXY_CREATE_TOKEN", ""),
            max_object_bytes=_positive_int(
                source.get(
                    "A2X_STREAM_PROXY_MAX_OBJECT_BYTES",
                    str(256 * 1024 * 1024),
                ),
                "A2X_STREAM_PROXY_MAX_OBJECT_BYTES",
            ),
            max_chunk_bytes=_positive_int(
                source.get(
                    "A2X_STREAM_PROXY_MAX_CHUNK_BYTES",
                    str(1024 * 1024),
                ),
                "A2X_STREAM_PROXY_MAX_CHUNK_BYTES",
            ),
            max_sessions=_positive_int(
                source.get("A2X_STREAM_PROXY_MAX_SESSIONS", "128"),
                "A2X_STREAM_PROXY_MAX_SESSIONS",
            ),
            session_ttl_seconds=_positive_int(
                source.get("A2X_STREAM_PROXY_SESSION_TTL_SECONDS", "900"),
                "A2X_STREAM_PROXY_SESSION_TTL_SECONDS",
            ),
            cleanup_interval_seconds=_positive_int(
                source.get("A2X_STREAM_PROXY_CLEANUP_INTERVAL_SECONDS", "15"),
                "A2X_STREAM_PROXY_CLEANUP_INTERVAL_SECONDS",
            ),
            auth_timeout_seconds=_positive_int(
                source.get("A2X_STREAM_PROXY_AUTH_TIMEOUT_SECONDS", "10"),
                "A2X_STREAM_PROXY_AUTH_TIMEOUT_SECONDS",
            ),
            reconnect_grace_seconds=_positive_int(
                source.get("A2X_STREAM_PROXY_RECONNECT_GRACE_SECONDS", "120"),
                "A2X_STREAM_PROXY_RECONNECT_GRACE_SECONDS",
            ),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.host:
            raise ValueError("A2X_STREAM_PROXY_HOST must not be empty")
        if not 1 <= self.port <= 65535:
            raise ValueError("A2X_STREAM_PROXY_PORT must be between 1 and 65535")
        parsed = urlsplit(self.public_ws_base_url)
        if (
            parsed.scheme not in {"ws", "wss"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or (parsed.path and not parsed.path.startswith("/"))
            or "//" in parsed.path
            or any(part in {".", ".."} for part in parsed.path.split("/"))
        ):
            raise ValueError(
                "A2X_STREAM_PROXY_PUBLIC_WS_BASE_URL must be a safe WS(S) base URL"
            )
        if len(self.create_token) < 32:
            raise ValueError(
                "A2X_STREAM_PROXY_CREATE_TOKEN must contain at least 32 characters"
            )
        if self.max_chunk_bytes > self.max_object_bytes:
            raise ValueError(
                "A2X_STREAM_PROXY_MAX_CHUNK_BYTES must not exceed the object limit"
            )
        if self.reconnect_grace_seconds > self.session_ttl_seconds:
            raise ValueError(
                "A2X_STREAM_PROXY_RECONNECT_GRACE_SECONDS must not exceed session TTL"
            )
