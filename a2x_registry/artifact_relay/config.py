"""Environment-backed configuration for the artifact relay."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit


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


@dataclass(frozen=True)
class ArtifactRelayConfig:
    enabled: bool = False
    public_base_url: str = ""
    storage_dir: Path = Path("/var/tmp/a2x-artifact-relay")
    create_token: str = ""
    max_object_bytes: int = 256 * 1024 * 1024
    chunk_bytes: int = 1024 * 1024
    default_ttl_seconds: int = 3600
    max_ttl_seconds: int = 24 * 3600
    cleanup_interval_seconds: int = 60

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
    ) -> "ArtifactRelayConfig":
        source = os.environ if env is None else env
        config = cls(
            enabled=_as_bool(
                source.get("A2X_ARTIFACT_RELAY_ENABLED", "false"),
                "A2X_ARTIFACT_RELAY_ENABLED",
            ),
            public_base_url=source.get(
                "A2X_ARTIFACT_RELAY_PUBLIC_BASE_URL",
                "",
            ).rstrip("/"),
            storage_dir=Path(
                source.get(
                    "A2X_ARTIFACT_RELAY_STORAGE_DIR",
                    "/var/tmp/a2x-artifact-relay",
                )
            ),
            create_token=source.get(
                "A2X_ARTIFACT_RELAY_CREATE_TOKEN",
                "",
            ),
            max_object_bytes=_positive_int(
                source.get(
                    "A2X_ARTIFACT_RELAY_MAX_OBJECT_BYTES",
                    str(256 * 1024 * 1024),
                ),
                "A2X_ARTIFACT_RELAY_MAX_OBJECT_BYTES",
            ),
            chunk_bytes=_positive_int(
                source.get(
                    "A2X_ARTIFACT_RELAY_CHUNK_BYTES",
                    str(1024 * 1024),
                ),
                "A2X_ARTIFACT_RELAY_CHUNK_BYTES",
            ),
            default_ttl_seconds=_positive_int(
                source.get("A2X_ARTIFACT_RELAY_DEFAULT_TTL_SECONDS", "3600"),
                "A2X_ARTIFACT_RELAY_DEFAULT_TTL_SECONDS",
            ),
            max_ttl_seconds=_positive_int(
                source.get("A2X_ARTIFACT_RELAY_MAX_TTL_SECONDS", str(24 * 3600)),
                "A2X_ARTIFACT_RELAY_MAX_TTL_SECONDS",
            ),
            cleanup_interval_seconds=_positive_int(
                source.get("A2X_ARTIFACT_RELAY_CLEANUP_INTERVAL_SECONDS", "60"),
                "A2X_ARTIFACT_RELAY_CLEANUP_INTERVAL_SECONDS",
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
                "A2X_ARTIFACT_RELAY_PUBLIC_BASE_URL must be an absolute "
                "HTTP(S) URL when artifact relay is enabled"
            )
        if (
            parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError(
                "A2X_ARTIFACT_RELAY_PUBLIC_BASE_URL must be an origin "
                "without credentials, path, query, or fragment"
            )
        # Deployment templates target Linux; accept POSIX-shaped absolute
        # paths so config validation also works on Windows dev machines.
        # Windows Path() normalizes a leading "/" to "\", so check the
        # drive-aware parts rather than the string form.
        if not (
            self.storage_dir.is_absolute()
            or (not self.storage_dir.drive and self.storage_dir.parts[:1] == ("/",))
            or (not self.storage_dir.drive and self.storage_dir.parts[:1] == ("\\",))
        ):
            raise ValueError("A2X_ARTIFACT_RELAY_STORAGE_DIR must be absolute")
        if len(self.create_token) < 32:
            raise ValueError(
                "A2X_ARTIFACT_RELAY_CREATE_TOKEN must contain at least "
                "32 characters when artifact relay is enabled"
            )
        if self.chunk_bytes > self.max_object_bytes:
            raise ValueError(
                "A2X_ARTIFACT_RELAY_CHUNK_BYTES must not exceed "
                "A2X_ARTIFACT_RELAY_MAX_OBJECT_BYTES"
            )
        if self.default_ttl_seconds > self.max_ttl_seconds:
            raise ValueError(
                "A2X_ARTIFACT_RELAY_DEFAULT_TTL_SECONDS must not exceed "
                "A2X_ARTIFACT_RELAY_MAX_TTL_SECONDS"
            )
