"""Environment-backed configuration for the group chat data plane."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


def _positive_int(value: str, name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


def _bool(value: str, name: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError(f"{name} must be a boolean, got {value!r}")


@dataclass(frozen=True)
class GroupChatConfig:
    """Runtime configuration for the optional group chat module.

    ``dsn`` is only consulted by the PostgreSQL backend. The in-memory
    backend ignores it, which keeps the test suite free of a database
    dependency.
    """

    enabled: bool = False
    backend: str = "postgres"
    dsn: str = ""
    schema: str = "public"
    max_message_bytes: int = 64 * 1024
    max_groups_per_principal: int = 200
    max_members_per_group: int = 500
    max_page_size: int = 100
    default_page_size: int = 50
    rate_limit_per_group: int = 600
    rate_limit_window_seconds: int = 60
    create_timeout_seconds: int = 10

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "GroupChatConfig":
        source = os.environ if env is None else env
        config = cls(
            enabled=_bool(
                source.get("A2X_GROUPCHAT_ENABLED", "false"),
                "A2X_GROUPCHAT_ENABLED",
            ),
            backend=source.get("A2X_GROUPCHAT_BACKEND", "postgres").strip().lower(),
            dsn=source.get("A2X_GROUPCHAT_DSN", "").strip(),
            schema=source.get("A2X_GROUPCHAT_SCHEMA", "public").strip(),
            max_message_bytes=_positive_int(
                source.get("A2X_GROUPCHAT_MAX_MESSAGE_BYTES", str(64 * 1024)),
                "A2X_GROUPCHAT_MAX_MESSAGE_BYTES",
            ),
            max_groups_per_principal=_positive_int(
                source.get("A2X_GROUPCHAT_MAX_GROUPS_PER_PRINCIPAL", "200"),
                "A2X_GROUPCHAT_MAX_GROUPS_PER_PRINCIPAL",
            ),
            max_members_per_group=_positive_int(
                source.get("A2X_GROUPCHAT_MAX_MEMBERS_PER_GROUP", "500"),
                "A2X_GROUPCHAT_MAX_MEMBERS_PER_GROUP",
            ),
            max_page_size=_positive_int(
                source.get("A2X_GROUPCHAT_MAX_PAGE_SIZE", "100"),
                "A2X_GROUPCHAT_MAX_PAGE_SIZE",
            ),
            default_page_size=_positive_int(
                source.get("A2X_GROUPCHAT_DEFAULT_PAGE_SIZE", "50"),
                "A2X_GROUPCHAT_DEFAULT_PAGE_SIZE",
            ),
            rate_limit_per_group=_positive_int(
                source.get("A2X_GROUPCHAT_RATE_LIMIT_PER_GROUP", "600"),
                "A2X_GROUPCHAT_RATE_LIMIT_PER_GROUP",
            ),
            rate_limit_window_seconds=_positive_int(
                source.get("A2X_GROUPCHAT_RATE_LIMIT_WINDOW_SECONDS", "60"),
                "A2X_GROUPCHAT_RATE_LIMIT_WINDOW_SECONDS",
            ),
            create_timeout_seconds=_positive_int(
                source.get("A2X_GROUPCHAT_CREATE_TIMEOUT_SECONDS", "10"),
                "A2X_GROUPCHAT_CREATE_TIMEOUT_SECONDS",
            ),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.backend not in {"postgres", "memory"}:
            raise ValueError(
                "A2X_GROUPCHAT_BACKEND must be 'postgres' or 'memory', "
                f"got {self.backend!r}"
            )
        if self.enabled and self.backend == "postgres" and not self.dsn:
            raise ValueError(
                "A2X_GROUPCHAT_DSN is required when the group chat module is "
                "enabled with the postgres backend"
            )
        if not self.schema or not self.schema.replace("_", "").isalnum():
            raise ValueError(
                "A2X_GROUPCHAT_SCHEMA must be a simple identifier"
            )
        if self.default_page_size > self.max_page_size:
            raise ValueError(
                "A2X_GROUPCHAT_DEFAULT_PAGE_SIZE must not exceed "
                "A2X_GROUPCHAT_MAX_PAGE_SIZE"
            )
        if not 1 <= self.max_members_per_group <= 100_000:
            raise ValueError(
                "A2X_GROUPCHAT_MAX_MEMBERS_PER_GROUP must be between 1 and 100000"
            )
