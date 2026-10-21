"""Environment-backed configuration for the task-continuation data plane."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


def _positive_int(value: str, name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


def _non_negative_int(value: str, name: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"{name} must be zero or greater")
    return parsed


def _bool(value: str, name: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError(f"{name} must be a boolean, got {value!r}")


@dataclass(frozen=True)
class TasksConfig:
    """Runtime configuration for the optional personal-task module.

    ``dsn`` defaults to ``A2X_GROUPCHAT_DSN`` so a single PostgreSQL instance
    can host both features (different ``tk_*`` tables). Only the PostgreSQL
    backend consults it; the in-memory backend ignores it so the test suite
    stays free of a database dependency.
    """

    enabled: bool = False
    backend: str = "postgres"
    dsn: str = ""
    schema: str = "public"
    max_message_bytes: int = 64 * 1024
    max_tasks_per_principal: int = 200
    max_page_size: int = 100
    default_page_size: int = 50
    # Lease lifetimes. TTL is crash recovery only (fence is the truth); the
    # sweeper flips an expired lease back to ``input_required`` after this
    # long without a renew.
    lease_ttl_seconds: int = 300
    min_lease_ttl_seconds: int = 10
    max_lease_ttl_seconds: int = 3600
    sweeper_interval_seconds: int = 30
    # Wall-clock bound for a graceful handoff's soft wait before it degrades
    # to a force takeover (see design §3.2).
    graceful_timeout_seconds: int = 5
    rate_limit_per_task: int = 600
    rate_limit_window_seconds: int = 60

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "TasksConfig":
        source = os.environ if env is None else env
        config = cls(
            enabled=_bool(source.get("A2X_TASKS_ENABLED", "false"), "A2X_TASKS_ENABLED"),
            backend=source.get("A2X_TASKS_BACKEND", "postgres").strip().lower(),
            # Shared DSN fallback: reuse the group chat database when the tasks
            # module has not been given its own, matching design §7.
            dsn=(
                source.get("A2X_TASKS_DSN", "")
                or source.get("A2X_GROUPCHAT_DSN", "")
            ).strip(),
            schema=source.get("A2X_TASKS_SCHEMA", "public").strip(),
            max_message_bytes=_positive_int(
                source.get("A2X_TASKS_MAX_MESSAGE_BYTES", str(64 * 1024)),
                "A2X_TASKS_MAX_MESSAGE_BYTES",
            ),
            max_tasks_per_principal=_positive_int(
                source.get("A2X_TASKS_MAX_TASKS_PER_PRINCIPAL", "200"),
                "A2X_TASKS_MAX_TASKS_PER_PRINCIPAL",
            ),
            max_page_size=_positive_int(
                source.get("A2X_TASKS_MAX_PAGE_SIZE", "100"),
                "A2X_TASKS_MAX_PAGE_SIZE",
            ),
            default_page_size=_positive_int(
                source.get("A2X_TASKS_DEFAULT_PAGE_SIZE", "50"),
                "A2X_TASKS_DEFAULT_PAGE_SIZE",
            ),
            lease_ttl_seconds=_positive_int(
                source.get("A2X_TASKS_LEASE_TTL_SECONDS", "300"),
                "A2X_TASKS_LEASE_TTL_SECONDS",
            ),
            min_lease_ttl_seconds=_positive_int(
                source.get("A2X_TASKS_MIN_LEASE_TTL_SECONDS", "10"),
                "A2X_TASKS_MIN_LEASE_TTL_SECONDS",
            ),
            max_lease_ttl_seconds=_positive_int(
                source.get("A2X_TASKS_MAX_LEASE_TTL_SECONDS", "3600"),
                "A2X_TASKS_MAX_LEASE_TTL_SECONDS",
            ),
            sweeper_interval_seconds=_positive_int(
                source.get("A2X_TASKS_SWEEPER_INTERVAL_SECONDS", "30"),
                "A2X_TASKS_SWEEPER_INTERVAL_SECONDS",
            ),
            graceful_timeout_seconds=_non_negative_int(
                source.get("A2X_TASKS_GRACEFUL_TIMEOUT_SECONDS", "5"),
                "A2X_TASKS_GRACEFUL_TIMEOUT_SECONDS",
            ),
            rate_limit_per_task=_positive_int(
                source.get("A2X_TASKS_RATE_LIMIT_PER_TASK", "600"),
                "A2X_TASKS_RATE_LIMIT_PER_TASK",
            ),
            rate_limit_window_seconds=_positive_int(
                source.get("A2X_TASKS_RATE_LIMIT_WINDOW_SECONDS", "60"),
                "A2X_TASKS_RATE_LIMIT_WINDOW_SECONDS",
            ),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.backend not in {"postgres", "memory"}:
            raise ValueError(
                "A2X_TASKS_BACKEND must be 'postgres' or 'memory', "
                f"got {self.backend!r}"
            )
        if self.enabled and self.backend == "postgres" and not self.dsn:
            raise ValueError(
                "A2X_TASKS_DSN is required when the tasks module is enabled "
                "with the postgres backend (or set A2X_GROUPCHAT_DSN)"
            )
        if not self.schema or not self.schema.replace("_", "").isalnum():
            raise ValueError("A2X_TASKS_SCHEMA must be a simple identifier")
        if self.default_page_size > self.max_page_size:
            raise ValueError(
                "A2X_TASKS_DEFAULT_PAGE_SIZE must not exceed "
                "A2X_TASKS_MAX_PAGE_SIZE"
            )
        if not self.min_lease_ttl_seconds <= self.max_lease_ttl_seconds:
            raise ValueError(
                "A2X_TASKS_MIN_LEASE_TTL_SECONDS must not exceed "
                "A2X_TASKS_MAX_LEASE_TTL_SECONDS"
            )