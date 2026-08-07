"""Small transport-neutral relay data models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RelayTarget:
    dataset: str
    service_id: str
    url: str
    protocol_binding: str
    protocol_version: str
    agent_card: dict[str, Any]
