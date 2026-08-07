from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from a2x_registry.register.models import AgentCard


@dataclass
class FakeEntry:
    type: str
    agent_card: AgentCard | None


class FakeRegistry:
    def __init__(
        self,
        card: dict[str, Any] | None,
        *,
        service_type: str = "a2a",
        unhealthy: bool = False,
    ) -> None:
        self._entry = FakeEntry(
            service_type,
            AgentCard(**card) if card is not None else None,
        )
        self._unhealthy = unhealthy

    def get_entry(self, dataset: str, service_id: str):
        if dataset == "agents" and service_id == "target":
            return self._entry
        return None

    def is_unhealthy(self, dataset: str, service_id: str) -> bool:
        return self._unhealthy


def legacy_card(url: str = "http://127.0.0.1:9101/a2a") -> dict[str, Any]:
    return {
        "name": "Target Agent",
        "description": "relay target",
        "version": "1.0.0",
        "protocolVersion": "0.3",
        "url": url,
        "preferredTransport": "JSONRPC",
        "capabilities": {"streaming": True, "pushNotifications": True},
        "skills": [
            {
                "id": "relay-test",
                "name": "Relay Test",
                "description": "test",
                "tags": ["test"],
            }
        ],
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
    }


def v1_card(url: str = "http://127.0.0.1:9101/a2a") -> dict[str, Any]:
    card = legacy_card("")
    card["protocolVersion"] = "1.0"
    card["preferredTransport"] = ""
    card["supportedInterfaces"] = [
        {
            "url": url,
            "protocolBinding": "JSONRPC",
            "protocolVersion": "1.0",
        }
    ]
    return card
