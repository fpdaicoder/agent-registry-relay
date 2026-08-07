"""Resolve registered Agent Cards into relay targets."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable
from urllib.parse import quote

from .config import RelayConfig
from .errors import RelayError
from .models import RelayTarget


_SUPPORTED_BINDINGS = frozenset({"JSONRPC", "HTTP+JSON"})


def _foreign_card(dataset: str, service_id: str) -> dict[str, Any] | None:
    try:
        from a2x_registry.cluster.deps import get_cluster_store

        store = get_cluster_store()
        wrapped = store.foreign_entry(dataset, service_id) if store is not None else None
    except Exception:
        wrapped = None
    if not isinstance(wrapped, dict) or wrapped.get("type") != "a2a":
        return None
    metadata = wrapped.get("metadata")
    return metadata if isinstance(metadata, dict) else None


def _select_interface(card: dict[str, Any]) -> tuple[str, str, str]:
    interfaces = card.get("supportedInterfaces")
    unsupported_bindings: list[str] = []
    if isinstance(interfaces, list):
        for interface in interfaces:
            if not isinstance(interface, dict):
                continue
            url = interface.get("url")
            binding = str(interface.get("protocolBinding", "")).upper()
            if isinstance(url, str) and url:
                if binding in _SUPPORTED_BINDINGS:
                    return url, binding, str(interface.get("protocolVersion", ""))
                if binding:
                    unsupported_bindings.append(binding)

    url = card.get("url")
    binding = str(card.get("preferredTransport") or "JSONRPC").upper()
    if isinstance(url, str) and url and binding in _SUPPORTED_BINDINGS:
        return url, binding, str(card.get("protocolVersion", ""))
    if unsupported_bindings or (isinstance(url, str) and url and binding):
        names = sorted(set(
            unsupported_bindings + ([binding] if isinstance(url, str) and url else [])
        ))
        raise RelayError(
            501,
            "relay_binding_not_supported",
            f"Relay does not support Agent bindings: {', '.join(names)}",
        )
    raise RelayError(422, "relay_interface_missing", "Agent Card has no relay-compatible interface")


class RelayResolver:
    def __init__(
        self,
        config: RelayConfig,
        registry_getter: Callable[[], Any],
    ) -> None:
        self._config = config
        self._registry_getter = registry_getter

    def _card(self, dataset: str, service_id: str) -> dict[str, Any]:
        registry = self._registry_getter()
        entry = registry.get_entry(dataset, service_id)
        if entry is not None:
            if entry.type != "a2a":
                raise RelayError(422, "relay_target_not_a2a", "Target service is not an A2A Agent")
            if registry.is_unhealthy(dataset, service_id):
                raise RelayError(409, "relay_target_unhealthy", "Target Agent is unhealthy")
            if entry.agent_card is None:
                raise RelayError(422, "relay_card_unresolved", "Target Agent Card is unresolved")
            return entry.agent_card.model_dump(exclude_none=True)

        card = _foreign_card(dataset, service_id)
        if card is not None:
            return card
        raise RelayError(404, "relay_target_not_found", "Target Agent is not registered")

    def resolve(self, dataset: str, service_id: str) -> RelayTarget:
        card = self._card(dataset, service_id)
        return self._target(dataset, service_id, card)

    def _target(
        self,
        dataset: str,
        service_id: str,
        card: dict[str, Any],
    ) -> RelayTarget:
        url, binding, version = _select_interface(card)
        public_prefix = self._config.public_base_url.rstrip("/") + "/a2a/"
        if url.startswith(public_prefix):
            raise RelayError(409, "relay_loop_detected", "Target Agent Card points back to this relay")
        return RelayTarget(
            dataset=dataset,
            service_id=service_id,
            url=url,
            protocol_binding=binding,
            protocol_version=version,
            agent_card=card,
        )

    def agent_card(self, dataset: str, service_id: str, route: str) -> dict[str, Any]:
        if route not in {"direct", "relay"}:
            raise RelayError(400, "relay_invalid_route", "route must be direct or relay")
        card = deepcopy(self._card(dataset, service_id))
        if route == "direct":
            return card

        target = self._target(dataset, service_id, card)
        relay_url = (
            f"{self._config.public_base_url.rstrip('/')}/a2a/"
            f"{quote(dataset, safe='')}/{quote(service_id, safe='')}"
        )
        version = target.protocol_version or str(card.get("protocolVersion", "1.0"))
        card["url"] = relay_url
        card["preferredTransport"] = target.protocol_binding
        card["supportedInterfaces"] = [{
            "url": relay_url,
            "protocolBinding": target.protocol_binding,
            "protocolVersion": version,
        }]
        capabilities = card.get("capabilities")
        capabilities = dict(capabilities) if isinstance(capabilities, dict) else {}
        capabilities["pushNotifications"] = False
        card["capabilities"] = capabilities
        return card
