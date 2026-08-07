from __future__ import annotations

import pytest

from a2x_registry.relay.config import RelayConfig
from a2x_registry.relay.errors import RelayError
from a2x_registry.relay.resolver import RelayResolver

from .conftest import FakeRegistry, legacy_card, v1_card


def config() -> RelayConfig:
    return RelayConfig.from_env({
        "A2X_RELAY_ENABLED": "true",
        "A2X_RELAY_PUBLIC_BASE_URL": "http://registry.example:8000",
        "A2X_RELAY_ALLOWED_ORIGINS": "http://127.0.0.1:9101",
    })


@pytest.mark.parametrize("card_factory", [legacy_card, v1_card])
def test_resolves_legacy_and_v1_cards(card_factory):
    registry = FakeRegistry(card_factory())
    target = RelayResolver(config(), lambda: registry).resolve("agents", "target")
    assert target.url == "http://127.0.0.1:9101/a2a"
    assert target.protocol_binding == "JSONRPC"


def test_derived_relay_card_does_not_mutate_origin():
    card = v1_card()
    registry = FakeRegistry(card)
    resolver = RelayResolver(config(), lambda: registry)

    relay_card = resolver.agent_card("agents", "target", "relay")
    direct_card = resolver.agent_card("agents", "target", "direct")

    relay_url = "http://registry.example:8000/a2a/agents/target"
    assert relay_card["url"] == relay_url
    assert relay_card["supportedInterfaces"][0]["url"] == relay_url
    assert relay_card["capabilities"]["pushNotifications"] is False
    assert direct_card["supportedInterfaces"][0]["url"] == "http://127.0.0.1:9101/a2a"


def test_relay_card_preserves_http_json_binding():
    card = v1_card()
    card["supportedInterfaces"][0]["protocolBinding"] = "HTTP+JSON"
    resolver = RelayResolver(config(), lambda: FakeRegistry(card))

    relay_card = resolver.agent_card("agents", "target", "relay")

    assert relay_card["preferredTransport"] == "HTTP+JSON"
    assert relay_card["supportedInterfaces"][0]["protocolBinding"] == "HTTP+JSON"


def test_direct_card_does_not_require_a_relay_supported_binding():
    card = v1_card("grpc://agent.example:443")
    card["supportedInterfaces"][0]["protocolBinding"] = "GRPC"
    resolver = RelayResolver(config(), lambda: FakeRegistry(card))

    direct_card = resolver.agent_card("agents", "target", "direct")

    assert direct_card["supportedInterfaces"][0]["protocolBinding"] == "GRPC"


def test_missing_non_a2a_and_unhealthy_targets_are_rejected():
    missing = RelayResolver(config(), lambda: FakeRegistry(v1_card()))
    with pytest.raises(RelayError) as exc:
        missing.resolve("agents", "missing")
    assert exc.value.status_code == 404

    non_a2a = RelayResolver(
        config(),
        lambda: FakeRegistry(v1_card(), service_type="generic"),
    )
    with pytest.raises(RelayError) as exc:
        non_a2a.resolve("agents", "target")
    assert exc.value.code == "relay_target_not_a2a"

    unhealthy = RelayResolver(
        config(),
        lambda: FakeRegistry(v1_card(), unhealthy=True),
    )
    with pytest.raises(RelayError) as exc:
        unhealthy.resolve("agents", "target")
    assert exc.value.code == "relay_target_unhealthy"


def test_relay_loop_is_rejected():
    registry = FakeRegistry(v1_card("http://registry.example:8000/a2a/agents/target"))
    resolver = RelayResolver(config(), lambda: registry)
    with pytest.raises(RelayError) as exc:
        resolver.resolve("agents", "target")
    assert exc.value.code == "relay_loop_detected"
