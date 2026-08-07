from __future__ import annotations

import pytest

from a2x_registry.relay.config import RelayConfig


def test_disabled_by_default():
    config = RelayConfig.from_env({})
    assert config.enabled is False
    assert config.public_base_url == ""
    assert config.allow_all_targets is False


def test_enabled_requires_public_url_and_allowlist():
    with pytest.raises(ValueError, match="PUBLIC_BASE_URL"):
        RelayConfig.from_env({"A2X_RELAY_ENABLED": "true"})

    with pytest.raises(ValueError, match="ALLOWED_ORIGINS"):
        RelayConfig.from_env({
            "A2X_RELAY_ENABLED": "true",
            "A2X_RELAY_PUBLIC_BASE_URL": "http://registry.example:8000",
        })


def test_enabled_config_normalizes_exact_origins():
    config = RelayConfig.from_env({
        "A2X_RELAY_ENABLED": "true",
        "A2X_RELAY_PUBLIC_BASE_URL": "http://registry.example:8000/",
        "A2X_RELAY_ALLOWED_ORIGINS": "http://127.0.0.1:9101/",
        "A2X_RELAY_MAX_INFLIGHT": "12",
    })
    assert config.public_base_url == "http://registry.example:8000"
    assert config.allowed_origins == frozenset({"http://127.0.0.1:9101"})
    assert config.max_inflight == 12


def test_allow_all_does_not_require_an_allowlist():
    config = RelayConfig.from_env({
        "A2X_RELAY_ENABLED": "true",
        "A2X_RELAY_PUBLIC_BASE_URL": "http://registry.example:8000",
        "A2X_RELAY_ALLOW_ALL_TARGETS": "true",
    })
    assert config.allow_all_targets is True
    assert config.allowed_origins == frozenset()
    assert config.allowed_cidrs == ()


def test_invalid_boolean_is_rejected():
    with pytest.raises(ValueError, match="true or false"):
        RelayConfig.from_env({"A2X_RELAY_ENABLED": "sometimes"})
