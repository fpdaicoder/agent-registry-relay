from __future__ import annotations

import ipaddress

import pytest

from a2x_registry.relay.config import RelayConfig
from a2x_registry.relay.errors import RelayError
from a2x_registry.relay.security import (
    filter_request_headers,
    validate_target_url,
)


def test_exact_origin_allows_explicit_loopback_target():
    config = RelayConfig(
        enabled=True,
        public_base_url="http://registry.example:8000",
        allowed_origins=frozenset({"http://127.0.0.1:9101"}),
    )
    validate_target_url("http://127.0.0.1:9101/a2a", config)


def test_cloud_metadata_is_blocked_even_by_broad_cidr():
    config = RelayConfig(
        enabled=True,
        public_base_url="http://registry.example:8000",
        allowed_cidrs=(ipaddress.ip_network("0.0.0.0/0"),),
        allowed_ports=frozenset({80}),
    )
    with pytest.raises(RelayError) as exc:
        validate_target_url("http://169.254.169.254/latest/meta-data", config)
    assert exc.value.code == "relay_target_forbidden"


def test_allow_all_accepts_an_unlisted_ip_and_port():
    config = RelayConfig(
        enabled=True,
        public_base_url="http://registry.example:8000",
        allow_all_targets=True,
    )
    validate_target_url("http://8.8.8.8:65535/a2a", config)


def test_allow_all_still_blocks_cloud_metadata():
    config = RelayConfig(
        enabled=True,
        public_base_url="http://registry.example:8000",
        allow_all_targets=True,
    )
    with pytest.raises(RelayError) as exc:
        validate_target_url("http://169.254.169.254/latest/meta-data", config)
    assert exc.value.code == "relay_target_forbidden"


def test_unlisted_port_is_blocked():
    config = RelayConfig(
        enabled=True,
        public_base_url="http://registry.example:8000",
        allowed_cidrs=(ipaddress.ip_network("10.0.0.0/8"),),
        allowed_ports=frozenset({8080}),
    )
    with pytest.raises(RelayError, match="port"):
        validate_target_url("http://10.1.2.3:9000/a2a", config)


def test_hostname_target_is_rejected_to_prevent_dns_rebinding():
    config = RelayConfig(
        enabled=True,
        public_base_url="http://registry.example:8000",
        allowed_origins=frozenset({"http://agent.example:8080"}),
    )
    with pytest.raises(RelayError, match="IP address"):
        validate_target_url("http://agent.example:8080/a2a", config)


def test_registry_authorization_is_not_forwarded_by_default():
    headers = filter_request_headers(
        {
            "Authorization": "Bearer registry-secret",
            "Content-Type": "application/json",
            "Connection": "keep-alive",
            "Traceparent": "00-abc-def-01",
        },
        forward_authorization=False,
    )
    assert "Authorization" not in headers
    assert headers["Content-Type"] == "application/json"
    assert "Connection" not in headers
