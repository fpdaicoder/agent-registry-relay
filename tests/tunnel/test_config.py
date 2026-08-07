import pytest

from a2x_registry.tunnel.config import TunnelConfig


def test_tunnel_disabled_by_default():
    config = TunnelConfig.from_env({})

    assert config.enabled is False
    assert config.auto_bind_registered_services is False
    assert config.port == 8001
    assert config.request_timeout_seconds == 300
    assert config.max_message_bytes == 50 * 1024 * 1024
    assert config.shared_token == ""


def test_tunnel_reads_environment():
    config = TunnelConfig.from_env(
        {
            "A2X_TUNNEL_ENABLED": "true",
            "A2X_TUNNEL_AUTO_BIND_REGISTERED_SERVICES": "true",
            "A2X_TUNNEL_HOST": "127.0.0.1",
            "A2X_TUNNEL_PORT": "9001",
            "A2X_TUNNEL_MAX_DEVICES": "12",
            "A2X_TUNNEL_SHARED_TOKEN": "secret",
        }
    )

    assert config.enabled is True
    assert config.auto_bind_registered_services is True
    assert config.host == "127.0.0.1"
    assert config.port == 9001
    assert config.max_devices == 12
    assert config.shared_token == "secret"


def test_default_timeout_cannot_exceed_maximum():
    with pytest.raises(ValueError, match="must not exceed"):
        TunnelConfig.from_env(
            {
                "A2X_TUNNEL_REQUEST_TIMEOUT_SECONDS": "20",
                "A2X_TUNNEL_MAX_REQUEST_TIMEOUT_SECONDS": "10",
            }
        )
