import pytest

from a2x_registry.tcp_tunnel.config import TcpTunnelConfig


def test_tcp_tunnel_disabled_by_default():
    config = TcpTunnelConfig.from_env({})

    assert config.enabled is False
    assert config.auto_bind_registered_services is False
    assert config.host == "0.0.0.0"
    assert config.port == 8003
    assert config.proxy_host == "0.0.0.0"
    assert config.port_range_min == 10000
    assert config.port_range_max == 11000
    assert config.max_devices == 1000
    assert config.max_connections == 10000
    assert config.max_targets_per_device == 32
    assert config.shared_token == ""


def test_tcp_tunnel_reads_environment():
    config = TcpTunnelConfig.from_env(
        {
            "A2X_TCP_TUNNEL_ENABLED": "true",
            "A2X_TCP_TUNNEL_AUTO_BIND_REGISTERED_SERVICES": "true",
            "A2X_TCP_TUNNEL_HOST": "127.0.0.1",
            "A2X_TCP_TUNNEL_PORT": "9003",
            "A2X_TCP_TUNNEL_PROXY_HOST": "127.0.0.1",
            "A2X_TCP_TUNNEL_PORT_RANGE_MIN": "20000",
            "A2X_TCP_TUNNEL_PORT_RANGE_MAX": "20010",
            "A2X_TCP_TUNNEL_MAX_DEVICES": "12",
            "A2X_TCP_TUNNEL_SHARED_TOKEN": "secret",
        }
    )

    assert config.enabled is True
    assert config.auto_bind_registered_services is True
    assert config.host == "127.0.0.1"
    assert config.port == 9003
    assert config.proxy_host == "127.0.0.1"
    assert config.port_range_min == 20000
    assert config.port_range_max == 20010
    assert config.max_devices == 12
    assert config.shared_token == "secret"


def test_port_range_min_cannot_exceed_max():
    with pytest.raises(ValueError, match="must not exceed"):
        TcpTunnelConfig.from_env(
            {
                "A2X_TCP_TUNNEL_PORT_RANGE_MIN": "20010",
                "A2X_TCP_TUNNEL_PORT_RANGE_MAX": "20000",
            }
        )


def test_control_port_must_stay_outside_proxy_port_range():
    with pytest.raises(ValueError, match="outside the proxy port range"):
        TcpTunnelConfig.from_env(
            {
                "A2X_TCP_TUNNEL_PORT": "10050",
                "A2X_TCP_TUNNEL_PORT_RANGE_MIN": "10000",
                "A2X_TCP_TUNNEL_PORT_RANGE_MAX": "11000",
            }
        )
