import pytest

from a2x_registry.stream_proxy.config import StreamProxyConfig


def test_config_accepts_exact_ws_origin_and_bounded_limits():
    config = StreamProxyConfig.from_env(
        {
            "A2X_STREAM_PROXY_HOST": "0.0.0.0",
            "A2X_STREAM_PROXY_PORT": "8012",
            "A2X_STREAM_PROXY_PUBLIC_WS_BASE_URL": "wss://proxy.example/stream",
            "A2X_STREAM_PROXY_CREATE_TOKEN": "x" * 32,
            "A2X_STREAM_PROXY_MAX_OBJECT_BYTES": "10485760",
            "A2X_STREAM_PROXY_MAX_CHUNK_BYTES": "262144",
            "A2X_STREAM_PROXY_MAX_SESSIONS": "10",
            "A2X_STREAM_PROXY_SESSION_TTL_SECONDS": "600",
            "A2X_STREAM_PROXY_CLEANUP_INTERVAL_SECONDS": "10",
            "A2X_STREAM_PROXY_AUTH_TIMEOUT_SECONDS": "5",
            "A2X_STREAM_PROXY_RECONNECT_GRACE_SECONDS": "120",
        }
    )
    assert config.port == 8012
    assert config.public_ws_base_url == "wss://proxy.example/stream"
    assert config.max_chunk_bytes == 262144
    assert config.max_sessions == 10


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("public_ws_base_url", "https://proxy.example"),
        ("public_ws_base_url", "wss://proxy.example/path?token=bad"),
        ("public_ws_base_url", "wss://proxy.example/a//b"),
        ("public_ws_base_url", "wss://proxy.example/a/../b"),
        ("create_token", "short"),
        ("port", 70000),
        ("max_chunk_bytes", 2048),
    ],
)
def test_config_rejects_invalid_security_and_capacity_values(field, value):
    values = {
        "create_token": "x" * 32,
        "max_object_bytes": 1024,
        "max_chunk_bytes": 512,
    }
    values[field] = value
    with pytest.raises(ValueError):
        StreamProxyConfig(**values).validate()
