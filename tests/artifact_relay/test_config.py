from pathlib import Path

import pytest

from a2x_registry.artifact_relay.config import ArtifactRelayConfig


def test_defaults_are_disabled():
    config = ArtifactRelayConfig.from_env({})
    assert config.enabled is False
    assert config.max_object_bytes == 256 * 1024 * 1024
    assert config.chunk_bytes == 1024 * 1024


def test_enabled_config_is_bounded_and_absolute(tmp_path):
    config = ArtifactRelayConfig.from_env(
        {
            "A2X_ARTIFACT_RELAY_ENABLED": "true",
            "A2X_ARTIFACT_RELAY_PUBLIC_BASE_URL": "https://registry.example",
            "A2X_ARTIFACT_RELAY_STORAGE_DIR": str(tmp_path),
            "A2X_ARTIFACT_RELAY_CREATE_TOKEN": "x" * 32,
            "A2X_ARTIFACT_RELAY_MAX_OBJECT_BYTES": "4096",
            "A2X_ARTIFACT_RELAY_CHUNK_BYTES": "1024",
            "A2X_ARTIFACT_RELAY_DEFAULT_TTL_SECONDS": "30",
            "A2X_ARTIFACT_RELAY_MAX_TTL_SECONDS": "60",
        }
    )
    assert config.enabled is True
    assert config.storage_dir == Path(tmp_path)
    assert config.chunk_bytes == 1024


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("A2X_ARTIFACT_RELAY_PUBLIC_BASE_URL", "registry.example"),
        ("A2X_ARTIFACT_RELAY_STORAGE_DIR", "relative/path"),
        ("A2X_ARTIFACT_RELAY_CHUNK_BYTES", str(8192)),
        ("A2X_ARTIFACT_RELAY_DEFAULT_TTL_SECONDS", "120"),
        ("A2X_ARTIFACT_RELAY_CREATE_TOKEN", "too-short"),
    ],
)
def test_enabled_config_rejects_unsafe_values(tmp_path, name, value):
    env = {
        "A2X_ARTIFACT_RELAY_ENABLED": "true",
        "A2X_ARTIFACT_RELAY_PUBLIC_BASE_URL": "https://registry.example",
        "A2X_ARTIFACT_RELAY_STORAGE_DIR": str(tmp_path),
        "A2X_ARTIFACT_RELAY_CREATE_TOKEN": "x" * 32,
        "A2X_ARTIFACT_RELAY_MAX_OBJECT_BYTES": "4096",
        "A2X_ARTIFACT_RELAY_CHUNK_BYTES": "1024",
        "A2X_ARTIFACT_RELAY_DEFAULT_TTL_SECONDS": "30",
        "A2X_ARTIFACT_RELAY_MAX_TTL_SECONDS": "60",
    }
    env[name] = value
    with pytest.raises(ValueError):
        ArtifactRelayConfig.from_env(env)
