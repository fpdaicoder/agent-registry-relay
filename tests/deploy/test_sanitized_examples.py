"""Keep shareable deployment templates free of environment-specific secrets."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[2]
DEPLOY_DIR = ROOT / "deploy"


def _deployment_text() -> str:
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(DEPLOY_DIR.iterdir())
        if path.is_file()
    )


def _environment_values(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip().removeprefix('Environment="').removesuffix('"')
        if "=" not in stripped:
            continue
        name, value = stripped.split("=", 1)
        if name.startswith("A2X_"):
            values[name] = value
    return values


def test_public_urls_use_documentation_hosts() -> None:
    values = _environment_values(_deployment_text())
    public_urls = {
        name: value
        for name, value in values.items()
        if name.endswith(("PUBLIC_BASE_URL", "PUBLIC_WS_BASE_URL"))
    }

    assert public_urls
    for name, value in public_urls.items():
        hostname = urlsplit(value).hostname
        assert hostname == "example.com" or hostname.endswith(".example.com"), (
            f"{name} must use an RFC 2606 documentation hostname"
        )


def test_deployment_templates_match_runtime_config() -> None:
    from a2x_registry.artifact_relay.config import ArtifactRelayConfig
    from a2x_registry.stream_proxy.config import StreamProxyConfig
    from a2x_registry.tunnel.config import TunnelConfig

    values = _environment_values(_deployment_text())

    assert ArtifactRelayConfig.from_env(values).enabled
    assert TunnelConfig.from_env(values).enabled
    assert StreamProxyConfig.from_env(values).public_ws_base_url.startswith("wss://")


def test_service_template_has_no_host_account_fingerprint() -> None:
    service = (DEPLOY_DIR / "agentregistry-stream-proxy.service").read_text(encoding="utf-8")

    assert "/home/" not in service
    assert "User=agent-registry" in service
    assert "Group=agent-registry" in service
    assert "WorkingDirectory=/opt/agent-registry" in service


def test_deployment_templates_have_no_private_key_material() -> None:
    text = _deployment_text()
    assert not re.search(
        r"BEGIN (?:OPENSSH|RSA|EC|DSA) PRIVATE KEY",
        text,
        flags=re.IGNORECASE,
    )


def test_sensitive_runtime_files_are_ignored() -> None:
    patterns = {
        line.strip()
        for line in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    required = {
        ".env",
        ".env.*",
        "*.pem",
        "*.key",
        "id_rsa*",
        "id_ed25519*",
        "known_hosts",
        "authorized_keys",
        "credentials/",
        "secrets/",
    }
    assert required <= patterns
    assert "!*.env.example" in patterns
