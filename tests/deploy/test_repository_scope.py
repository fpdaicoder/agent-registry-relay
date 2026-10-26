"""Release-scope guardrails for the standalone registry/relay repository."""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_a2x_search_is_present_but_vector_is_not_a_required_dependency():
    required = []
    for path in (
        "a2x_registry/a2x",
        "a2x_registry/vector",
        "a2x_registry/traditional",
        "a2x_registry/backend/routers/search.py",
        "a2x_registry/backend/routers/build.py",
        "a2x_registry/backend/routers/provider.py",
        "a2x_registry/common/llm_client.py",
    ):
        target = ROOT / path
        if not (target.is_file() or target.is_dir()):
            required.append(path)
    assert required == [], f"A2X search modules missing: {required}"

    metadata = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    dependencies = metadata.split("[project.optional-dependencies]", 1)[0]
    assert "sentence-transformers" not in dependencies
    assert "chromadb" not in dependencies
    assert "sentence-transformers" in metadata
    assert "a2x-build" in metadata


def _route_paths(app) -> list[str]:
    paths = []
    for route in app.routes:
        router = getattr(route, "original_router", None)
        nested = router.routes if router is not None else [route]
        for item in nested:
            path = getattr(item, "path", None)
            if path:
                paths.append(path)
    return paths


def test_search_routes_do_not_replace_relay_groupchat_or_tasks():
    import sys

    from a2x_registry.backend.app import app

    assert "chromadb" not in sys.modules
    assert "sentence_transformers" not in sys.modules
    route_paths = _route_paths(app)
    assert "/api/search" in route_paths
    assert "/api/datasets/{dataset}/build" in route_paths
    assert "/api/providers" in route_paths
    assert "/a2a/{dataset}/{service_id}" in route_paths
    assert "/api/groups" in route_paths
    assert "/api/tasks" in route_paths
    assert "/api/tunnel/status" in route_paths
    assert "/tunnel" in route_paths
    assert "/api/tcp-tunnel/status" in route_paths
    assert "/api/artifact-relay/status" in route_paths


def test_readme_documents_only_supported_relay_routes():
    from a2x_registry.relay.router import router

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    relay_section = readme.split("## Relay", 1)[1].split("### WebSocket Tunnel", 1)[0]
    documented = {
        (method, path)
        for method, path in re.findall(r"`(GET|POST) ([^`]+)`", relay_section)
    }
    supported = {
        (method, route.path)
        for route in router.routes
        for method in route.methods
    }

    assert documented == supported


def test_readme_stream_proxy_variables_are_consumed_by_config():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    section = readme.split("### Stream Proxy", 1)[1].split("## ", 1)[0]
    documented = set(re.findall(r"\bA2X_STREAM_PROXY_[A-Z0-9_]+\b", section))
    config = (
        ROOT / "a2x_registry" / "stream_proxy" / "config.py"
    ).read_text(encoding="utf-8")

    unknown = sorted(variable for variable in documented if variable not in config)
    assert unknown == [], f"README documents unused Stream Proxy variables: {unknown}"
