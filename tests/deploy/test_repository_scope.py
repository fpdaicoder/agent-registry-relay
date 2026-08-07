"""Release-scope guardrails for the standalone registry/relay repository."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_search_and_evaluation_features_are_not_shipped():
    forbidden_paths = (
        "a2x_registry/a2x",
        "a2x_registry/vector",
        "a2x_registry/traditional",
        "a2x_registry/backend/routers/search.py",
        "a2x_registry/backend/routers/build.py",
        "a2x_registry/backend/routers/provider.py",
        "a2x_registry/backend/services",
        "a2x_registry/common/llm_client.py",
        "a2x_registry/llm_apikey.example.json",
        "tests/query",
        "ui",
    )

    leaked = []
    for path in forbidden_paths:
        target = ROOT / path
        if target.is_file() or (
            target.is_dir()
            and any(
                child.is_file() and "__pycache__" not in child.parts
                for child in target.rglob("*")
            )
        ):
            leaked.append(path)
    assert leaked == [], f"out-of-scope A2X features are still shipped: {leaked}"


def test_registry_app_exposes_no_search_build_or_provider_routes():
    from a2x_registry.backend.app import app

    forbidden_prefixes = (
        "/api/search",
        "/api/providers",
        "/api/datasets/embedding-models",
    )
    forbidden_fragments = ("/build", "/taxonomy", "/vector-config", "/default-queries")
    route_paths = [route.path for route in app.routes if hasattr(route, "path")]
    leaked = sorted(
        path
        for path in route_paths
        if path.startswith(forbidden_prefixes)
        or any(fragment in path for fragment in forbidden_fragments)
    )

    assert leaked == [], f"out-of-scope A2X routes are still mounted: {leaked}"


def test_package_metadata_has_no_search_or_evaluation_entry_points():
    metadata = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    forbidden = (
        "a2x-build",
        "a2x-evaluate-a2x",
        "a2x-evaluate-vector",
        "a2x-evaluate-traditional",
        "sentence-transformers",
        "chromadb",
        "llm_apikey.example.json",
    )

    leaked = [value for value in forbidden if value in metadata]
    assert leaked == [], f"out-of-scope package metadata remains: {leaked}"
