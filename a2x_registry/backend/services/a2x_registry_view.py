"""Read-only view that lets A2X search use the relay registry's dataset files.

The relay ``RegistryService`` owns registration, auth, and leases. Search and
taxonomy build only need paths plus a hash check. This wrapper does not call
register, update, or delete, so those control-plane paths stay unchanged.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


class A2XRegistryView:
    def __init__(self, registry) -> None:
        self._registry = registry

    def list_datasets_with_counts(self):
        return self._registry.list_datasets_with_counts()

    def dataset_dir(self, dataset: str) -> Path:
        return self._registry.dataset_dir(dataset)

    def service_json_path(self, dataset: str) -> Path:
        return self._registry.service_json_path(dataset)

    def query_path(self, dataset: str) -> Path:
        return self.dataset_dir(dataset) / "query" / "query.json"

    def taxonomy_dir(self, dataset: str) -> Path:
        return self.dataset_dir(dataset) / "taxonomy"

    def taxonomy_path(self, dataset: str) -> Path:
        return self.taxonomy_dir(dataset) / "taxonomy.json"

    def class_path(self, dataset: str) -> Path:
        return self.taxonomy_dir(dataset) / "class.json"

    def chroma_dir(self) -> Path:
        return self.service_json_path("_").parent.parent / "chroma"

    def get_vector_config(self, dataset: str) -> dict:
        from a2x_registry.vector.utils.embedding_constants import (
            DEFAULT_EMBEDDING_MODEL,
            EMBEDDING_MODELS,
        )

        path = self.dataset_dir(dataset) / "vector_config.json"
        if path.is_file():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                data = None
            if isinstance(data, dict) and data.get("embedding_model"):
                return dict(data)
        model = DEFAULT_EMBEDDING_MODEL
        return {
            "embedding_model": model,
            "embedding_dim": EMBEDDING_MODELS[model]["dim"],
        }

    def check_taxonomy_state(self, dataset: str):
        from a2x_registry.register.models import TaxonomyState

        taxonomy = self.taxonomy_path(dataset)
        config = self.taxonomy_dir(dataset) / "build_config.json"
        service = self.service_json_path(dataset)
        if not taxonomy.is_file() or not config.is_file() or not service.is_file():
            return TaxonomyState.NONEXISTENT
        try:
            stored = json.loads(config.read_text(encoding="utf-8")).get("service_hash")
            services = json.loads(service.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError):
            return TaxonomyState.NONEXISTENT
        if not stored or not isinstance(services, list):
            return TaxonomyState.NONEXISTENT
        pairs = sorted(
            (item.get("name", ""), item.get("description", ""))
            for item in services
            if isinstance(item, dict)
        )
        current = hashlib.sha256(
            json.dumps(pairs, ensure_ascii=False).encode()
        ).hexdigest()
        if current == stored:
            return TaxonomyState.AVAILABLE
        return TaxonomyState.UNAVAILABLE
