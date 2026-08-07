"""Runtime path resolution.

The library ships **without** bundled runtime data. The external ``database/``
directory stores registry namespaces and service records.
  Lookup order:
    1. ``<A2X_REGISTRY_HOME>/database`` (when the env var is set)
    2. ``./database`` under CWD — convenient for devs running from a
       cloned source tree where the database is a git submodule.
    3. ``~/.a2x_registry/database``

The target is created by the registry service on write.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

ENV_VAR = "A2X_REGISTRY_HOME"
DEFAULT_USER_HOME = Path.home() / ".a2x_registry"


def _env_home() -> Path | None:
    """Return the explicit ``A2X_REGISTRY_HOME`` if set, else ``None``."""
    env = os.environ.get(ENV_VAR)
    return Path(env).expanduser().resolve() if env else None


@lru_cache(maxsize=1)
def get_home() -> Path:
    """Home directory used by :func:`database_dir`.

    Lookup:
      1. ``A2X_REGISTRY_HOME`` env var
      2. CWD if it contains ``./database/`` (source-tree dev mode)
      3. ``~/.a2x_registry/``
    """
    env = _env_home()
    if env:
        return env
    if (Path.cwd() / "database").is_dir():
        return Path.cwd().resolve()
    return DEFAULT_USER_HOME


def database_dir() -> Path:
    return get_home() / "database"


def dataset_dir(dataset: str) -> Path:
    return database_dir() / dataset


def reset_cache() -> None:
    """Clear the cached home lookup. Tests call this after mutating ``A2X_REGISTRY_HOME``."""
    get_home.cache_clear()
