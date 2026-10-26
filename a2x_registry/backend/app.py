"""FastAPI application for the registry control plane and relay data plane."""

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from a2x_registry import __version__
from a2x_registry.backend.routers import dataset
from a2x_registry.backend.startup import shutdown_registry, startup_registry
from a2x_registry.auth.router import router as auth_router
from a2x_registry.heartbeat.router import router as heartbeat_router
from a2x_registry.cluster.router import router as cluster_router
from a2x_registry.relay.router import router as relay_router
from a2x_registry.relay.deps import startup_relay, shutdown_relay
from a2x_registry.tunnel.router import router as tunnel_router
from a2x_registry.tunnel.deps import startup_tunnel, shutdown_tunnel
from a2x_registry.tcp_tunnel.router import router as tcp_tunnel_router
from a2x_registry.tcp_tunnel.deps import startup_tcp_tunnel, shutdown_tcp_tunnel
from a2x_registry.artifact_relay.router import router as artifact_relay_router
from a2x_registry.artifact_relay.deps import (
    startup_artifact_relay,
    shutdown_artifact_relay,
)
from a2x_registry.groupchat.router import router as groupchat_router
from a2x_registry.groupchat.deps import (
    startup_groupchat,
    shutdown_groupchat,
)
from a2x_registry.tasks.router import router as tasks_router
from a2x_registry.tasks.deps import (
    startup_tasks,
    shutdown_tasks,
)
from a2x_registry.common.errors import FeatureNotInstalledError, LLMNotConfiguredError

logger = logging.getLogger("uvicorn")


def _load_a2x_routers():
    """Import A2X search routers without taking down registry or relay."""
    try:
        from a2x_registry.backend.routers import build, provider, search
    except Exception as exc:  # noqa: BLE001 — search is optional
        logger.error("A2X search routers unavailable: %s", exc, exc_info=True)
        return None
    return search, build, provider


def _bind_a2x_search() -> None:
    """Point search at dataset files. Failure leaves relay and tasks running."""
    try:
        from a2x_registry.backend.routers import build
        from a2x_registry.backend.routers.dataset import get_registry_service
        from a2x_registry.backend.services.a2x_registry_view import A2XRegistryView
        from a2x_registry.backend.services.search_service import set_registry

        view = A2XRegistryView(get_registry_service())
        set_registry(view)
        build.init_registry_service(view)
    except Exception as exc:  # noqa: BLE001 — search binding must not block startup
        logger.error("A2X search binding skipped: %s", exc, exc_info=True)


_a2x_routers = _load_a2x_routers()


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Own the registry and optional data-plane service lifecycle."""
    startup_registry()
    _bind_a2x_search()
    await startup_relay()
    await startup_tunnel()
    await startup_tcp_tunnel()
    await startup_artifact_relay()
    await startup_groupchat()
    await startup_tasks()

    try:
        yield
    finally:
        await shutdown_tasks()
        await shutdown_groupchat()
        await shutdown_artifact_relay()
        await shutdown_tcp_tunnel()
        await shutdown_tunnel()
        await shutdown_relay()
        shutdown_registry()


app = FastAPI(
    title="Agent Registry Relay",
    description="Agent service registry, discovery control plane, and optional data plane",
    version=__version__,
    lifespan=_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dataset.router)
if _a2x_routers is not None:
    _search_router, _build_router, _provider_router = _a2x_routers
    # A2X search, taxonomy build, and LLM provider selection. Vector search
    # returns 503 until the optional vector extra is installed. A2X and
    # traditional search return 503 until llm_apikey.json is configured.
    app.include_router(_search_router.router)
    app.include_router(_build_router.router)
    app.include_router(_provider_router.router)
# /api/auth/* — the router itself returns 404 when auth is not initialized,
# so mounting it unconditionally is safe and keeps the app graph simple.
app.include_router(auth_router)
# Heartbeat endpoints. Same 404 fallback semantics when the heartbeat
# module isn't initialized (e.g. lite mode without startup hook).
app.include_router(heartbeat_router)
# Cluster (distributed sync) endpoints. Opt-in: every route 404s until the
# cluster module is initialized (cluster_state.json present at startup).
app.include_router(cluster_router)
# Optional A2A relay data plane. The router is always mounted, but returns a
# structured 404 unless A2X_RELAY_ENABLED=true.
app.include_router(relay_router)
# Optional WebSocket tunnel. It uses its own listener (8001 by default) but
# shares this process and systemd lifecycle with the registry.
app.include_router(tunnel_router)


@app.websocket("/tunnel")
async def _tunnel_websocket(websocket: WebSocket) -> None:
    from a2x_registry.tunnel.asgi import tunnel_websocket

    await tunnel_websocket(websocket)
# Optional reverse TCP tunnel (port forwarding for arbitrary TCP services).
# Its control/data listener defaults to 8003 and proxy ports come from a
# configurable range; same process and systemd lifecycle as the registry.
app.include_router(tcp_tunnel_router)
# Optional store-and-forward artifact relay. Payload bytes use this bounded,
# resumable HTTP surface instead of A2A JSON or WebSocket control frames.
app.include_router(artifact_relay_router)
# Optional multi-Agent group chat. Always mounted; every route returns a
# structured 404 unless A2X_GROUPCHAT_ENABLED=true.
app.include_router(groupchat_router)
# Optional personal task-continuation. Always mounted; every route returns a
# structured 404 unless A2X_TASKS_ENABLED=true.
app.include_router(tasks_router)


@app.exception_handler(FeatureNotInstalledError)
async def _feature_not_installed_handler(_request: Request, exc: FeatureNotInstalledError):
    return JSONResponse(
        status_code=503,
        content={"feature": exc.feature, "extras": exc.extras, "detail": str(exc)},
    )


@app.exception_handler(LLMNotConfiguredError)
async def _llm_not_configured_handler(_request: Request, exc: LLMNotConfiguredError):
    return JSONResponse(
        status_code=503,
        content={"reason": "llm_not_configured", "detail": str(exc)},
    )


_frontend_dist = os.environ.get("A2X_FRONTEND_DIST_DIR")
if _frontend_dist and Path(_frontend_dist).is_dir():
    app.mount("/", StaticFiles(directory=_frontend_dist, html=True), name="frontend")
