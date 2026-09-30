"""FastAPI application for the registry control plane and relay data plane."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Own the registry and optional data-plane service lifecycle."""
    startup_registry()
    await startup_relay()
    await startup_tunnel()
    await startup_tcp_tunnel()
    await startup_artifact_relay()
    await startup_groupchat()

    try:
        yield
    finally:
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
