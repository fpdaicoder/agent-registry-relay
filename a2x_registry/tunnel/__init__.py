"""Optional WebSocket tunnel for cross-network agents."""

from .config import TunnelConfig
from .server import WebSocketTunnelServer

__all__ = ["TunnelConfig", "WebSocketTunnelServer"]
