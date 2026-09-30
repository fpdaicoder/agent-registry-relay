"""Agent registry control plane and optional relay data planes.

Public sub-packages:

- :mod:`a2x_registry.backend` — FastAPI registry and relay backend
- :mod:`a2x_registry.register` — service registration business logic
- :mod:`a2x_registry.relay` — A2A request forwarding
- :mod:`a2x_registry.tunnel` — optional WebSocket tunnel
- :mod:`a2x_registry.tcp_tunnel` — optional reverse TCP tunnel (port forwarding)
- :mod:`a2x_registry.artifact_relay` — resumable artifact transfer
- :mod:`a2x_registry.stream_proxy` — standalone binary stream proxy
- :mod:`a2x_registry.common` — shared infrastructure utilities

Runtime data (``database/``) is resolved via
:func:`a2x_registry.common.paths.get_home` — set the ``A2X_REGISTRY_HOME``
environment variable to pick a location, otherwise the library looks for
that resource in the current working directory, falling back to
``~/.a2x_registry/``.
"""

__version__ = "0.3.3"
