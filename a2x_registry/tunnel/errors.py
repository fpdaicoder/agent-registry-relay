"""WebSocket tunnel errors used by the Relay integration."""


class TunnelForwardError(Exception):
    """A mapped tunnel target could not complete a forwarded request."""

    def __init__(self, message: str, code: str = "tunnel_unavailable") -> None:
        super().__init__(message)
        self.code = code
