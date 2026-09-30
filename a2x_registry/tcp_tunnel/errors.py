"""Reverse TCP tunnel errors."""


class TcpTunnelError(Exception):
    """A reverse TCP tunnel operation failed."""

    def __init__(self, message: str, code: str = "tcp_tunnel_unavailable") -> None:
        super().__init__(message)
        self.message = message
        self.code = code
