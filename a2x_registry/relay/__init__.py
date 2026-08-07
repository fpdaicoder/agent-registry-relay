"""Optional A2A relay data plane.

The relay is disabled by default and deliberately kept outside
``RegistryService`` so registration/discovery remain independent of message
traffic.
"""

from .config import RelayConfig
from .errors import RelayError
from .service import RelayService

__all__ = ["RelayConfig", "RelayError", "RelayService"]
