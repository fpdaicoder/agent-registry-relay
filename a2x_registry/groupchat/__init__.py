"""Optional group chat data plane (multi-Agent group conversations).

Design: ``docs/groupchat_design.md``. The module is off unless
``A2X_GROUPCHAT_ENABLED=true``; when off, every route returns a structured
404 the same way the relay and tunnel modules do.

Layout mirrors the rest of the repository: ``router`` for the HTTP/WS
surface, ``service`` for business rules, ``sqlstore`` for persistence, and
``delivery`` for the live-subscription fan-out.
"""

from .config import GroupChatConfig
from .errors import GroupChatError
from .models import Group, Invite, Membership, Message, Page
from .service import GroupChatService

__all__ = [
    "GroupChatConfig",
    "GroupChatError",
    "GroupChatService",
    "Group",
    "Invite",
    "Membership",
    "Message",
    "Page",
]
