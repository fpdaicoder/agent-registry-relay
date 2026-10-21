"""Optional personal task-continuation data plane (cross-device Agent tasks).

Design: ``docs/tasks_design.md``. The module is off unless
``A2X_TASKS_ENABLED=true``; when off, every route returns a structured 404 the
same way the relay, tunnel and group chat modules do.

Layout mirrors the rest of the repository and ``groupchat/`` in particular:
``router`` for the HTTP/WS surface, ``service`` for business rules and the
fencing-token orchestration, ``sqlstore`` for persistence, and ``delivery`` for
the live-subscription fan-out (``lease_lost`` / ``lease_preempt``).
"""

from .config import TasksConfig
from .errors import TaskError
from .models import Task, TaskCursor, TaskMessage, TaskPage
from .service import TasksService

__all__ = [
    "TasksConfig",
    "TaskError",
    "TasksService",
    "Task",
    "TaskCursor",
    "TaskMessage",
    "TaskPage",
]