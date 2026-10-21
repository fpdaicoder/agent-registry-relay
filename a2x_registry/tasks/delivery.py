"""Subscription fan-out for the personal task-continuation data plane.

Same shape as ``groupchat/delivery.py``: a single consumer — the event loop —
serializes mutation of the connection registry, so there is no lock to hold and
no lock-ordering to get wrong. The two properties stay load-bearing:

* A slow subscriber never blocks anyone: each connection has a bounded queue,
  and when it fills the frame is dropped for that connection only.
* Delivery is best-effort. The pull path is the correctness mechanism; this
  Hub is only an accelerator (and the one that carries ``lease_lost``, the
  real-time "you have been fenced off" signal — still best-effort, because a
  device's next write being rejected by the fence is what actually matters).

The only structural difference from group chat is the key: one task, not one
group, and the fan-out target is a *task's* lease holder rather than a
*group's* members.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)

DEFAULT_QUEUE_SIZE = 256


@dataclass
class Subscriber:
    connection_id: str
    principal_id: str
    tasks: set[int] = field(default_factory=set)
    queue: asyncio.Queue = field(
        default_factory=lambda: asyncio.Queue(maxsize=DEFAULT_QUEUE_SIZE)
    )
    dropped: int = 0
    closed: bool = False

    def offer(self, frame: dict[str, Any]) -> bool:
        if self.closed:
            return False
        try:
            self.queue.put_nowait(frame)
            return True
        except asyncio.QueueFull:
            self.dropped += 1
            return False


class Hub:
    """Registry of live subscriptions plus the fan-out path."""

    def __init__(self) -> None:
        self._by_connection: dict[str, Subscriber] = {}
        self._by_task: dict[int, set[str]] = {}
        self.metrics = {
            "published": 0,
            "delivered": 0,
            "dropped": 0,
            "connections": 0,
        }

    def register(self, connection_id: str, principal_id: str) -> Subscriber:
        subscriber = Subscriber(connection_id=connection_id, principal_id=principal_id)
        self._by_connection[connection_id] = subscriber
        self.metrics["connections"] = len(self._by_connection)
        return subscriber

    def unregister(self, connection_id: str) -> None:
        subscriber = self._by_connection.pop(connection_id, None)
        if subscriber is None:
            return
        subscriber.closed = True
        for task_id in list(subscriber.tasks):
            self._detach(subscriber, task_id)
        self.metrics["connections"] = len(self._by_connection)

    def subscribe(self, subscriber: Subscriber, task_id: int) -> None:
        subscriber.tasks.add(task_id)
        self._by_task.setdefault(task_id, set()).add(subscriber.connection_id)

    def unsubscribe(self, subscriber: Subscriber, task_id: int) -> None:
        self._detach(subscriber, task_id)

    def drop_principal(self, principal_id: str) -> int:
        """Remove every connection belonging to one principal. Returns count."""
        victims = [
            connection_id
            for connection_id, subscriber in self._by_connection.items()
            if subscriber.principal_id == principal_id
        ]
        for connection_id in victims:
            self.unregister(connection_id)
        return len(victims)

    def _detach(self, subscriber: Subscriber, task_id: int) -> None:
        subscriber.tasks.discard(task_id)
        members = self._by_task.get(task_id)
        if members is None:
            return
        members.discard(subscriber.connection_id)
        if not members:
            del self._by_task[task_id]

    def publish(
        self,
        task_id: int,
        frame: dict[str, Any],
        *,
        only: Optional[frozenset[str]] = None,
        exclude: Optional[str] = None,
    ) -> int:
        """Offer ``frame`` to every subscriber of ``task_id``.

        Returns the number of subscribers that actually accepted the frame.
        """
        self.metrics["published"] += 1
        connection_ids = self._by_task.get(task_id)
        if not connection_ids:
            return 0
        delivered = 0
        for connection_id in list(connection_ids):
            subscriber = self._by_connection.get(connection_id)
            if subscriber is None:
                continue
            if exclude is not None and connection_id == exclude:
                continue
            if only is not None and subscriber.principal_id not in only:
                continue
            if subscriber.offer(frame):
                delivered += 1
            else:
                self.metrics["dropped"] += 1
                logger.debug(
                    "tasks slow subscriber: principal=%s conn=%s task=%s",
                    subscriber.principal_id,
                    connection_id,
                    task_id,
                )
        self.metrics["delivered"] += delivered
        return delivered

    def subscriber_count(self, task_id: int) -> int:
        return len(self._by_task.get(task_id, ()))

    def status(self) -> dict[str, Any]:
        return {
            "connections": len(self._by_connection),
            "tasks": len(self._by_task),
            "published": self.metrics["published"],
            "delivered": self.metrics["delivered"],
        }