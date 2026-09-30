"""Subscription fan-out for the group chat data plane.

The Hub owns the set of live WebSocket subscriptions and fans messages out to
them. It follows the same shape as the canonical (Go) Hub pattern: a single
consumer — not a mutex — serializes mutation of the connection registry, so
there is no lock to hold correctly and no lock-ordering to get wrong.

Two properties are load-bearing and are enforced here rather than left to
callers:

* **A slow subscriber never blocks anyone.** Each connection has a bounded
  queue; when it fills, the frame is dropped for that connection only. The
  subscriber recovers the gap by pulling ``after_seq`` — which is why the
  pull path is the correctness mechanism and this Hub is only an accelerator.
* **A published frame is never re-delivered to the sender's own connection.**
  Delivery is best-effort, so duplicates would be harmless but wasteful; the
  sender skips its own connection for the same reason it skips its own ack.

Unsubscribing has a security consequence: a connection that keeps its group
subscription after being removed from the group would keep receiving that
group's messages. :meth:`Hub.unsubscribe` and :meth:`Hub.drop_principal` are
therefore part of the membership-mutation path, not an optimization.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Bounded so that a wedged connection cannot grow without limit. Sized well
# above typical burst traffic; the recovery path is the pull endpoint, so
# dropping here costs latency, not correctness.
DEFAULT_QUEUE_SIZE = 256


@dataclass
class Subscriber:
    connection_id: str
    principal_id: str
    groups: set[int] = field(default_factory=set)
    queue: asyncio.Queue = field(
        default_factory=lambda: asyncio.Queue(maxsize=DEFAULT_QUEUE_SIZE)
    )
    dropped: int = 0
    closed: bool = False

    def offer(self, frame: dict[str, Any]) -> bool:
        """Enqueue without blocking. False means the frame was dropped."""
        if self.closed:
            return False
        try:
            self.queue.put_nowait(frame)
            return True
        except asyncio.QueueFull:
            self.dropped += 1
            return False


class Hub:
    """Registry of live subscriptions plus the fan-out path.

    All mutating methods are synchronous: they are called from the event loop
    that owns the subscriber queues (FastAPI's), and the queue itself provides
    the ordering. This keeps the call sites — the message send path and the
    membership mutation path — free of awaits that could be held across a
    transaction or a database call.
    """

    def __init__(self) -> None:
        self._by_connection: dict[str, Subscriber] = {}
        self._by_group: dict[int, set[str]] = {}
        self.metrics = {
            "published": 0,
            "delivered": 0,
            "dropped": 0,
            "connections": 0,
        }

    # ── Registration ─────────────────────────────────────────────────────

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
        for group_id in list(subscriber.groups):
            self._detach(subscriber, group_id)
        self.metrics["connections"] = len(self._by_connection)

    def subscribe(self, subscriber: Subscriber, group_id: int) -> None:
        subscriber.groups.add(group_id)
        self._by_group.setdefault(group_id, set()).add(subscriber.connection_id)

    def unsubscribe(self, subscriber: Subscriber, group_id: int) -> None:
        """Drop one group subscription.

        Must be called when a member leaves or is removed; otherwise the
        connection keeps receiving that group's messages.
        """
        self._detach(subscriber, group_id)

    def drop_principal(self, principal_id: str) -> int:
        """Remove every connection belonging to one principal.

        Returns the number of connections dropped. Used by kick/ban — the
        target's connection may live on this process while the mutation was
        executed here, and it must stop receiving immediately.
        """
        victims = [
            connection_id
            for connection_id, subscriber in self._by_connection.items()
            if subscriber.principal_id == principal_id
        ]
        for connection_id in victims:
            self.unregister(connection_id)
        return len(victims)

    def _detach(self, subscriber: Subscriber, group_id: int) -> None:
        subscriber.groups.discard(group_id)
        members = self._by_group.get(group_id)
        if members is None:
            return
        members.discard(subscriber.connection_id)
        if not members:
            del self._by_group[group_id]

    # ── Fan-out ──────────────────────────────────────────────────────────

    def publish(
        self,
        group_id: int,
        frame: dict[str, Any],
        *,
        only: Optional[frozenset[str]] = None,
        exclude: Optional[str] = None,
    ) -> int:
        """Offer ``frame`` to every subscriber of ``group_id``.

        ``only`` restricts delivery to a set of principals (used for
        ``mentions`` so an @-targeted wakeup does not fan out to the whole
        group). ``exclude`` skips one connection, normally the sender's.

        Returns the number of subscribers that actually accepted the frame.
        """
        self.metrics["published"] += 1
        connection_ids = self._by_group.get(group_id)
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
                    "groupchat slow subscriber: principal=%s conn=%s group=%s",
                    subscriber.principal_id,
                    connection_id,
                    group_id,
                )
        self.metrics["delivered"] += delivered
        return delivered

    def subscriber_count(self, group_id: int) -> int:
        return len(self._by_group.get(group_id, ()))

    def status(self) -> dict[str, Any]:
        return {
            "connections": len(self._by_connection),
            "groups": len(self._by_group),
            "published": self.metrics["published"],
            "delivered": self.metrics["delivered"],
        }
