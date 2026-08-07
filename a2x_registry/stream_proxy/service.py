"""In-memory stream-session state and binary forwarding rules."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import secrets
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

from .config import StreamProxyConfig


Role = Literal["sender", "receiver"]
TERMINAL_STATES = {"completed", "failed", "canceled", "expired"}


class StreamProxyError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def _token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@dataclass
class StreamSession:
    transfer_id: str
    filename: str
    byte_length: int
    sha256: str
    sender_token_sha256: str
    receiver_token_sha256: str
    created_at: float
    expires_at: float
    state: str = "created"
    sender_socket: Any | None = field(default=None, repr=False)
    receiver_socket: Any | None = field(default=None, repr=False)
    sender_send_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        repr=False,
    )
    receiver_send_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        repr=False,
    )
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    next_offset: int = 0
    acked_offset: int = 0
    sender_connected_at: float | None = None
    receiver_connected_at: float | None = None
    paired_at: float | None = None
    completed_at: float | None = None
    last_progress_at: float | None = None

    def public_status(self) -> dict[str, Any]:
        return {
            "transferId": self.transfer_id,
            "filename": self.filename,
            "byteLength": self.byte_length,
            "sha256": self.sha256,
            "state": self.state,
            "senderConnected": self.sender_socket is not None,
            "receiverConnected": self.receiver_socket is not None,
            "nextOffset": self.next_offset,
            "ackedOffset": self.acked_offset,
            "createdAtEpoch": self.created_at,
            "expiresAtEpoch": self.expires_at,
            "pairedAtEpoch": self.paired_at,
            "completedAtEpoch": self.completed_at,
        }


class StreamProxyService:
    def __init__(self, config: StreamProxyConfig):
        config.validate()
        self.config = config
        self.sessions: dict[str, StreamSession] = {}
        self._cleanup_task: asyncio.Task[None] | None = None
        self._sessions_lock = asyncio.Lock()
        self.metrics = {
            "createdSessions": 0,
            "pairedSessions": 0,
            "completedSessions": 0,
            "failedSessions": 0,
            "canceledSessions": 0,
            "expiredSessions": 0,
            "ingressBytes": 0,
            "egressBytes": 0,
            "resumedConnections": 0,
            "protocolErrors": 0,
        }

    async def start(self) -> None:
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        task, self._cleanup_task = self._cleanup_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        for session in list(self.sessions.values()):
            await self._close_session_sockets(
                session,
                code=1012,
                reason="stream proxy stopped",
            )

    def require_create_token(self, token: str | None) -> None:
        if (
            not isinstance(token, str)
            or not hmac.compare_digest(token, self.config.create_token)
        ):
            raise StreamProxyError(
                "stream_create_token_invalid",
                "Invalid stream proxy create token",
            )

    async def create_session(
        self,
        *,
        filename: str,
        byte_length: int,
        sha256: str,
        ttl_seconds: int | None,
    ) -> dict[str, Any]:
        if (
            isinstance(byte_length, bool)
            or byte_length <= 0
            or byte_length > self.config.max_object_bytes
        ):
            raise StreamProxyError(
                "stream_size_invalid",
                "byteLength must be within the configured object limit",
            )
        if (
            not isinstance(sha256, str)
            or len(sha256) != 64
            or any(character not in "0123456789abcdef" for character in sha256)
        ):
            raise StreamProxyError(
                "stream_sha256_invalid",
                "sha256 must be 64 lowercase hexadecimal characters",
            )
        ttl = self.config.session_ttl_seconds if ttl_seconds is None else ttl_seconds
        if (
            isinstance(ttl, bool)
            or ttl <= 0
            or ttl > self.config.session_ttl_seconds
        ):
            raise StreamProxyError(
                "stream_ttl_invalid",
                "ttlSeconds must be within the configured limit",
            )
        async with self._sessions_lock:
            await self._expire_sessions_locked()
            if len(self.sessions) >= self.config.max_sessions:
                raise StreamProxyError(
                    "stream_capacity_exceeded",
                    "Stream proxy session capacity is exhausted",
                )
            transfer_id = uuid.uuid4().hex
            sender_token = secrets.token_urlsafe(32)
            receiver_token = secrets.token_urlsafe(32)
            now = time.time()
            session = StreamSession(
                transfer_id=transfer_id,
                filename=filename.rsplit("/", 1)[-1][:255] or "artifact.bin",
                byte_length=byte_length,
                sha256=sha256,
                sender_token_sha256=_token_digest(sender_token),
                receiver_token_sha256=_token_digest(receiver_token),
                created_at=now,
                expires_at=now + ttl,
            )
            self.sessions[transfer_id] = session
            self.metrics["createdSessions"] += 1
        base = self.config.public_ws_base_url
        return {
            "transferId": transfer_id,
            "state": session.state,
            "filename": session.filename,
            "byteLength": byte_length,
            "sha256": sha256,
            "chunkBytes": self.config.max_chunk_bytes,
            "expiresAtEpoch": session.expires_at,
            "sender": {
                "url": f"{base}/v1/stream/{transfer_id}/sender",
                "token": sender_token,
            },
            "receiver": {
                "url": f"{base}/v1/stream/{transfer_id}/receiver",
                "token": receiver_token,
            },
        }

    def get_session(self, transfer_id: str) -> StreamSession:
        session = self.sessions.get(transfer_id)
        if session is None:
            raise StreamProxyError(
                "stream_session_not_found",
                "Stream proxy session was not found",
            )
        if session.expires_at <= time.time():
            raise StreamProxyError(
                "stream_session_expired",
                "Stream proxy session has expired",
            )
        return session

    @staticmethod
    def authenticate(session: StreamSession, role: Role, token: str) -> None:
        expected = (
            session.sender_token_sha256
            if role == "sender"
            else session.receiver_token_sha256
        )
        if not token or not hmac.compare_digest(_token_digest(token), expected):
            raise StreamProxyError(
                "stream_token_invalid",
                f"Invalid {role} token",
            )

    async def attach(
        self,
        session: StreamSession,
        role: Role,
        socket: Any,
        *,
        resume_offset: int,
    ) -> None:
        async with session.state_lock:
            if session.state in TERMINAL_STATES:
                raise StreamProxyError(
                    "stream_session_terminal",
                    f"Session is already {session.state}",
                )
            attribute = f"{role}_socket"
            if getattr(session, attribute) is not None:
                raise StreamProxyError(
                    "stream_role_in_use",
                    f"Session already has an active {role}",
                )
            if role == "receiver":
                if (
                    isinstance(resume_offset, bool)
                    or resume_offset < 0
                    or resume_offset > session.next_offset
                ):
                    raise StreamProxyError(
                        "stream_resume_offset_invalid",
                        "Receiver resumeOffset exceeds forwarded data",
                    )
                if resume_offset < session.acked_offset:
                    self.metrics["resumedConnections"] += 1
                session.next_offset = resume_offset
                session.acked_offset = resume_offset
            elif resume_offset != 0:
                raise StreamProxyError(
                    "stream_resume_offset_invalid",
                    "Sender must not declare resumeOffset",
                )
            setattr(session, attribute, socket)
            setattr(session, f"{role}_connected_at", time.time())
            if session.sender_socket is not None and session.receiver_socket is not None:
                if session.paired_at is None:
                    session.paired_at = time.time()
                    self.metrics["pairedSessions"] += 1
                session.state = (
                    "paired" if session.next_offset == 0 else "streaming"
                )

    async def announce_pair(self, session: StreamSession) -> None:
        async with session.state_lock:
            paired = (
                session.sender_socket is not None
                and session.receiver_socket is not None
            )
            offset = session.next_offset
        if not paired:
            return
        await self.send_control(
            session,
            "receiver",
            {
                "type": "paired",
                "transferId": session.transfer_id,
                "offset": offset,
            },
        )
        await self.send_control(
            session,
            "sender",
            {
                "type": "ready",
                "transferId": session.transfer_id,
                "resumeOffset": offset,
                "chunkBytes": self.config.max_chunk_bytes,
            },
        )

    async def send_control(
        self,
        session: StreamSession,
        role: Role,
        payload: dict[str, Any],
    ) -> None:
        socket = getattr(session, f"{role}_socket")
        if socket is None:
            raise StreamProxyError(
                "stream_peer_unavailable",
                f"{role} is not connected",
            )
        lock = getattr(session, f"{role}_send_lock")
        async with lock:
            await socket.send_json(payload)

    async def forward_data(
        self,
        session: StreamSession,
        sender_socket: Any,
        frame: bytes,
    ) -> None:
        if len(frame) <= 8:
            raise StreamProxyError(
                "stream_frame_invalid",
                "Binary frame must contain an offset and payload",
            )
        payload_length = len(frame) - 8
        if payload_length > self.config.max_chunk_bytes:
            raise StreamProxyError(
                "stream_chunk_too_large",
                "Binary frame exceeds the configured chunk limit",
            )
        offset = int.from_bytes(frame[:8], "big", signed=False)
        async with session.state_lock:
            if session.sender_socket is not sender_socket:
                raise StreamProxyError(
                    "stream_sender_mismatch",
                    "Binary frame did not come from the active sender",
                )
            if session.receiver_socket is None:
                raise StreamProxyError(
                    "stream_peer_unavailable",
                    "Receiver is not connected",
                )
            if offset != session.next_offset:
                raise StreamProxyError(
                    "stream_offset_mismatch",
                    f"Expected offset {session.next_offset}, received {offset}",
                )
            if offset + payload_length > session.byte_length:
                raise StreamProxyError(
                    "stream_length_exceeded",
                    "Binary frame exceeds the declared file length",
                )
            receiver_socket = session.receiver_socket
        async with session.receiver_send_lock:
            await receiver_socket.send_bytes(frame)
        async with session.state_lock:
            if session.receiver_socket is not receiver_socket:
                raise StreamProxyError(
                    "stream_peer_changed",
                    "Receiver changed while forwarding a data frame",
                )
            session.next_offset += payload_length
            session.state = "streaming"
            session.last_progress_at = time.time()
            self.metrics["ingressBytes"] += payload_length
            self.metrics["egressBytes"] += payload_length

    async def acknowledge(
        self,
        session: StreamSession,
        receiver_socket: Any,
        offset: int,
    ) -> None:
        async with session.state_lock:
            if session.receiver_socket is not receiver_socket:
                raise StreamProxyError(
                    "stream_receiver_mismatch",
                    "ACK did not come from the active receiver",
                )
            if (
                isinstance(offset, bool)
                or offset < session.acked_offset
                or offset > session.next_offset
            ):
                raise StreamProxyError(
                    "stream_ack_invalid",
                    "ACK offset is outside the forwarded range",
                )
            session.acked_offset = offset
            session.last_progress_at = time.time()
        await self.send_control(
            session,
            "sender",
            {"type": "ack", "offset": offset},
        )

    async def finish(
        self,
        session: StreamSession,
        sender_socket: Any,
        payload: dict[str, Any],
    ) -> None:
        async with session.state_lock:
            if session.sender_socket is not sender_socket:
                raise StreamProxyError(
                    "stream_sender_mismatch",
                    "FIN did not come from the active sender",
                )
            if (
                session.next_offset != session.byte_length
                or payload.get("byteLength") != session.byte_length
                or payload.get("sha256") != session.sha256
            ):
                raise StreamProxyError(
                    "stream_finish_invalid",
                    "FIN does not match the declared file",
                )
            session.state = "verifying"
        await self.send_control(
            session,
            "receiver",
            {
                "type": "fin",
                "byteLength": session.byte_length,
                "sha256": session.sha256,
            },
        )

    async def complete(
        self,
        session: StreamSession,
        receiver_socket: Any,
        payload: dict[str, Any],
    ) -> None:
        async with session.state_lock:
            if session.receiver_socket is not receiver_socket:
                raise StreamProxyError(
                    "stream_receiver_mismatch",
                    "COMPLETE did not come from the active receiver",
                )
            if (
                session.acked_offset != session.byte_length
                or payload.get("byteLength") != session.byte_length
                or payload.get("sha256") != session.sha256
            ):
                raise StreamProxyError(
                    "stream_complete_invalid",
                    "COMPLETE does not match the declared file",
                )
            session.state = "completed"
            session.completed_at = time.time()
            self.metrics["completedSessions"] += 1
        await self.send_control(
            session,
            "sender",
            {
                "type": "complete",
                "byteLength": session.byte_length,
                "sha256": session.sha256,
            },
        )

    async def detach(
        self,
        session: StreamSession,
        role: Role,
        socket: Any,
    ) -> None:
        peer_role: Role = "receiver" if role == "sender" else "sender"
        notify_peer = False
        async with session.state_lock:
            attribute = f"{role}_socket"
            if getattr(session, attribute) is socket:
                setattr(session, attribute, None)
                if session.state not in TERMINAL_STATES:
                    notify_peer = (
                        getattr(session, f"{peer_role}_socket") is not None
                    )
                    session.state = "suspended"
        if notify_peer:
            try:
                await self.send_control(
                    session,
                    peer_role,
                    {"type": "peer_disconnected", "role": role},
                )
            except Exception:
                pass

    async def fail_session(
        self,
        session: StreamSession,
        *,
        code: str,
        message: str,
    ) -> None:
        async with session.state_lock:
            if session.state in TERMINAL_STATES:
                return
            session.state = "failed"
            self.metrics["failedSessions"] += 1
        for role in ("sender", "receiver"):
            try:
                await self.send_control(
                    session,
                    role,
                    {"type": "error", "code": code, "message": message},
                )
            except Exception:
                pass
        await self._close_session_sockets(
            session,
            code=1011,
            reason=code[:120],
        )

    async def cancel_session(self, session: StreamSession) -> None:
        async with session.state_lock:
            if session.state in TERMINAL_STATES:
                return
            session.state = "canceled"
            self.metrics["canceledSessions"] += 1
        await self._close_session_sockets(
            session,
            code=1000,
            reason="session canceled",
        )

    async def status(self) -> dict[str, Any]:
        states: dict[str, int] = {}
        for session in self.sessions.values():
            states[session.state] = states.get(session.state, 0) + 1
        return {
            "enabled": True,
            "mode": "in-memory-binary-websocket-proxy",
            "strictP2P": False,
            "maxObjectBytes": self.config.max_object_bytes,
            "maxChunkBytes": self.config.max_chunk_bytes,
            "maxSessions": self.config.max_sessions,
            "activeSessions": sum(
                count
                for state, count in states.items()
                if state not in TERMINAL_STATES
            ),
            "states": states,
            **self.metrics,
        }

    async def cleanup_expired(self) -> int:
        async with self._sessions_lock:
            return await self._expire_sessions_locked()

    async def _expire_sessions_locked(self) -> int:
        now = time.time()
        expired: list[StreamSession] = []
        for transfer_id, session in list(self.sessions.items()):
            if session.expires_at > now:
                continue
            self.sessions.pop(transfer_id, None)
            expired.append(session)
        for session in expired:
            async with session.state_lock:
                if session.state not in TERMINAL_STATES:
                    session.state = "expired"
                    self.metrics["expiredSessions"] += 1
            await self._close_session_sockets(
                session,
                code=1008,
                reason="session expired",
            )
        return len(expired)

    async def _cleanup_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.config.cleanup_interval_seconds)
                await self.cleanup_expired()
        except asyncio.CancelledError:
            pass

    @staticmethod
    async def _close_session_sockets(
        session: StreamSession,
        *,
        code: int,
        reason: str,
    ) -> None:
        sockets = [session.sender_socket, session.receiver_socket]
        session.sender_socket = None
        session.receiver_socket = None
        for socket in sockets:
            if socket is None:
                continue
            try:
                await socket.close(code=code, reason=reason)
            except Exception:
                pass
