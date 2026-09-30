"""WebSocket tunnel compatible with the existing device client protocol."""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import math
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

import websockets
from websockets.exceptions import ConnectionClosed

from .config import TunnelConfig
from .errors import TunnelForwardError


logger = logging.getLogger(__name__)
_DEVICE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


@dataclass
class _PendingRequest:
    source_ws: Any | None
    target_ws: Any
    source_device: str
    target_device: str
    source_message_id: Any
    response_future: asyncio.Future[dict[str, Any]] | None = None
    timeout_task: asyncio.Task[None] | None = None


class WebSocketTunnelServer:
    """Maintain device connections and forward client-compatible messages."""

    def __init__(
        self,
        config: TunnelConfig,
        registry_getter: Callable[[], Any] | None = None,
    ):
        self.config = config
        self._registry_getter = registry_getter
        self._server: Any = None
        self._devices: dict[str, Any] = {}
        self._last_active: dict[str, float] = {}
        self._pending: dict[str, _PendingRequest] = {}
        self._service_bindings: dict[tuple[str, str], str] = {}
        self._device_bindings: dict[str, set[tuple[str, str]]] = {}

    @property
    def connected_devices(self) -> int:
        return len(self._devices)

    @property
    def pending_requests(self) -> int:
        return len(self._pending)

    @property
    def bound_services(self) -> int:
        return len(self._service_bindings)

    @property
    def bound_port(self) -> int:
        if self._server is None or not self._server.sockets:
            return self.config.port
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        if self._server is not None:
            return
        self._server = await websockets.serve(
            self.handle_connection,
            self.config.host,
            self.config.port,
            max_size=self.config.max_message_bytes,
            ping_interval=None,
        )
        logger.info(
            "WebSocket tunnel listening on ws://%s:%s",
            self.config.host,
            self.bound_port,
        )

    async def stop(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.close()

        clients = list({id(ws): ws for ws in self._devices.values()}.values())
        if clients:
            await asyncio.gather(
                *(ws.close(code=1001, reason="server shutdown") for ws in clients),
                return_exceptions=True,
            )
        if server is not None:
            await server.wait_closed()

        for internal_id, pending in list(self._pending.items()):
            self._pending.pop(internal_id, None)
            if pending.timeout_task is not None:
                pending.timeout_task.cancel()
            if pending.response_future is not None and not pending.response_future.done():
                pending.response_future.set_exception(
                    TunnelForwardError("WebSocket tunnel stopped")
                )
        self._devices.clear()
        self._last_active.clear()
        self._service_bindings.clear()
        self._device_bindings.clear()

    def status(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "host": self.config.host,
            "port": self.bound_port,
            "connected_devices": self.connected_devices,
            "bound_services": self.bound_services,
            "pending_requests": self.pending_requests,
        }

    def device_for_service(self, dataset: str, service_id: str) -> str | None:
        device_id = self._service_bindings.get((dataset, service_id))
        if device_id is None or device_id not in self._devices:
            return None
        return device_id

    def has_online_service(self, dataset: str, service_id: str) -> bool:
        return self.device_for_service(dataset, service_id) is not None

    async def forward_http(
        self,
        dataset: str,
        service_id: str,
        http_request: dict[str, Any],
        timeout: float,
    ) -> dict[str, Any]:
        device_id = self.device_for_service(dataset, service_id)
        if device_id is None:
            raise TunnelForwardError("Mapped WebSocket device is offline")
        target_ws = self._devices.get(device_id)
        if target_ws is None:
            raise TunnelForwardError("Mapped WebSocket device is offline")
        if len(self._pending) >= self.config.max_pending_requests:
            raise TunnelForwardError("WebSocket tunnel is busy", "tunnel_busy")

        timeout = min(
            max(float(timeout), 0.001),
            self.config.max_request_timeout_seconds,
        )
        internal_id = uuid.uuid4().hex
        response_future: asyncio.Future[dict[str, Any]] = (
            asyncio.get_running_loop().create_future()
        )
        pending = _PendingRequest(
            source_ws=None,
            target_ws=target_ws,
            source_device="registry-relay",
            target_device=device_id,
            source_message_id=internal_id,
            response_future=response_future,
        )
        self._pending[internal_id] = pending
        pending.timeout_task = asyncio.create_task(
            self._request_timeout(internal_id, timeout)
        )
        try:
            await self._send_json(
                target_ws,
                {
                    "type": "forward_request",
                    "message_id": internal_id,
                    "source_device": "registry-relay",
                    "target_device": device_id,
                    "dataset": dataset,
                    "service_id": service_id,
                    "http_request": http_request,
                    "timeout": timeout,
                },
            )
        except ConnectionClosed as exc:
            removed = self._remove_pending(internal_id)
            if (
                removed is not None
                and removed.response_future is not None
                and not removed.response_future.done()
            ):
                removed.response_future.cancel()
            raise TunnelForwardError("Mapped WebSocket device disconnected") from exc
        try:
            return await response_future
        except asyncio.CancelledError:
            removed = self._remove_pending(internal_id)
            if (
                removed is not None
                and removed.response_future is not None
                and not removed.response_future.done()
            ):
                removed.response_future.cancel()
            raise

    async def handle_connection(self, websocket: Any) -> None:
        device_id: str | None = None
        heartbeat_task: asyncio.Task[None] | None = None
        remote_address = getattr(websocket, "remote_address", None)
        try:
            logger.info("WebSocket tunnel connection from %s", remote_address)
            register_message = await asyncio.wait_for(
                websocket.recv(),
                timeout=self.config.register_timeout_seconds,
            )
            register_data = self._decode_message(register_message)
            device_id = self._validate_registration(register_data)
            bindings, registry_status = await self._prepare_bindings(
                device_id,
                register_data,
            )

            if (
                device_id not in self._devices
                and len(self._devices) >= self.config.max_devices
            ):
                await self._send_error(websocket, None, "device limit reached")
                await websocket.close(code=1013, reason="device limit reached")
                return

            old_websocket = self._devices.get(device_id)
            self._devices[device_id] = websocket
            self._last_active[device_id] = asyncio.get_running_loop().time()
            self._replace_device_bindings(device_id, bindings)
            if old_websocket is not None and old_websocket is not websocket:
                logger.warning(
                    "Device %s already connected; replacing the old connection",
                    device_id,
                )
                # The original relay-server.py used a normal WebSocket close
                # here. Keep that wire behavior because legacy device clients
                # treat private close codes such as 4001 as fatal.
                await old_websocket.close()

            registered: dict[str, Any] = {
                "type": "registered",
                "device_id": device_id,
                # Preserve relay-server.py's timestamp representation.
                "timestamp": datetime.now().isoformat(),
            }
            # A connection-only registration must retain the exact shape used
            # by relay-server.py. New fields are added only for clients that
            # explicitly request a registry service binding.
            if bindings:
                registered_bindings = [
                    {"dataset": dataset, "service_id": service_id}
                    for dataset, service_id in sorted(bindings)
                ]
                registered["binding"] = registered_bindings[0]
                if len(registered_bindings) > 1:
                    registered["bindings"] = registered_bindings
                registered["registry_status"] = registry_status
            await self._send_json(websocket, registered)
            logger.info(
                "WebSocket tunnel device registered: %s from %s",
                device_id,
                remote_address,
            )
            heartbeat_task = asyncio.create_task(
                self._heartbeat(websocket, device_id)
            )

            async for message in websocket:
                if self._devices.get(device_id) is not websocket:
                    break
                self._last_active[device_id] = asyncio.get_running_loop().time()
                try:
                    data = self._decode_message(message)
                    await self._dispatch(websocket, device_id, data)
                except json.JSONDecodeError:
                    # relay-server.py ignored malformed post-registration
                    # frames and kept the connection alive.
                    logger.warning("Invalid JSON from tunnel device %s", device_id)
                except (TypeError, ValueError) as exc:
                    await self._send_error(websocket, None, str(exc))
        except asyncio.TimeoutError:
            await self._send_error(websocket, None, "registration timeout")
            await websocket.close(code=4008, reason="registration timeout")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            await self._send_error(websocket, None, str(exc))
            # Match relay-server.py: after returning the handler, the
            # WebSocket closes normally instead of using a private error code.
            await websocket.close()
        except ConnectionClosed as exc:
            logger.warning(
                "WebSocket tunnel connection closed: %s code=%s reason=%s",
                remote_address,
                exc.code,
                exc.reason,
            )
        except Exception:
            logger.exception("WebSocket tunnel connection failed")
            try:
                await self._send_error(websocket, None, "registration failed")
                await websocket.close(code=1011, reason="registration failed")
            except ConnectionClosed:
                pass
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
            if device_id is not None:
                await self._disconnect(websocket, device_id)
                logger.info("WebSocket tunnel device unregistered: %s", device_id)

    def _validate_registration(self, data: dict[str, Any]) -> str:
        if data.get("type") != "register":
            raise ValueError("First message must be register")
        device_id = data.get("device_id")
        if device_id is None or device_id == "":
            raise ValueError("Missing device_id")
        if not isinstance(device_id, str) or not _DEVICE_ID_RE.fullmatch(device_id):
            raise ValueError("invalid device_id")
        if self.config.shared_token:
            token = data.get("token")
            if not isinstance(token, str) or not hmac.compare_digest(
                token,
                self.config.shared_token,
            ):
                raise ValueError("invalid tunnel token")
        return device_id

    async def _prepare_bindings(
        self,
        device_id: str,
        data: dict[str, Any],
    ) -> tuple[set[tuple[str, str]], str]:
        try:
            return await self._prepare_bindings_inner(device_id, data)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            # handle_connection deliberately returns an error frame without
            # logging the reason, so record it here for diagnosis.
            logger.warning(
                "WebSocket tunnel binding failed for device %s: %s",
                device_id,
                exc,
            )
            raise

    async def _prepare_bindings_inner(
        self,
        device_id: str,
        data: dict[str, Any],
    ) -> tuple[set[tuple[str, str]], str]:
        dataset = data.get("dataset")
        service_id = data.get("service_id")
        agent_card = data.get("agent_card")
        if dataset is None and service_id is None and agent_card is None:
            if (
                not self.config.auto_bind_registered_services
                or self._registry_getter is None
            ):
                return set(), "connection-only"
            bindings = await asyncio.to_thread(
                self._find_registered_bindings,
                device_id,
            )
            return bindings, "auto-bound" if bindings else "connection-only"
        if (
            not isinstance(dataset, str)
            or not _DEVICE_ID_RE.fullmatch(dataset)
            or not isinstance(service_id, str)
            or not _DEVICE_ID_RE.fullmatch(service_id)
        ):
            raise ValueError("dataset and service_id are required for service mapping")
        if self._registry_getter is None:
            raise ValueError("registry mapping is unavailable")

        registry = self._registry_getter()
        if isinstance(agent_card, dict):
            from a2x_registry.register.models import AgentCard, RegisterA2ARequest

            card_data = dict(agent_card)
            metadata = card_data.get("metadata")
            metadata = dict(metadata) if isinstance(metadata, dict) else {}
            metadata["tunnelDeviceId"] = device_id
            card_data["metadata"] = metadata
            request = RegisterA2ARequest(
                dataset=dataset,
                service_id=service_id,
                agent_card=AgentCard(**card_data),
                persistent=bool(data.get("persistent", True)),
            )
            result = await asyncio.to_thread(registry.register_a2a, request)
            status = result.status
        else:
            entry = await asyncio.to_thread(
                registry.get_entry,
                dataset,
                service_id,
            )
            if entry is None or entry.type != "a2a" or entry.agent_card is None:
                raise ValueError("mapped A2A service is not registered")
            card_data = entry.agent_card.model_dump(exclude_none=True)
            metadata = card_data.get("metadata")
            mapped_device = (
                metadata.get("tunnelDeviceId")
                if isinstance(metadata, dict)
                else None
            )
            if mapped_device != device_id:
                raise ValueError("Agent Card tunnelDeviceId does not match device_id")
            status = "bound"
        return {(dataset, service_id)}, status

    def _find_registered_bindings(
        self,
        device_id: str,
    ) -> set[tuple[str, str]]:
        if self._registry_getter is None:
            return set()
        registry = self._registry_getter()
        result: set[tuple[str, str]] = set()
        for dataset in registry.list_datasets():
            for service in registry.list_services(dataset):
                if service.get("type") != "a2a":
                    continue
                service_id = service.get("id")
                card = service.get("metadata")
                metadata = card.get("metadata") if isinstance(card, dict) else None
                if (
                    isinstance(service_id, str)
                    and isinstance(metadata, dict)
                    and metadata.get("tunnelDeviceId") == device_id
                ):
                    result.add((dataset, service_id))
        return result

    def _replace_device_bindings(
        self,
        device_id: str,
        bindings: set[tuple[str, str]],
    ) -> None:
        for previous in self._device_bindings.pop(device_id, set()):
            if self._service_bindings.get(previous) == device_id:
                self._service_bindings.pop(previous, None)
        if not bindings:
            return
        for binding in bindings:
            previous_device = self._service_bindings.get(binding)
            if previous_device is not None and previous_device != device_id:
                previous_bindings = self._device_bindings.get(previous_device)
                if previous_bindings is not None:
                    previous_bindings.discard(binding)
            self._service_bindings[binding] = device_id
        self._device_bindings[device_id] = set(bindings)

    async def _dispatch(
        self,
        websocket: Any,
        device_id: str,
        data: dict[str, Any],
    ) -> None:
        message_type = data.get("type")
        if message_type == "forward_request":
            await self._handle_forward_request(websocket, device_id, data)
        elif message_type == "forward_response":
            await self._handle_forward_response(websocket, device_id, data)
        elif message_type == "discover_request":
            await self._handle_discover_request(websocket, data)
        elif message_type == "ping":
            await self._send_json(websocket, {"type": "pong"})
        elif message_type == "pong":
            return
        else:
            # relay-server.py logged and ignored unknown message types.
            logger.warning(
                "Unknown tunnel message type from %s: %s",
                device_id,
                message_type,
            )

    async def _handle_discover_request(
        self,
        websocket: Any,
        data: dict[str, Any],
    ) -> None:
        message_id = data.get("message_id")
        dataset = data.get("dataset")
        name = data.get("name")
        if message_id is None:
            await self._send_error(websocket, None, "missing message_id")
            return
        if not isinstance(dataset, str) or not _DEVICE_ID_RE.fullmatch(dataset):
            await self._send_error(websocket, message_id, "invalid dataset")
            return
        if name is not None and not isinstance(name, str):
            await self._send_error(websocket, message_id, "invalid name")
            return
        if self._registry_getter is None:
            await self._send_error(websocket, message_id, "registry is unavailable")
            return
        registry = self._registry_getter()
        services = await asyncio.to_thread(registry.list_services, dataset)
        result = []
        for service in services:
            if name and service.get("name") != name:
                continue
            item = dict(service)
            service_id = item.get("id")
            if isinstance(service_id, str):
                device_id = self.device_for_service(dataset, service_id)
                item["tunnel"] = {
                    "online": device_id is not None,
                    **({"deviceId": device_id} if device_id is not None else {}),
                }
            result.append(item)
        await self._send_json(
            websocket,
            {
                "type": "discover_response",
                "message_id": message_id,
                "dataset": dataset,
                "services": result,
            },
        )

    async def _handle_forward_request(
        self,
        source_ws: Any,
        source_device: str,
        data: dict[str, Any],
    ) -> None:
        source_message_id = data.get("message_id")
        target_device = data.get("target_device")
        http_request = data.get("http_request")
        if source_message_id is None:
            await self._send_error(source_ws, None, "missing message_id")
            return
        if not isinstance(target_device, str) or not _DEVICE_ID_RE.fullmatch(target_device):
            await self._send_error(source_ws, source_message_id, "invalid target_device")
            return
        if not isinstance(http_request, dict):
            await self._send_error(source_ws, source_message_id, "invalid http_request")
            return
        if len(self._pending) >= self.config.max_pending_requests:
            await self._send_error(source_ws, source_message_id, "pending request limit reached")
            return

        target_ws = self._devices.get(target_device)
        if target_ws is None:
            await self._send_error(
                source_ws,
                source_message_id,
                f"Device {target_device} not found",
            )
            return

        try:
            requested_timeout = data.get(
                "timeout",
                self.config.request_timeout_seconds,
            )
            if isinstance(requested_timeout, bool):
                raise ValueError
            timeout = float(requested_timeout)
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValueError
        except (TypeError, ValueError):
            await self._send_error(source_ws, source_message_id, "invalid timeout")
            return
        timeout = min(timeout, self.config.max_request_timeout_seconds)

        internal_id = uuid.uuid4().hex
        pending = _PendingRequest(
            source_ws=source_ws,
            target_ws=target_ws,
            source_device=source_device,
            target_device=target_device,
            source_message_id=source_message_id,
        )
        self._pending[internal_id] = pending
        pending.timeout_task = asyncio.create_task(
            self._request_timeout(internal_id, timeout)
        )
        forward_data = {
            "type": "forward_request",
            "message_id": internal_id,
            "source_device": source_device,
            "target_device": target_device,
            "http_request": http_request,
            "timeout": timeout,
        }
        try:
            await self._send_json(target_ws, forward_data)
        except ConnectionClosed:
            self._remove_pending(internal_id)
            await self._send_error(
                source_ws,
                source_message_id,
                f"Device {target_device} disconnected",
            )

    async def _handle_forward_response(
        self,
        responder_ws: Any,
        responder_device: str,
        data: dict[str, Any],
    ) -> None:
        internal_id = data.get("message_id")
        if not isinstance(internal_id, str):
            await self._send_error(responder_ws, internal_id, "invalid message_id")
            return
        pending = self._pending.get(internal_id)
        if pending is None:
            await self._send_error(responder_ws, internal_id, "request not found")
            return
        if (
            pending.target_ws is not responder_ws
            or pending.target_device != responder_device
        ):
            await self._send_error(responder_ws, internal_id, "response source mismatch")
            return

        self._remove_pending(internal_id)
        response = dict(data)
        response["message_id"] = pending.source_message_id
        if pending.response_future is not None:
            if not pending.response_future.done():
                pending.response_future.set_result(response)
            return
        try:
            await self._send_json(pending.source_ws, response)
        except ConnectionClosed:
            pass

    async def _request_timeout(self, internal_id: str, timeout: float) -> None:
        try:
            await asyncio.sleep(timeout)
            pending = self._pending.pop(internal_id, None)
            if pending is not None:
                if pending.response_future is not None:
                    if not pending.response_future.done():
                        pending.response_future.set_exception(
                            TunnelForwardError(
                                "WebSocket target timed out",
                                "tunnel_timeout",
                            )
                        )
                elif pending.source_ws is not None:
                    await self._send_error(
                        pending.source_ws,
                        pending.source_message_id,
                        "timeout",
                    )
        except (asyncio.CancelledError, ConnectionClosed):
            pass

    def _remove_pending(self, internal_id: str) -> _PendingRequest | None:
        pending = self._pending.pop(internal_id, None)
        if pending is not None and pending.timeout_task is not None:
            pending.timeout_task.cancel()
        return pending

    async def _heartbeat(self, websocket: Any, device_id: str) -> None:
        try:
            while self._devices.get(device_id) is websocket:
                await asyncio.sleep(self.config.heartbeat_interval_seconds)
                last_active = self._last_active.get(device_id)
                if last_active is None:
                    return
                elapsed = asyncio.get_running_loop().time() - last_active
                if elapsed > self.config.device_timeout_seconds:
                    logger.warning(
                        "Tunnel device %s timed out after %.0fs",
                        device_id,
                        elapsed,
                    )
                    # Preserve the original relay's normal-close behavior.
                    await websocket.close()
                    return
                await self._send_json(websocket, {"type": "ping"})
        except (asyncio.CancelledError, ConnectionClosed):
            pass

    async def _disconnect(self, websocket: Any, device_id: str) -> None:
        if self._devices.get(device_id) is websocket:
            self._devices.pop(device_id, None)
            self._last_active.pop(device_id, None)
            for binding in self._device_bindings.pop(device_id, set()):
                if self._service_bindings.get(binding) == device_id:
                    self._service_bindings.pop(binding, None)

        for internal_id, pending in list(self._pending.items()):
            if pending.source_ws is websocket:
                self._remove_pending(internal_id)
            elif pending.target_ws is websocket:
                self._remove_pending(internal_id)
                if (
                    pending.response_future is not None
                    and not pending.response_future.done()
                ):
                    pending.response_future.set_exception(
                        TunnelForwardError(
                            f"Device {pending.target_device} disconnected"
                        )
                    )
                elif pending.source_ws is not None:
                    await self._send_error(
                        pending.source_ws,
                        pending.source_message_id,
                        f"Device {pending.target_device} disconnected",
                    )

    @staticmethod
    def _decode_message(message: Any) -> dict[str, Any]:
        data = json.loads(message)
        if not isinstance(data, dict):
            raise ValueError("message must be a JSON object")
        return data

    @staticmethod
    async def _send_json(websocket: Any, data: dict[str, Any]) -> None:
        await websocket.send(json.dumps(data, ensure_ascii=False, separators=(",", ":")))

    async def _send_error(
        self,
        websocket: Any,
        message_id: Any,
        error: str,
    ) -> None:
        payload: dict[str, Any] = {"type": "error", "error": error}
        if message_id is not None:
            payload["message_id"] = message_id
        try:
            await self._send_json(websocket, payload)
        except ConnectionClosed:
            pass
