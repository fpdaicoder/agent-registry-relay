"""Reverse TCP tunnel: device-initiated control channel plus public port forwarding."""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from .config import TcpTunnelConfig
from .errors import TcpTunnelError


logger = logging.getLogger(__name__)
_DEVICE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_HOST_RE = re.compile(
    r"^(?:[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)*$"
)


@dataclass
class _Target:
    name: str
    local_host: str
    local_port: int
    public_port: int
    listener: asyncio.AbstractServer | None = field(default=None, repr=False)


@dataclass
class _Bridge:
    """One client-to-device byte stream, keyed by an unguessable conn_id."""

    conn_id: str
    device_id: str
    target_name: str
    client_reader: asyncio.StreamReader
    client_writer: asyncio.StreamWriter
    open_future: asyncio.Future[
        tuple[asyncio.StreamReader, asyncio.StreamWriter]
    ] | None = None
    device_reader: asyncio.StreamReader | None = None
    device_writer: asyncio.StreamWriter | None = None
    open_timeout_task: asyncio.Task[None] | None = None
    watchdog_task: asyncio.Task[None] | None = None
    copy_tasks: list[asyncio.Task[None]] = field(default_factory=list, repr=False)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    finished: bool = False
    last_activity: float = field(default_factory=time.monotonic)


class TcpTunnelServer:
    """Maintain device control connections and bridge public ports to them.

    Wire protocol (UTF-8 JSON lines terminated by ``\\n``):

    - Control connection (first frame ``register``): ``register`` /
      ``registered`` / ``open`` / ``ping`` / ``pong`` / ``closed`` / ``error``.
    - Data connection (first frame ``connect``): after the ``connect`` line the
      stream is raw bidirectional TCP bytes. ``conn_id`` values are unguessable
      capabilities handed only to the owning device's ``open`` frame.
    """

    def __init__(
        self,
        config: TcpTunnelConfig,
        registry_getter: Callable[[], Any] | None = None,
    ):
        self.config = config
        self._registry_getter = registry_getter
        self._server: asyncio.AbstractServer | None = None
        # device_id -> {"reader", "writer"}
        self._devices: dict[str, dict[str, Any]] = {}
        # (device_id, target_name) -> _Target
        self._targets: dict[tuple[str, str], _Target] = {}
        # public port -> asyncio server for that proxy port
        self._public_listeners: dict[int, asyncio.AbstractServer] = {}
        # conn_id -> _Bridge
        self._bridges: dict[str, _Bridge] = {}
        self._service_bindings: dict[tuple[str, str], str] = {}
        self._device_bindings: dict[str, set[tuple[str, str]]] = {}
        self._next_port = config.port_range_min

    # ------------------------------------------------------------------
    # Lifecycle and status
    # ------------------------------------------------------------------

    @property
    def connected_devices(self) -> int:
        return len(self._devices)

    @property
    def bound_targets(self) -> int:
        return len(self._targets)

    @property
    def active_bridges(self) -> int:
        return len(self._bridges)

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
        self._server = await asyncio.start_server(
            self._handle_connection,
            self.config.host,
            self.config.port,
            # readline() enforces this limit; oversized control frames are
            # rejected instead of buffering unbounded memory.
            limit=self.config.max_line_bytes + 1,
        )
        logger.info(
            "TCP tunnel listening on %s:%s",
            self.config.host,
            self.bound_port,
        )

    async def stop(self) -> None:
        server, self._server = self._server, None

        # Close every proxy listener first so no new client connections can
        # arrive while bridges and control channels tear down.
        for target in list(self._targets.values()):
            await self._close_target_listener(target)
        self._targets.clear()
        self._public_listeners.clear()

        for bridge in list(self._bridges.values()):
            self._finish_bridge(bridge)

        for device in list(self._devices.values()):
            device["writer"].close()
        if self._devices:
            await asyncio.gather(
                *(d["writer"].wait_closed() for d in self._devices.values()),
                return_exceptions=True,
            )
        self._devices.clear()
        self._service_bindings.clear()
        self._device_bindings.clear()

        if server is not None:
            server.close()
            await server.wait_closed()

    def status(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "host": self.config.host,
            "port": self.bound_port,
            "connected_devices": self.connected_devices,
            "bound_targets": self.bound_targets,
            "bound_services": self.bound_services,
            "active_bridges": self.active_bridges,
            "port_range": [self.config.port_range_min, self.config.port_range_max],
        }

    def device_for_service(self, dataset: str, service_id: str) -> str | None:
        device_id = self._service_bindings.get((dataset, service_id))
        if device_id is None or device_id not in self._devices:
            return None
        return device_id

    def has_online_service(self, dataset: str, service_id: str) -> bool:
        return self.device_for_service(dataset, service_id) is not None

    # ------------------------------------------------------------------
    # Shared entry point: control vs data connection
    # ------------------------------------------------------------------

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        try:
            first_line = await asyncio.wait_for(
                reader.readline(),
                timeout=self.config.register_timeout_seconds,
            )
            data = self._decode_line(first_line)
            message_type = data.get("type")
            if message_type == "connect":
                # Data connection: the rest of the stream is raw TCP bytes.
                await self._handle_data_connection(reader, writer, data)
                return
            if message_type == "register":
                await self._handle_control_connection(reader, writer, data, peer)
                return
            raise ValueError("First message must be register or connect")
        except asyncio.TimeoutError:
            await self._send_error(writer, None, "registration timeout")
            await self._close_writer(writer)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            await self._send_error(writer, None, str(exc))
            await self._close_writer(writer)
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            await self._close_writer(writer)
        except Exception:
            logger.exception("TCP tunnel connection failed from %s", peer)
            await self._send_error(writer, None, "registration failed")
            await self._close_writer(writer)

    # ------------------------------------------------------------------
    # Control channel
    # ------------------------------------------------------------------

    async def _handle_control_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        register_data: dict[str, Any],
        peer: Any,
    ) -> None:
        device_id = self._validate_registration(register_data)
        targets = self._validate_targets(register_data)

        if (
            device_id not in self._devices
            and len(self._devices) >= self.config.max_devices
        ):
            await self._send_error(writer, None, "device limit reached")
            await self._close_writer(writer)
            return

        bindings, registry_status = await self._prepare_bindings(
            device_id,
            register_data,
        )

        old_device = self._devices.get(device_id)
        if old_device is not None:
            logger.warning(
                "TCP tunnel device %s already connected; replacing the old connection",
                device_id,
            )
            await self._disconnect_device(device_id)

        prepared_targets: dict[str, _Target] = {}
        try:
            for target in targets:
                public_port = await self._allocate_public_port(
                    device_id,
                    target,
                    prepared_targets,
                )
                prepared_targets[target["name"]] = _Target(
                    name=target["name"],
                    local_host=target["local_host"],
                    local_port=target["local_port"],
                    public_port=public_port,
                )
        except (OSError, TcpTunnelError) as exc:
            for prepared in prepared_targets.values():
                await self._close_target_listener(prepared)
            message = exc.message if isinstance(exc, TcpTunnelError) else str(exc)
            await self._send_error(writer, None, message)
            await self._close_writer(writer)
            return

        self._devices[device_id] = {"reader": reader, "writer": writer}
        for name, target in prepared_targets.items():
            self._targets[(device_id, name)] = target
        self._replace_device_bindings(device_id, bindings)

        registered: dict[str, Any] = {
            "type": "registered",
            "device_id": device_id,
            "timestamp": datetime.now().isoformat(),
            "targets": [
                {"name": target.name, "public_port": target.public_port}
                for target in prepared_targets.values()
            ],
        }
        if bindings:
            registered_bindings = [
                {"dataset": dataset, "service_id": service_id}
                for dataset, service_id in sorted(bindings)
            ]
            registered["binding"] = registered_bindings[0]
            if len(registered_bindings) > 1:
                registered["bindings"] = registered_bindings
            registered["registry_status"] = registry_status
        await self._send_json(writer, registered)
        logger.info(
            "TCP tunnel device registered: %s from %s with %d target(s)",
            device_id,
            peer,
            len(prepared_targets),
        )

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    data = self._decode_line(line)
                    await self._dispatch_control(
                        writer,
                        device_id,
                        data,
                    )
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    # Keep the control channel alive on malformed frames,
                    # matching the WebSocket tunnel's tolerance.
                    logger.warning(
                        "Invalid TCP tunnel control frame from %s: %s",
                        device_id,
                        exc,
                    )
                    await self._send_error(writer, None, str(exc))
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            pass
        finally:
            if self._devices.get(device_id, {}).get("writer") is writer:
                await self._disconnect_device(device_id)
                logger.info("TCP tunnel device unregistered: %s", device_id)
            await self._close_writer(writer)

    async def _dispatch_control(
        self,
        writer: asyncio.StreamWriter,
        device_id: str,
        data: dict[str, Any],
    ) -> None:
        message_type = data.get("type")
        if message_type == "ping":
            await self._send_json(writer, {"type": "pong"})
        elif message_type == "pong":
            return
        elif message_type == "closed":
            # The device aborted a bridge (e.g. its local target refused the
            # connection). Only the owning device may close it.
            conn_id = data.get("conn_id")
            if isinstance(conn_id, str):
                bridge = self._bridges.get(conn_id)
                if bridge is not None and bridge.device_id == device_id:
                    self._finish_bridge(bridge)
        else:
            logger.warning(
                "Unknown TCP tunnel message type from %s: %s",
                device_id,
                message_type,
            )

    def _validate_registration(self, data: dict[str, Any]) -> str:
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

    def _validate_targets(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        raw_targets = data.get("targets", [])
        if not isinstance(raw_targets, list):
            raise ValueError("targets must be a list")
        if len(raw_targets) > self.config.max_targets_per_device:
            raise ValueError("too many targets")
        validated: list[dict[str, Any]] = []
        seen_names: set[str] = set()
        for raw in raw_targets:
            if not isinstance(raw, dict):
                raise ValueError("each target must be an object")
            name = raw.get("name")
            local_host = raw.get("local_host", "127.0.0.1")
            local_port = raw.get("local_port")
            public_port = raw.get("public_port", 0)
            if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
                raise ValueError(f"invalid target name: {name!r}")
            if name in seen_names:
                raise ValueError(f"duplicate target name: {name!r}")
            seen_names.add(name)
            if not isinstance(local_host, str) or not _HOST_RE.fullmatch(local_host):
                raise ValueError(f"invalid target local_host: {local_host!r}")
            if (
                isinstance(local_port, bool)
                or not isinstance(local_port, int)
                or not 1 <= local_port <= 65535
            ):
                raise ValueError(f"invalid target local_port: {local_port!r}")
            if (
                isinstance(public_port, bool)
                or not isinstance(public_port, int)
                or not 0 <= public_port <= 65535
            ):
                raise ValueError(f"invalid target public_port: {public_port!r}")
            validated.append(
                {
                    "name": name,
                    "local_host": local_host,
                    "local_port": local_port,
                    "public_port": public_port,
                }
            )
        return validated

    async def _allocate_public_port(
        self,
        device_id: str,
        target: dict[str, Any],
        prepared: dict[str, _Target],
    ) -> int:
        """Start the proxy listener for one target and return its public port."""
        requested = target["public_port"]
        occupied = {existing.public_port for existing in self._targets.values()} | {
            entry.public_port for entry in prepared.values()
        }
        if requested == 0:
            port = self._pick_free_port(occupied)
            if port is None:
                raise TcpTunnelError("No free port in the proxy port range")
        else:
            if requested in occupied:
                raise TcpTunnelError(f"public port {requested} is already in use")
            port = requested

        listener = await asyncio.start_server(
            self._make_proxy_acceptor(device_id, target["name"]),
            self.config.proxy_host,
            port,
            limit=self.config.buffer_bytes,
        )
        self._public_listeners[port] = listener
        return port

    def _pick_free_port(self, occupied: set[int]) -> int | None:
        for _ in range(self.config.port_range_max - self.config.port_range_min + 1):
            port = self._next_port
            self._next_port += 1
            if self._next_port > self.config.port_range_max:
                self._next_port = self.config.port_range_min
            if port not in occupied and port not in self._public_listeners:
                return port
        return None

    def _make_proxy_acceptor(self, device_id: str, target_name: str):
        async def accept_proxy_connection(
            client_reader: asyncio.StreamReader,
            client_writer: asyncio.StreamWriter,
        ) -> None:
            await self._open_bridge(
                device_id,
                target_name,
                client_reader,
                client_writer,
            )

        return accept_proxy_connection

    # ------------------------------------------------------------------
    # Bridging
    # ------------------------------------------------------------------

    async def _open_bridge(
        self,
        device_id: str,
        target_name: str,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        device = self._devices.get(device_id)
        if device is None or (device_id, target_name) not in self._targets:
            await self._close_writer(client_writer)
            return
        if len(self._bridges) >= self.config.max_connections:
            logger.warning(
                "TCP tunnel rejected a client connection: bridge limit reached"
            )
            await self._close_writer(client_writer)
            return

        conn_id = uuid.uuid4().hex
        open_future: asyncio.Future[
            tuple[asyncio.StreamReader, asyncio.StreamWriter]
        ] = asyncio.get_running_loop().create_future()
        bridge = _Bridge(
            conn_id=conn_id,
            device_id=device_id,
            target_name=target_name,
            client_reader=client_reader,
            client_writer=client_writer,
            open_future=open_future,
        )
        self._bridges[conn_id] = bridge
        bridge.open_timeout_task = asyncio.create_task(
            self._open_timeout(conn_id, self.config.open_timeout_seconds)
        )
        try:
            await self._send_json(
                device["writer"],
                {
                    "type": "open",
                    "conn_id": conn_id,
                    "target": target_name,
                    "client_addr": _format_addr(
                        client_writer.get_extra_info("peername")
                    ),
                },
            )
        except (ConnectionError, OSError):
            self._finish_bridge(bridge)
            await self._close_writer(client_writer)
            return

        try:
            bridge.device_reader, bridge.device_writer = await open_future
        except (TcpTunnelError, asyncio.CancelledError, ConnectionError):
            self._finish_bridge(bridge)
            await self._close_writer(client_writer)
            return
        if bridge.finished:
            # Torn down between the data connection arriving and here.
            return

        bridge.copy_tasks = [
            asyncio.create_task(
                self._copy_stream(client_reader, bridge.device_writer, bridge)
            ),
            asyncio.create_task(
                self._copy_stream(bridge.device_reader, client_writer, bridge)
            ),
        ]
        if self.config.idle_timeout_seconds > 0:
            bridge.watchdog_task = asyncio.create_task(
                self._idle_watchdog(bridge)
            )
        # The first finished direction tears the whole bridge down; there is
        # no TCP half-close relay.
        await asyncio.wait(
            bridge.copy_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        self._finish_bridge(bridge)
        await self._close_writer(client_writer)
        if bridge.device_writer is not None:
            await self._close_writer(bridge.device_writer)

    async def _handle_data_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        data: dict[str, Any],
    ) -> None:
        conn_id = data.get("conn_id")
        if not isinstance(conn_id, str):
            await self._send_error(writer, None, "missing conn_id")
            await self._close_writer(writer)
            return
        bridge = self._bridges.get(conn_id)
        if (
            bridge is None
            or bridge.finished
            or bridge.open_future is None
            or bridge.open_future.done()
        ):
            await self._send_error(writer, conn_id, "connection not found")
            await self._close_writer(writer)
            return

        try:
            bridge.open_future.set_result((reader, writer))
        except asyncio.InvalidStateError:
            # The bridge was torn down (e.g. open timeout) between our lookup
            # and here; drop the late data connection.
            await self._close_writer(writer)
            return
        # Keep the data connection's handler alive for the bridge lifetime;
        # _finish_bridge closes both writers and sets the event.
        await bridge.done.wait()

    async def _copy_stream(
        self,
        source: asyncio.StreamReader,
        sink: asyncio.StreamWriter,
        bridge: _Bridge,
    ) -> None:
        try:
            while True:
                chunk = await source.read(self.config.buffer_bytes)
                if not chunk:
                    break
                bridge.last_activity = time.monotonic()
                sink.write(chunk)
                await sink.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            sink.close()

    async def _open_timeout(self, conn_id: str, timeout: float) -> None:
        try:
            await asyncio.sleep(timeout)
        except asyncio.CancelledError:
            return
        bridge = self._bridges.get(conn_id)
        if bridge is not None and bridge.open_future is not None:
            if not bridge.open_future.done():
                bridge.open_future.set_exception(
                    TcpTunnelError(
                        "Device did not open the data connection",
                        "tunnel_timeout",
                    )
                )

    async def _idle_watchdog(self, bridge: _Bridge) -> None:
        try:
            while not bridge.done.is_set():
                await asyncio.sleep(self.config.idle_timeout_seconds)
                elapsed = time.monotonic() - bridge.last_activity
                if elapsed >= self.config.idle_timeout_seconds:
                    logger.info(
                        "TCP tunnel bridge %s idle for %.0fs; closing",
                        bridge.conn_id,
                        elapsed,
                    )
                    self._finish_bridge(bridge)
                    return
        except asyncio.CancelledError:
            pass

    def _finish_bridge(self, bridge: _Bridge) -> None:
        """Idempotently tear down one bridge and close both sides."""
        if bridge.finished:
            return
        bridge.finished = True
        self._bridges.pop(bridge.conn_id, None)
        if bridge.open_timeout_task is not None:
            bridge.open_timeout_task.cancel()
        if bridge.watchdog_task is not None:
            bridge.watchdog_task.cancel()
        for task in bridge.copy_tasks:
            task.cancel()
        if bridge.open_future is not None and not bridge.open_future.done():
            bridge.open_future.set_exception(
                TcpTunnelError("connection closed")
            )
        bridge.client_writer.close()
        if bridge.device_writer is not None:
            bridge.device_writer.close()
        bridge.done.set()

    # ------------------------------------------------------------------
    # Device teardown
    # ------------------------------------------------------------------

    async def _disconnect_device(self, device_id: str) -> None:
        device = self._devices.pop(device_id, None)
        for key in [key for key in self._targets if key[0] == device_id]:
            target = self._targets.pop(key, None)
            if target is not None:
                await self._close_target_listener(target)
        for binding in self._device_bindings.pop(device_id, set()):
            if self._service_bindings.get(binding) == device_id:
                self._service_bindings.pop(binding, None)

        if device is not None:
            writer = device["writer"]
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

        for bridge in [
            bridge
            for bridge in self._bridges.values()
            if bridge.device_id == device_id
        ]:
            self._finish_bridge(bridge)

    async def _close_target_listener(self, target: _Target) -> None:
        listener = self._public_listeners.pop(target.public_port, None)
        if listener is not None:
            listener.close()
            try:
                await listener.wait_closed()
            except (ConnectionError, OSError):
                pass

    # ------------------------------------------------------------------
    # Registry service bindings (mirrors the WebSocket tunnel semantics)
    # ------------------------------------------------------------------

    async def _prepare_bindings(
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
            metadata["tcpTunnelDeviceId"] = device_id
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
                metadata.get("tcpTunnelDeviceId")
                if isinstance(metadata, dict)
                else None
            )
            if mapped_device != device_id:
                raise ValueError(
                    "Agent Card tcpTunnelDeviceId does not match device_id"
                )
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
                    and metadata.get("tcpTunnelDeviceId") == device_id
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

    # ------------------------------------------------------------------
    # Wire helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _decode_line(line: bytes | str) -> dict[str, Any]:
        raw = line.decode("utf-8") if isinstance(line, bytes) else line
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("message must be a JSON object")
        return data

    @staticmethod
    async def _send_json(writer: asyncio.StreamWriter, data: dict[str, Any]) -> None:
        payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        writer.write(payload.encode("utf-8") + b"\n")
        await writer.drain()

    async def _send_error(
        self,
        writer: asyncio.StreamWriter,
        conn_id: Any,
        error: str,
    ) -> None:
        payload: dict[str, Any] = {"type": "error", "error": error}
        if conn_id is not None:
            payload["conn_id"] = conn_id
        try:
            await self._send_json(writer, payload)
        except (ConnectionError, OSError):
            pass

    @staticmethod
    async def _close_writer(writer: asyncio.StreamWriter) -> None:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass


def _format_addr(addr: Any) -> str:
    if isinstance(addr, tuple) and len(addr) >= 2:
        return f"{addr[0]}:{addr[1]}"
    return "unknown"
