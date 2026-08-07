import asyncio
import json
from types import SimpleNamespace

import websockets

from a2x_registry.register.models import AgentCard
from a2x_registry.tunnel.config import TunnelConfig
from a2x_registry.tunnel.server import WebSocketTunnelServer


async def _send(websocket, payload):
    await websocket.send(json.dumps(payload))


async def _receive(websocket):
    return json.loads(await asyncio.wait_for(websocket.recv(), timeout=1))


async def _connect(uri, device_id, token=None):
    websocket = await websockets.connect(uri, proxy=None)
    registration = {"type": "register", "device_id": device_id}
    if token is not None:
        registration["token"] = token
    await _send(websocket, registration)
    response = await _receive(websocket)
    assert response["type"] == "registered"
    return websocket


def _config(**overrides):
    values = {
        "enabled": True,
        "host": "127.0.0.1",
        "port": 0,
        "heartbeat_interval_seconds": 30,
        "device_timeout_seconds": 60,
    }
    values.update(overrides)
    return TunnelConfig(**values)


class _Registry:
    def __init__(self):
        self.entries = {}

    def register_a2a(self, request):
        self.entries[(request.dataset, request.service_id)] = SimpleNamespace(
            type="a2a",
            agent_card=request.agent_card,
        )
        return SimpleNamespace(status="registered")

    def get_entry(self, dataset, service_id):
        return self.entries.get((dataset, service_id))

    def list_services(self, dataset):
        return [
            {
                "id": service_id,
                "type": "a2a",
                "name": entry.agent_card.name,
                "description": entry.agent_card.description,
                "metadata": entry.agent_card.model_dump(exclude_none=True),
                "source": "api_config",
            }
            for (entry_dataset, service_id), entry in self.entries.items()
            if entry_dataset == dataset
        ]

    def list_datasets(self):
        return sorted({dataset for dataset, _ in self.entries})


def test_register_ping_and_status():
    async def scenario():
        server = WebSocketTunnelServer(_config())
        await server.start()
        websocket = await _connect(
            f"ws://127.0.0.1:{server.bound_port}",
            "device-a",
        )
        try:
            assert server.connected_devices == 1
            await _send(websocket, {"type": "ping"})
            assert await _receive(websocket) == {"type": "pong"}
        finally:
            await websocket.close()
            await server.stop()

        assert server.connected_devices == 0
        assert server.pending_requests == 0

    asyncio.run(scenario())


def test_connection_only_registration_matches_legacy_relay_contract():
    async def scenario():
        server = WebSocketTunnelServer(_config())
        await server.start()
        websocket = await websockets.connect(
            f"ws://127.0.0.1:{server.bound_port}",
            proxy=None,
        )
        try:
            await _send(
                websocket,
                {"type": "register", "device_id": "HW-Phone1"},
            )
            registered = await _receive(websocket)
            assert set(registered) == {"type", "device_id", "timestamp"}
            assert registered["type"] == "registered"
            assert registered["device_id"] == "HW-Phone1"

            # The legacy relay ignored unknown and malformed post-registration
            # frames instead of closing the device connection.
            await _send(websocket, {"type": "legacy_extension"})
            await websocket.send("{invalid-json")
            await _send(websocket, {"type": "ping"})
            assert await _receive(websocket) == {"type": "pong"}
        finally:
            await websocket.close()
            await server.stop()

    asyncio.run(scenario())


def test_connection_only_registration_auto_binds_pre_registered_services():
    async def scenario():
        registry = _Registry()
        card = AgentCard(
            name="PC Agent",
            description="reachable through the tunnel",
            version="1.0.0",
            protocolVersion="0.3.0",
            url="http://127.0.0.1:18800/a2a/jsonrpc",
            preferredTransport="JSONRPC",
            metadata={"tunnelDeviceId": "HW-PC1"},
        )
        registry.entries[("cli_agents", "hw-pc1-openclaw")] = SimpleNamespace(
            type="a2a",
            agent_card=card,
        )
        server = WebSocketTunnelServer(
            _config(auto_bind_registered_services=True),
            lambda: registry,
        )
        await server.start()
        websocket = await websockets.connect(
            f"ws://127.0.0.1:{server.bound_port}",
            proxy=None,
        )
        try:
            await _send(
                websocket,
                {"type": "register", "device_id": "HW-PC1"},
            )
            registered = await _receive(websocket)
            assert registered["binding"] == {
                "dataset": "cli_agents",
                "service_id": "hw-pc1-openclaw",
            }
            assert registered["registry_status"] == "auto-bound"
            assert server.device_for_service(
                "cli_agents",
                "hw-pc1-openclaw",
            ) == "HW-PC1"
            assert server.bound_services == 1
        finally:
            await websocket.close()
            await server.stop()

    asyncio.run(scenario())


def test_duplicate_legacy_device_replaces_old_connection_with_normal_close():
    async def scenario():
        server = WebSocketTunnelServer(_config())
        await server.start()
        uri = f"ws://127.0.0.1:{server.bound_port}"
        old_websocket = await _connect(uri, "HW-Phone1")
        new_websocket = await _connect(uri, "HW-Phone1")
        try:
            await asyncio.wait_for(old_websocket.wait_closed(), timeout=1)
            assert old_websocket.close_code == 1000
            assert old_websocket.close_reason == ""

            await _send(new_websocket, {"type": "ping"})
            assert await _receive(new_websocket) == {"type": "pong"}
            assert server.connected_devices == 1
        finally:
            await old_websocket.close()
            await new_websocket.close()
            await server.stop()

    asyncio.run(scenario())


def test_openclaw_2026_3_13_tunnel_session_wire_contract():
    """Exercise the exact frames emitted by the packaged TunnelSession."""

    async def scenario():
        server = WebSocketTunnelServer(_config())
        await server.start()
        uri = f"ws://127.0.0.1:{server.bound_port}"
        phone_one = await _connect(uri, "HW-Phone1")
        phone_two = await _connect(uri, "HW-Phone2")
        try:
            await _send(
                phone_one,
                {
                    "type": "forward_request",
                    "message_id": "openclaw-request-1",
                    "source_device": "HW-Phone1",
                    "target_device": "HW-Phone2",
                    "http_request": {
                        "method": "POST",
                        "path": "/a2a",
                        "headers": {"content-type": "application/json"},
                        "body": '{"message":"hello"}',
                    },
                    "timeout": 300,
                },
            )

            forwarded = await _receive(phone_two)
            assert forwarded["type"] == "forward_request"
            assert forwarded["source_device"] == "HW-Phone1"
            assert forwarded["target_device"] == "HW-Phone2"
            assert forwarded["http_request"]["body"] == '{"message":"hello"}'

            await _send(
                phone_two,
                {
                    "type": "forward_response",
                    "message_id": forwarded["message_id"],
                    "http_response": {
                        "status": 200,
                        "headers": {"content-type": "application/json"},
                        "body": '{"ok":true}',
                    },
                    "status": 200,
                },
            )

            response = await _receive(phone_one)
            assert response["type"] == "forward_response"
            assert response["message_id"] == "openclaw-request-1"
            assert response["http_response"] == {
                "status": 200,
                "headers": {"content-type": "application/json"},
                "body": '{"ok":true}',
            }

            # TunnelSession sends its own application heartbeat every 15s.
            await _send(phone_one, {"type": "ping"})
            assert await _receive(phone_one) == {"type": "pong"}
        finally:
            await phone_one.close()
            await phone_two.close()
            await server.stop()

    asyncio.run(scenario())


def test_forward_restores_source_message_id_and_allows_duplicates():
    async def scenario():
        server = WebSocketTunnelServer(_config())
        await server.start()
        uri = f"ws://127.0.0.1:{server.bound_port}"
        source = await _connect(uri, "device-a")
        target = await _connect(uri, "device-b")
        try:
            request = {
                "type": "forward_request",
                "message_id": "same-client-id",
                "target_device": "device-b",
                "http_request": {"method": "POST", "path": "/a2a", "body": {"n": 1}},
            }
            await _send(source, request)
            request["http_request"]["body"]["n"] = 2
            await _send(source, request)

            forwarded_one = await _receive(target)
            forwarded_two = await _receive(target)
            assert forwarded_one["message_id"] != "same-client-id"
            assert forwarded_one["message_id"] != forwarded_two["message_id"]

            await _send(
                target,
                {
                    "type": "forward_response",
                    "message_id": forwarded_two["message_id"],
                    "http_response": {"status": 200, "body": {"n": 2}},
                },
            )
            await _send(
                target,
                {
                    "type": "forward_response",
                    "message_id": forwarded_one["message_id"],
                    "http_response": {"status": 200, "body": {"n": 1}},
                },
            )

            response_two = await _receive(source)
            response_one = await _receive(source)
            assert response_two["message_id"] == "same-client-id"
            assert response_two["http_response"]["body"] == {"n": 2}
            assert response_one["message_id"] == "same-client-id"
            assert response_one["http_response"]["body"] == {"n": 1}
            assert server.pending_requests == 0
        finally:
            await source.close()
            await target.close()
            await server.stop()

    asyncio.run(scenario())


def test_only_target_device_can_complete_request():
    async def scenario():
        server = WebSocketTunnelServer(_config())
        await server.start()
        uri = f"ws://127.0.0.1:{server.bound_port}"
        source = await _connect(uri, "device-a")
        target = await _connect(uri, "device-b")
        attacker = await _connect(uri, "device-c")
        try:
            await _send(
                source,
                {
                    "type": "forward_request",
                    "message_id": "source-id",
                    "target_device": "device-b",
                    "http_request": {"method": "GET", "path": "/"},
                },
            )
            forwarded = await _receive(target)

            await _send(
                attacker,
                {
                    "type": "forward_response",
                    "message_id": forwarded["message_id"],
                    "http_response": {"status": 200, "body": "spoofed"},
                },
            )
            rejection = await _receive(attacker)
            assert rejection["type"] == "error"
            assert rejection["error"] == "response source mismatch"
            assert server.pending_requests == 1

            await _send(
                target,
                {
                    "type": "forward_response",
                    "message_id": forwarded["message_id"],
                    "http_response": {"status": 200, "body": "real"},
                },
            )
            response = await _receive(source)
            assert response["message_id"] == "source-id"
            assert response["http_response"]["body"] == "real"
        finally:
            await source.close()
            await target.close()
            await attacker.close()
            await server.stop()

    asyncio.run(scenario())


def test_shared_token_is_optional_but_enforced_when_set():
    async def scenario():
        server = WebSocketTunnelServer(_config(shared_token="secret"))
        await server.start()
        uri = f"ws://127.0.0.1:{server.bound_port}"
        rejected = await websockets.connect(uri, proxy=None)
        try:
            await _send(rejected, {"type": "register", "device_id": "device-a"})
            error = await _receive(rejected)
            assert error["type"] == "error"
            assert error["error"] == "invalid tunnel token"
        finally:
            await rejected.close()

        accepted = await _connect(uri, "device-a", token="secret")
        await accepted.close()
        await server.stop()

    asyncio.run(scenario())


def test_service_mapping_discovery_and_registry_originated_forward():
    async def scenario():
        registry = _Registry()
        server = WebSocketTunnelServer(_config(), lambda: registry)
        await server.start()
        uri = f"ws://127.0.0.1:{server.bound_port}"
        target = await websockets.connect(uri, proxy=None)
        observer = await _connect(uri, "observer")
        card = AgentCard(
            name="Mapped Agent",
            description="reachable through the tunnel",
            version="1.0.0",
            protocolVersion="1.0",
            url="http://127.0.0.1:18800/a2a",
            preferredTransport="JSONRPC",
        ).model_dump()
        try:
            await _send(
                target,
                {
                    "type": "register",
                    "device_id": "mapped-device",
                    "dataset": "agents",
                    "service_id": "mapped-agent",
                    "agent_card": card,
                },
            )
            registered = await _receive(target)
            assert registered["binding"] == {
                "dataset": "agents",
                "service_id": "mapped-agent",
            }
            assert server.device_for_service("agents", "mapped-agent") == "mapped-device"

            await _send(
                observer,
                {
                    "type": "discover_request",
                    "message_id": "discover-1",
                    "dataset": "agents",
                },
            )
            discovery = await _receive(observer)
            assert discovery["type"] == "discover_response"
            assert discovery["services"][0]["id"] == "mapped-agent"
            assert discovery["services"][0]["tunnel"] == {
                "online": True,
                "deviceId": "mapped-device",
            }

            forward_task = asyncio.create_task(
                server.forward_http(
                    "agents",
                    "mapped-agent",
                    {
                        "method": "POST",
                        "path": "/a2a",
                        "headers": {"content-type": "application/json"},
                        "body": {"hello": "world"},
                    },
                    timeout=1,
                )
            )
            forwarded = await _receive(target)
            assert forwarded["source_device"] == "registry-relay"
            await _send(
                target,
                {
                    "type": "forward_response",
                    "message_id": forwarded["message_id"],
                    "http_response": {
                        "status": 200,
                        "headers": {"content-type": "application/json"},
                        "body": {"ok": True},
                    },
                },
            )
            response = await forward_task
            assert response["http_response"]["body"] == {"ok": True}
        finally:
            await target.close()
            await observer.close()
            await server.stop()

        assert server.device_for_service("agents", "mapped-agent") is None

    asyncio.run(scenario())
