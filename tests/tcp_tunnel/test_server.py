import asyncio
import json
from types import SimpleNamespace

from a2x_registry.register.models import AgentCard
from a2x_registry.tcp_tunnel.config import TcpTunnelConfig
from a2x_registry.tcp_tunnel.server import TcpTunnelServer


async def _send_line(writer, payload):
    writer.write(json.dumps(payload).encode("utf-8") + b"\n")
    await writer.drain()


async def _receive_line(reader):
    line = await asyncio.wait_for(reader.readline(), timeout=5)
    return json.loads(line.decode("utf-8"))


async def _connect_control(server, device_id, targets, token=None):
    reader, writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
    registration = {"type": "register", "device_id": device_id, "targets": targets}
    if token is not None:
        registration["token"] = token
    await _send_line(writer, registration)
    response = await _receive_line(reader)
    assert response["type"] == "registered", response
    return reader, writer, response


async def _start_echo_server():
    """A tiny TCP echo server standing in for the device-local service."""

    async def handle(reader, writer):
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                writer.write(chunk)
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, port


async def _run_device_side_bridge(data_reader, data_writer, local_port):
    """The device half a real client performs: pump bytes between its data
    connection to the tunnel and the registered local service."""

    async def pump(source, sink):
        try:
            while True:
                chunk = await source.read(65536)
                if not chunk:
                    break
                sink.write(chunk)
                await sink.drain()
        except ConnectionError:
            pass
        finally:
            sink.close()

    local_reader, local_writer = await asyncio.open_connection(
        "127.0.0.1",
        local_port,
    )
    await asyncio.gather(
        pump(data_reader, local_writer),
        pump(local_reader, data_writer),
    )


def _config(**overrides):
    values = {
        "enabled": True,
        "host": "127.0.0.1",
        "port": 0,
        "proxy_host": "127.0.0.1",
        "port_range_min": 20000,
        "port_range_max": 20050,
        "open_timeout_seconds": 1,
    }
    values.update(overrides)
    return TcpTunnelConfig(**values)


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


def test_register_ping_status_and_targets():
    async def scenario():
        server = TcpTunnelServer(_config())
        await server.start()
        echo, echo_port = await _start_echo_server()
        reader, writer = None, None
        try:
            reader, writer, registered = await _connect_control(
                server,
                "device-a",
                [{"name": "echo", "local_port": echo_port}],
            )
            target = registered["targets"][0]
            assert target["name"] == "echo"
            assert 20000 <= target["public_port"] <= 20050
            assert server.connected_devices == 1
            assert server.bound_targets == 1

            await _send_line(writer, {"type": "ping"})
            assert await _receive_line(reader) == {"type": "pong"}
        finally:
            if writer is not None:
                writer.close()
            echo.close()
            await server.stop()

        assert server.connected_devices == 0
        assert server.bound_targets == 0
        assert server.active_bridges == 0

    asyncio.run(scenario())


def test_full_bridge_round_trip():
    """Bytes written by a public-port client are echoed back verbatim."""

    async def scenario():
        server = TcpTunnelServer(_config())
        await server.start()
        echo, echo_port = await _start_echo_server()
        device_reader, device_writer, registered = await _connect_control(
            server,
            "device-a",
            [{"name": "echo", "local_port": echo_port, "public_port": 0}],
        )
        public_port = registered["targets"][0]["public_port"]
        try:
            client_reader, client_writer = await asyncio.open_connection(
                "127.0.0.1",
                public_port,
            )

            open_frame = await _receive_line(device_reader)
            assert open_frame["type"] == "open"
            assert open_frame["target"] == "echo"
            conn_id = open_frame["conn_id"]

            data_reader, data_writer = await asyncio.open_connection(
                "127.0.0.1",
                server.bound_port,
            )
            await _send_line(
                data_writer,
                {"type": "connect", "conn_id": conn_id},
            )
            # A real device client bridges its data connection to the
            # registered local service; do the same here.
            device_side = asyncio.create_task(
                _run_device_side_bridge(data_reader, data_writer, echo_port)
            )

            payload = b"hello tcp tunnel"
            client_writer.write(payload)
            await client_writer.drain()
            echoed = await asyncio.wait_for(
                client_reader.readexactly(len(payload)),
                timeout=5,
            )
            assert echoed == payload
            assert server.active_bridges == 1

            client_writer.close()
            for _ in range(50):
                if server.active_bridges == 0:
                    break
                await asyncio.sleep(0.05)
            assert server.active_bridges == 0
        finally:
            device_side.cancel()
            client_writer.close()
            data_writer.close()
            device_writer.close()
            echo.close()
            await server.stop()

    asyncio.run(scenario())


def test_explicit_public_port_and_conflict_rejection():
    async def scenario():
        server = TcpTunnelServer(_config())
        await server.start()
        echo, echo_port = await _start_echo_server()
        _, writer_one, registered_one = await _connect_control(
            server,
            "device-a",
            [{"name": "echo", "local_port": echo_port, "public_port": 20010}],
        )
        assert registered_one["targets"][0]["public_port"] == 20010

        reader_two, writer_two = await asyncio.open_connection(
            "127.0.0.1",
            server.bound_port,
        )
        try:
            await _send_line(
                writer_two,
                {
                    "type": "register",
                    "device_id": "device-b",
                    "targets": [
                        {
                            "name": "echo",
                            "local_port": echo_port,
                            "public_port": 20010,
                        }
                    ],
                },
            )
            error = await _receive_line(reader_two)
            assert error["type"] == "error"
            assert "already in use" in error["error"]
            assert server.connected_devices == 1
        finally:
            writer_one.close()
            writer_two.close()
            echo.close()
            await server.stop()

    asyncio.run(scenario())


def test_shared_token_is_enforced():
    async def scenario():
        server = TcpTunnelServer(_config(shared_token="secret"))
        await server.start()
        echo, echo_port = await _start_echo_server()
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            server.bound_port,
        )
        try:
            await _send_line(
                writer,
                {
                    "type": "register",
                    "device_id": "device-a",
                    "token": "wrong",
                    "targets": [{"name": "echo", "local_port": echo_port}],
                },
            )
            error = await _receive_line(reader)
            assert error["type"] == "error"
            assert error["error"] == "invalid tunnel token"
        finally:
            writer.close()
            echo.close()

        _, accepted_writer, _ = await _connect_control(
            server,
            "device-a",
            [{"name": "echo", "local_port": echo_port}],
            token="secret",
        )
        accepted_writer.close()
        await server.stop()

    asyncio.run(scenario())


def test_device_disconnect_closes_proxy_port_and_bridges():
    async def scenario():
        server = TcpTunnelServer(_config())
        await server.start()
        echo, echo_port = await _start_echo_server()
        device_reader, device_writer, registered = await _connect_control(
            server,
            "device-a",
            [{"name": "echo", "local_port": echo_port, "public_port": 20011}],
        )
        public_port = registered["targets"][0]["public_port"]
        client_reader, client_writer = await asyncio.open_connection(
            "127.0.0.1",
            public_port,
        )
        open_frame = await _receive_line(device_reader)
        data_reader, data_writer = await asyncio.open_connection(
            "127.0.0.1",
            server.bound_port,
        )
        await _send_line(
            data_writer,
            {"type": "connect", "conn_id": open_frame["conn_id"]},
        )
        await asyncio.sleep(0.1)
        assert server.active_bridges == 1

        try:
            device_writer.close()
            for _ in range(100):
                if server.connected_devices == 0 and server.active_bridges == 0:
                    break
                await asyncio.sleep(0.05)
            assert server.connected_devices == 0
            assert server.active_bridges == 0
            # The client connection must be closed by the server.
            data = await asyncio.wait_for(client_reader.read(), timeout=5)
            assert data == b""
        finally:
            client_writer.close()
            data_writer.close()
            echo.close()
            await server.stop()

    asyncio.run(scenario())


def test_duplicate_device_replaces_old_connection():
    async def scenario():
        server = TcpTunnelServer(_config())
        await server.start()
        echo, echo_port = await _start_echo_server()
        old_reader, old_writer, _ = await _connect_control(
            server,
            "device-a",
            [{"name": "echo", "local_port": echo_port, "public_port": 20012}],
        )
        _, new_writer, _ = await _connect_control(
            server,
            "device-a",
            [{"name": "echo", "local_port": echo_port, "public_port": 20013}],
        )
        try:
            assert server.connected_devices == 1
            assert server.bound_targets == 1
            # The old control connection is closed by the server.
            leftover = await asyncio.wait_for(old_reader.read(), timeout=5)
            assert leftover == b""
        finally:
            old_writer.close()
            new_writer.close()
            echo.close()
            await server.stop()

    asyncio.run(scenario())


def test_service_binding_via_register():
    async def scenario():
        registry = _Registry()
        server = TcpTunnelServer(_config(), lambda: registry)
        await server.start()
        echo, echo_port = await _start_echo_server()
        card = AgentCard(
            name="Mapped Agent",
            description="reachable through the TCP tunnel",
            version="1.0.0",
            protocolVersion="1.0",
            url="http://127.0.0.1:18800/a2a",
            preferredTransport="JSONRPC",
        ).model_dump()
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            server.bound_port,
        )
        try:
            await _send_line(
                writer,
                {
                    "type": "register",
                    "device_id": "mapped-device",
                    "dataset": "agents",
                    "service_id": "mapped-agent",
                    "agent_card": card,
                    "targets": [{"name": "a2a", "local_port": echo_port}],
                },
            )
            registered = await _receive_line(reader)
            assert registered["binding"] == {
                "dataset": "agents",
                "service_id": "mapped-agent",
            }
            assert server.device_for_service("agents", "mapped-agent") == (
                "mapped-device"
            )
            assert server.has_online_service("agents", "mapped-agent") is True
        finally:
            writer.close()
            echo.close()
            await server.stop()

        assert server.device_for_service("agents", "mapped-agent") is None

    asyncio.run(scenario())


def test_auto_bind_finds_registered_services():
    async def scenario():
        registry = _Registry()
        card = AgentCard(
            name="PC Agent",
            description="reachable through the TCP tunnel",
            version="1.0.0",
            protocolVersion="0.3.0",
            url="http://127.0.0.1:18800/a2a/jsonrpc",
            preferredTransport="JSONRPC",
            metadata={"tcpTunnelDeviceId": "HW-PC1"},
        )
        registry.entries[("cli_agents", "hw-pc1-openclaw")] = SimpleNamespace(
            type="a2a",
            agent_card=card,
        )
        server = TcpTunnelServer(
            _config(auto_bind_registered_services=True),
            lambda: registry,
        )
        await server.start()
        echo, echo_port = await _start_echo_server()
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            server.bound_port,
        )
        try:
            await _send_line(
                writer,
                {
                    "type": "register",
                    "device_id": "HW-PC1",
                    "targets": [{"name": "a2a", "local_port": echo_port}],
                },
            )
            registered = await _receive_line(reader)
            assert registered["binding"] == {
                "dataset": "cli_agents",
                "service_id": "hw-pc1-openclaw",
            }
            assert registered["registry_status"] == "auto-bound"
        finally:
            writer.close()
            echo.close()
            await server.stop()

    asyncio.run(scenario())


def test_open_timeout_closes_client():
    async def scenario():
        server = TcpTunnelServer(_config(open_timeout_seconds=0.2))
        await server.start()
        echo, echo_port = await _start_echo_server()
        device_reader, device_writer, registered = await _connect_control(
            server,
            "device-a",
            [{"name": "echo", "local_port": echo_port}],
        )
        public_port = registered["targets"][0]["public_port"]
        client_reader, client_writer = await asyncio.open_connection(
            "127.0.0.1",
            public_port,
        )
        try:
            # The device never opens the data connection; the client must be
            # closed after the open timeout.
            data = await asyncio.wait_for(client_reader.read(), timeout=5)
            assert data == b""
            assert server.active_bridges == 0
        finally:
            client_writer.close()
            device_writer.close()
            echo.close()
            await server.stop()

    asyncio.run(scenario())
