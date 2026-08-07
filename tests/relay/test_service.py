import asyncio

import httpx

from a2x_registry.relay.config import RelayConfig
from a2x_registry.relay.service import RelayService

from .conftest import FakeRegistry, v1_card


def test_online_tunnel_mapping_is_selected_before_http():
    observed = {}

    class Tunnel:
        def has_online_service(self, dataset, service_id):
            return (dataset, service_id) == ("agents", "target")

        async def forward_http(self, dataset, service_id, http_request, timeout):
            observed.update(
                dataset=dataset,
                service_id=service_id,
                http_request=http_request,
                timeout=timeout,
            )
            return {
                "type": "forward_response",
                "http_response": {
                    "status": 200,
                    "headers": {"Content-Type": "application/json"},
                    "body": {"transport": "websocket"},
                },
            }

    async def fail_if_http_called(_request):
        raise AssertionError("HTTP backend must not run for an online tunnel mapping")

    async def scenario():
        config = RelayConfig(
            enabled=True,
            public_base_url="http://registry.example:8000",
            allowed_origins=frozenset({"http://127.0.0.1:9101"}),
        )
        async_client = httpx.AsyncClient(
            transport=httpx.MockTransport(fail_if_http_called)
        )
        service = RelayService(
            config,
            lambda: FakeRegistry(v1_card()),
            client=async_client,
            tunnel_getter=lambda: Tunnel(),
        )
        response = await service.forward(
            "agents",
            "target",
            b'{"jsonrpc":"2.0"}',
            {"Content-Type": "application/json"},
        )
        assert response.status_code == 200
        assert response.body == b'{"transport":"websocket"}'
        assert response.headers["x-a2x-relay"] == "1"
        assert observed["http_request"]["path"] == "/a2a"
        assert observed["http_request"]["body"] == '{"jsonrpc":"2.0"}'
        await service.close()

    asyncio.run(scenario())
