"""Relay orchestration independent of the HTTP router."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any, Callable
from urllib.parse import urlsplit

import httpx
from fastapi.responses import Response, StreamingResponse

from a2x_registry.tunnel.errors import TunnelForwardError

from .config import RelayConfig
from .errors import RelayError
from .http_backend import HttpRelayBackend
from .resolver import RelayResolver
from .security import (
    filter_request_headers,
    filter_response_headers,
    validate_target_url,
)


class RelayService:
    def __init__(
        self,
        config: RelayConfig,
        registry_getter: Callable[[], Any],
        client: httpx.AsyncClient | None = None,
        tunnel_getter: Callable[[], Any] | None = None,
    ) -> None:
        if not config.enabled:
            raise ValueError("RelayService cannot start with relay disabled")
        config.validate()
        self.config = config
        self.resolver = RelayResolver(config, registry_getter)
        self._backend = HttpRelayBackend(config, client=client)
        self._tunnel_getter = tunnel_getter or (lambda: None)
        self._inflight = asyncio.Semaphore(config.max_inflight)

    async def close(self) -> None:
        await self._backend.close()

    async def forward(
        self,
        dataset: str,
        service_id: str,
        body: bytes,
        headers: dict[str, str],
    ) -> Response:
        try:
            await asyncio.wait_for(self._inflight.acquire(), timeout=0.01)
        except TimeoutError as exc:
            raise RelayError(429, "relay_busy", "Relay concurrency limit reached") from exc
        release_immediately = True
        try:
            target = self.resolver.resolve(dataset, service_id)
            tunnel = self._tunnel_getter()
            if (
                tunnel is not None
                and tunnel.has_online_service(dataset, service_id)
            ):
                return await self._forward_tunnel(
                    tunnel,
                    target,
                    body,
                    headers,
                )
            validate_target_url(target.url, self.config)
            response = await self._backend.forward(target, body, headers)
            if isinstance(response, StreamingResponse):
                response.body_iterator = self._hold_slot(response.body_iterator)
                release_immediately = False
            return response
        finally:
            if release_immediately:
                self._inflight.release()

    async def _forward_tunnel(
        self,
        tunnel: Any,
        target: Any,
        body: bytes,
        headers: dict[str, str],
    ) -> Response:
        parsed = urlsplit(target.url)
        path = parsed.path or "/"
        if parsed.query:
            path += f"?{parsed.query}"
        # The OpenClaw tunnel client feeds this value directly into the
        # Fetch API, which requires a string/Buffer body rather than a parsed
        # JSON object. Preserve the original request bytes as UTF-8 text.
        request_body = body.decode("utf-8", errors="replace")
        outbound_headers = filter_request_headers(
            headers,
            forward_authorization=self.config.forward_authorization,
        )
        try:
            tunnel_response = await tunnel.forward_http(
                target.dataset,
                target.service_id,
                {
                    "method": "POST",
                    "path": path,
                    "headers": outbound_headers,
                    "body": request_body,
                },
                timeout=self.config.read_timeout_seconds,
            )
        except TunnelForwardError as exc:
            status_code = 504 if exc.code == "tunnel_timeout" else 502
            if exc.code == "tunnel_busy":
                status_code = 429
            raise RelayError(
                status_code,
                exc.code,
                str(exc),
            ) from exc

        http_response = tunnel_response.get("http_response")
        if not isinstance(http_response, dict):
            raise RelayError(
                502,
                "tunnel_response_invalid",
                "WebSocket target returned an invalid response",
            )
        status = http_response.get("status", 200)
        if isinstance(status, bool) or not isinstance(status, int) or not 100 <= status <= 599:
            raise RelayError(
                502,
                "tunnel_response_invalid",
                "WebSocket target returned an invalid HTTP status",
            )
        raw_headers = http_response.get("headers")
        safe_headers = (
            {
                name: value
                for name, value in raw_headers.items()
                if isinstance(name, str) and isinstance(value, str)
            }
            if isinstance(raw_headers, dict)
            else {}
        )
        response_headers = filter_response_headers(
            safe_headers
        )
        response_body = http_response.get("body")
        if isinstance(response_body, (dict, list)):
            content = json.dumps(
                response_body,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            response_headers.setdefault("Content-Type", "application/json")
        elif response_body is None:
            content = b""
        elif isinstance(response_body, str):
            content = response_body.encode("utf-8")
        else:
            raise RelayError(
                502,
                "tunnel_response_invalid",
                "WebSocket target returned an unsupported body",
            )
        if len(content) > self.config.max_response_bytes:
            raise RelayError(
                502,
                "relay_response_too_large",
                "Target Agent response exceeds relay limit",
            )
        return Response(
            content=content,
            status_code=status,
            headers=response_headers,
        )

    async def _hold_slot(self, body: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
        try:
            async for chunk in body:
                yield chunk
        finally:
            try:
                close = getattr(body, "aclose", None)
                if close is not None:
                    await close()
            finally:
                self._inflight.release()

    def agent_card(self, dataset: str, service_id: str, route: str) -> dict[str, Any]:
        return self.resolver.agent_card(dataset, service_id, route)
