"""Asynchronous HTTP/SSE relay backend."""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
from fastapi.responses import Response, StreamingResponse

from .config import RelayConfig
from .errors import RelayError
from .models import RelayTarget
from .security import filter_request_headers, filter_response_headers


class HttpRelayBackend:
    def __init__(
        self,
        config: RelayConfig,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = config
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(
                connect=config.connect_timeout_seconds,
                read=config.read_timeout_seconds,
                write=config.connect_timeout_seconds,
                pool=config.connect_timeout_seconds,
            ),
            limits=httpx.Limits(
                max_connections=config.max_inflight,
                max_keepalive_connections=min(config.max_inflight, 50),
            ),
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def forward(
        self,
        target: RelayTarget,
        body: bytes,
        headers: dict[str, str],
    ) -> Response:
        outbound_headers = filter_request_headers(
            headers,
            forward_authorization=self._config.forward_authorization,
        )
        request = self._client.build_request(
            "POST",
            target.url,
            content=body,
            headers=outbound_headers,
        )
        try:
            upstream = await self._client.send(request, stream=True)
        except httpx.TimeoutException as exc:
            raise RelayError(504, "relay_target_timeout", "Target Agent timed out") from exc
        except httpx.HTTPError as exc:
            raise RelayError(
                502,
                "relay_target_unreachable",
                "Target Agent could not be reached",
            ) from exc

        response_headers = filter_response_headers(dict(upstream.headers))
        content_type = upstream.headers.get("content-type", "").lower()
        if content_type.startswith("text/event-stream"):
            return StreamingResponse(
                self._stream(upstream),
                status_code=upstream.status_code,
                headers=response_headers,
            )

        try:
            content = await self._read_bounded(upstream)
        finally:
            await upstream.aclose()
        return Response(
            content=content,
            status_code=upstream.status_code,
            headers=response_headers,
        )

    async def _read_bounded(self, response: httpx.Response) -> bytes:
        if response.is_stream_consumed:
            content = response.content
            if len(content) > self._config.max_response_bytes:
                raise RelayError(
                    502,
                    "relay_response_too_large",
                    "Target Agent response exceeds relay limit",
                )
            return content

        chunks: list[bytes] = []
        total = 0
        async for chunk in response.aiter_raw():
            total += len(chunk)
            if total > self._config.max_response_bytes:
                raise RelayError(
                    502,
                    "relay_response_too_large",
                    "Target Agent response exceeds relay limit",
                )
            chunks.append(chunk)
        return b"".join(chunks)

    @staticmethod
    async def _stream(response: httpx.Response) -> AsyncIterator[bytes]:
        try:
            if response.is_stream_consumed:
                yield response.content
                return
            async for chunk in response.aiter_raw():
                yield chunk
        finally:
            await response.aclose()
