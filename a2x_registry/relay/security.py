"""Target and header policy for the A2A relay."""

from __future__ import annotations

import ipaddress
from urllib.parse import urlsplit

from .config import RelayConfig, normalize_origin
from .errors import RelayError


_REQUEST_HEADERS = frozenset({
    "accept",
    "content-type",
    "traceparent",
    "tracestate",
    "x-a2a-version",
    "x-request-id",
})
_RESPONSE_HEADERS = frozenset({
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-type",
    "etag",
    "x-a2a-version",
    "x-request-id",
})
_METADATA_IP = ipaddress.ip_address("169.254.169.254")


def filter_request_headers(
    headers: dict[str, str],
    forward_authorization: bool,
) -> dict[str, str]:
    allowed = set(_REQUEST_HEADERS)
    if forward_authorization:
        allowed.add("authorization")
    return {
        name: value
        for name, value in headers.items()
        if name.lower() in allowed
    }


def filter_response_headers(headers: dict[str, str]) -> dict[str, str]:
    filtered = {
        name: value
        for name, value in headers.items()
        if name.lower() in _RESPONSE_HEADERS
    }
    filtered["X-A2X-Relay"] = "1"
    return filtered


def _blocked_address(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return (
        address == _METADATA_IP
        or address.is_link_local
        or address.is_multicast
        or address.is_unspecified
        or address.is_reserved
    )


def validate_target_url(url: str, config: RelayConfig) -> None:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise RelayError(422, "relay_invalid_target", "Agent Card contains an invalid HTTP target")
    if parsed.username or parsed.password:
        raise RelayError(403, "relay_target_forbidden", "Target URL credentials are forbidden")
    if parsed.fragment:
        raise RelayError(422, "relay_invalid_target", "Target URL fragments are not supported")
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as exc:
        raise RelayError(422, "relay_invalid_target", "Target URL contains an invalid port") from exc

    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError as exc:
        raise RelayError(
            403,
            "relay_target_forbidden",
            "Relay targets must use an IP address instead of a hostname",
        ) from exc
    if _blocked_address(address):
        raise RelayError(403, "relay_target_forbidden", "Target uses a forbidden address")
    if config.allow_all_targets:
        return

    host = parsed.hostname
    if ":" in host:
        host = f"[{host}]"
    try:
        origin = normalize_origin(
            f"{parsed.scheme}://{host}:{port}"
        )
    except ValueError as exc:
        raise RelayError(422, "relay_invalid_target", str(exc)) from exc

    # Exact origins are an explicit operator grant after the hard address
    # checks above. IP literals avoid a second DNS resolution in httpx.
    if origin in config.allowed_origins:
        return

    if config.allowed_ports and port not in config.allowed_ports:
        raise RelayError(403, "relay_target_forbidden", f"Target port {port} is not allowed")

    if not config.allowed_cidrs or not any(
        address in network for network in config.allowed_cidrs
    ):
        raise RelayError(403, "relay_target_forbidden", "Target is outside the relay allowlist")
