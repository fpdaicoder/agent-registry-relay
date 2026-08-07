from __future__ import annotations

import hashlib

from fastapi.testclient import TestClient

from a2x_registry.stream_proxy.app import create_app
from a2x_registry.stream_proxy.config import StreamProxyConfig


CREATE_TOKEN = "create-" + "x" * 32
SUBPROTOCOL = "a2x.artifact.stream.v1"


def _app(*, max_chunk_bytes: int = 1024):
    config = StreamProxyConfig(
        public_ws_base_url="ws://proxy.example",
        create_token=CREATE_TOKEN,
        max_object_bytes=1024 * 1024,
        max_chunk_bytes=max_chunk_bytes,
        max_sessions=8,
        session_ttl_seconds=60,
        cleanup_interval_seconds=60,
        auth_timeout_seconds=2,
        reconnect_grace_seconds=30,
    )
    return create_app(config)


def _create(client: TestClient, payload: bytes):
    response = client.post(
        "/api/stream-proxy/sessions",
        headers={"X-Stream-Proxy-Key": CREATE_TOKEN},
        json={
            "filename": "../payload.bin",
            "byteLength": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "ttlSeconds": 60,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def _path(url: str) -> str:
    return "/" + url.split("/", 3)[-1]


def test_binary_roundtrip_ack_completion_and_metrics():
    payload = bytes(range(256)) * 8
    expected_sha256 = hashlib.sha256(payload).hexdigest()
    app = _app(max_chunk_bytes=1024)
    with TestClient(app) as client:
        created = _create(client, payload)
        assert created["filename"] == "payload.bin"
        with client.websocket_connect(
            _path(created["sender"]["url"]),
            subprotocols=[SUBPROTOCOL],
        ) as sender:
            sender.send_json(
                {
                    "type": "auth",
                    "token": created["sender"]["token"],
                    "resumeOffset": 0,
                }
            )
            sender_registered = sender.receive_json()
            assert sender_registered["type"] == "registered"
            with client.websocket_connect(
                _path(created["receiver"]["url"]),
                subprotocols=[SUBPROTOCOL],
            ) as receiver:
                receiver.send_json(
                    {
                        "type": "auth",
                        "token": created["receiver"]["token"],
                        "resumeOffset": 0,
                    }
                )
                receiver_registered = receiver.receive_json()
                assert receiver_registered["type"] == "registered"
                assert receiver.receive_json()["type"] == "paired"
                ready = sender.receive_json()
                assert ready == {
                    "type": "ready",
                    "transferId": created["transferId"],
                    "resumeOffset": 0,
                    "chunkBytes": 1024,
                }

                offset = 0
                for chunk in (payload[:1024], payload[1024:]):
                    frame = offset.to_bytes(8, "big") + chunk
                    sender.send_bytes(frame)
                    assert receiver.receive_bytes() == frame
                    offset += len(chunk)
                    receiver.send_json({"type": "ack", "offset": offset})
                    assert sender.receive_json() == {
                        "type": "ack",
                        "offset": offset,
                    }

                sender.send_json(
                    {
                        "type": "fin",
                        "byteLength": len(payload),
                        "sha256": expected_sha256,
                    }
                )
                assert receiver.receive_json() == {
                    "type": "fin",
                    "byteLength": len(payload),
                    "sha256": expected_sha256,
                }
                receiver.send_json(
                    {
                        "type": "complete",
                        "byteLength": len(payload),
                        "sha256": expected_sha256,
                    }
                )
                assert sender.receive_json() == {
                    "type": "complete",
                    "byteLength": len(payload),
                    "sha256": expected_sha256,
                }

        status = client.get(
            "/api/stream-proxy/status",
            headers={"X-Stream-Proxy-Key": CREATE_TOKEN},
        )
        assert status.status_code == 200
        assert status.json()["states"] == {"completed": 1}
        assert status.json()["completedSessions"] == 1
        assert status.json()["ingressBytes"] == len(payload)
        assert status.json()["egressBytes"] == len(payload)


def test_wrong_role_token_is_rejected_without_exposing_expected_token():
    payload = b"test"
    app = _app()
    with TestClient(app) as client:
        created = _create(client, payload)
        with client.websocket_connect(
            _path(created["sender"]["url"]),
            subprotocols=[SUBPROTOCOL],
        ) as sender:
            sender.send_json(
                {
                    "type": "auth",
                    "token": created["receiver"]["token"],
                    "resumeOffset": 0,
                }
            )
            error = sender.receive_json()
            assert error["type"] == "error"
            assert error["code"] == "stream_token_invalid"
            assert created["sender"]["token"] not in str(error)


def test_offset_mismatch_fails_closed_before_forwarding_data():
    payload = b"abcdefgh"
    app = _app()
    with TestClient(app) as client:
        created = _create(client, payload)
        with client.websocket_connect(
            _path(created["sender"]["url"]),
            subprotocols=[SUBPROTOCOL],
        ) as sender:
            sender.send_json(
                {
                    "type": "auth",
                    "token": created["sender"]["token"],
                    "resumeOffset": 0,
                }
            )
            assert sender.receive_json()["type"] == "registered"
            with client.websocket_connect(
                _path(created["receiver"]["url"]),
                subprotocols=[SUBPROTOCOL],
            ) as receiver:
                receiver.send_json(
                    {
                        "type": "auth",
                        "token": created["receiver"]["token"],
                        "resumeOffset": 0,
                    }
                )
                assert receiver.receive_json()["type"] == "registered"
                assert receiver.receive_json()["type"] == "paired"
                assert sender.receive_json()["type"] == "ready"
                sender.send_bytes((1).to_bytes(8, "big") + payload)
                error = sender.receive_json()
                assert error["type"] == "error"
                assert error["code"] == "stream_offset_mismatch"


def test_control_endpoints_require_scoped_create_key():
    app = _app()
    with TestClient(app) as client:
        denied = client.post(
            "/api/stream-proxy/sessions",
            json={
                "filename": "x",
                "byteLength": 1,
                "sha256": hashlib.sha256(b"x").hexdigest(),
            },
        )
        assert denied.status_code == 401
        denied_status = client.get("/api/stream-proxy/status")
        assert denied_status.status_code == 401
