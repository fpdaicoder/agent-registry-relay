from __future__ import annotations

import hashlib
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI
from fastapi.testclient import TestClient

from a2x_registry.artifact_relay.config import ArtifactRelayConfig
from a2x_registry.artifact_relay.deps import require_artifact_relay_service
from a2x_registry.artifact_relay.router import router
from a2x_registry.artifact_relay.service import ArtifactRelayService
from a2x_registry.auth.deps import set_auth_store


def _app(tmp_path: Path, *, chunk_bytes: int = 1024 * 1024):
    config = ArtifactRelayConfig(
        enabled=True,
        public_base_url="https://registry.example",
        storage_dir=tmp_path,
        create_token="create-token-" + "x" * 32,
        max_object_bytes=32 * 1024 * 1024,
        chunk_bytes=chunk_bytes,
        default_ttl_seconds=60,
        max_ttl_seconds=60,
        cleanup_interval_seconds=60,
    )
    service = ArtifactRelayService(config)
    service._ensure_storage()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[require_artifact_relay_service] = lambda: service
    set_auth_store(None)
    return app, service


def test_twenty_mib_chunked_roundtrip_and_ranges(tmp_path):
    app, service = _app(tmp_path)
    payload = (bytes(range(256)) * (20 * 1024 * 1024 // 256))
    sha256 = hashlib.sha256(payload).hexdigest()

    with TestClient(app) as client:
        created_response = client.post(
            "/api/artifact-relay/transfers",
            headers={"X-Artifact-Relay-Key": service.config.create_token},
            json={
                "filename": "../twenty.bin",
                "byteLength": len(payload),
                "sha256": sha256,
                "mediaType": "application/octet-stream",
                "ttlSeconds": 60,
            },
        )
        assert created_response.status_code == 201
        created = created_response.json()
        assert created["chunkBytes"] == 1024 * 1024
        assert created["download"]["url"].startswith("https://registry.example/")
        assert created["download"]["signedUrl"].startswith(
            "https://registry.example/api/artifact-relay/uri/"
        )

        transfer_id = created["transferId"]
        upload_token = created["upload"]["token"]
        download_token = created["download"]["token"]
        chunk_bytes = created["chunkBytes"]
        for start in range(0, len(payload), chunk_bytes):
            chunk = payload[start : start + chunk_bytes]
            end = start + len(chunk) - 1
            response = client.put(
                f"/api/artifact-relay/transfers/{transfer_id}",
                headers={
                    "Authorization": f"Bearer {upload_token}",
                    "Content-Range": f"bytes {start}-{end}/{len(payload)}",
                    "Content-Length": str(len(chunk)),
                    "Content-Type": "application/octet-stream",
                },
                content=chunk,
            )
            assert response.status_code == 200, response.text

        status = client.get(
            f"/api/artifact-relay/transfers/{transfer_id}/status",
            headers={"Authorization": f"Bearer {download_token}"},
        )
        assert status.status_code == 200
        assert status.json()["state"] == "ready"

        full = client.get(
            f"/api/artifact-relay/transfers/{transfer_id}",
            headers={"Authorization": f"Bearer {download_token}"},
        )
        assert full.status_code == 200
        assert full.content == payload
        assert full.headers["x-artifact-sha256"] == sha256
        assert full.headers["accept-ranges"] == "bytes"

        signed_path = urlsplit(created["download"]["signedUrl"]).path
        signed = client.get(signed_path)
        assert signed.status_code == 200
        assert signed.content == payload
        assert signed.headers["x-artifact-sha256"] == sha256

        wrong_signed = client.get(f"{signed_path}-wrong")
        assert wrong_signed.status_code == 401

        uri_token = signed_path.rsplit("/", 1)[-1]
        cannot_delete = client.delete(
            f"/api/artifact-relay/transfers/{transfer_id}",
            headers={"Authorization": f"Bearer {uri_token}"},
        )
        assert cannot_delete.status_code == 401

        partial = client.get(
            f"/api/artifact-relay/transfers/{transfer_id}",
            headers={
                "Authorization": f"Bearer {download_token}",
                "Range": "bytes=1048576-2097151",
            },
        )
        assert partial.status_code == 206
        assert partial.content == payload[1048576:2097152]
        assert partial.headers["content-range"] == (
            f"bytes 1048576-2097151/{len(payload)}"
        )

        head = client.head(
            f"/api/artifact-relay/transfers/{transfer_id}",
            headers={"Authorization": f"Bearer {download_token}"},
        )
        assert head.status_code == 200
        assert head.headers["content-length"] == str(len(payload))

        deleted = client.delete(
            f"/api/artifact-relay/transfers/{transfer_id}",
            headers={"Authorization": f"Bearer {download_token}"},
        )
        assert deleted.status_code == 204

    assert service._metrics == {
        "createdTransfers": 1,
        "completedTransfers": 1,
        "deletedTransfers": 1,
        "expiredTransfers": 0,
        "uploadedBytes": len(payload),
        "downloadedBytes": len(payload) * 2 + 1024 * 1024,
        "hashFailures": 0,
    }


def test_rejects_wrong_token_offset_oversized_chunk_and_hash(tmp_path):
    app, service = _app(tmp_path, chunk_bytes=4)
    payload = b"abcdefgh"
    sha256 = hashlib.sha256(payload).hexdigest()
    with TestClient(app) as client:
        created = client.post(
            "/api/artifact-relay/transfers",
            headers={"X-Artifact-Relay-Key": service.config.create_token},
            json={
                "filename": "small.bin",
                "byteLength": len(payload),
                "sha256": sha256,
            },
        ).json()
        transfer_id = created["transferId"]
        token = created["upload"]["token"]

        wrong_token = client.put(
            f"/api/artifact-relay/transfers/{transfer_id}",
            headers={
                "Authorization": "Bearer wrong",
                "Content-Range": "bytes 0-3/8",
                "Content-Length": "4",
            },
            content=b"abcd",
        )
        assert wrong_token.status_code == 401

        wrong_offset = client.put(
            f"/api/artifact-relay/transfers/{transfer_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Range": "bytes 4-7/8",
                "Content-Length": "4",
            },
            content=b"efgh",
        )
        assert wrong_offset.status_code == 409
        assert wrong_offset.json()["error"]["details"]["expectedOffset"] == 0

        oversized = client.put(
            f"/api/artifact-relay/transfers/{transfer_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Range": "bytes 0-7/8",
                "Content-Length": "8",
            },
            content=payload,
        )
        assert oversized.status_code == 413

        first = client.put(
            f"/api/artifact-relay/transfers/{transfer_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Range": "bytes 0-3/8",
                "Content-Length": "4",
            },
            content=b"xxxx",
        )
        assert first.status_code == 200
        final = client.put(
            f"/api/artifact-relay/transfers/{transfer_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Range": "bytes 4-7/8",
                "Content-Length": "4",
            },
            content=b"yyyy",
        )
        assert final.status_code == 422
        assert final.json()["error"]["code"] == "artifact_sha256_mismatch"


def test_create_and_global_status_require_scoped_key(tmp_path):
    app, service = _app(tmp_path)
    with TestClient(app) as client:
        denied_create = client.post(
            "/api/artifact-relay/transfers",
            json={
                "filename": "denied.bin",
                "byteLength": 1,
                "sha256": hashlib.sha256(b"x").hexdigest(),
            },
        )
        assert denied_create.status_code == 401
        assert (
            denied_create.json()["error"]["code"]
            == "artifact_create_token_invalid"
        )

        denied_status = client.get("/api/artifact-relay/status")
        assert denied_status.status_code == 401

        allowed_status = client.get(
            "/api/artifact-relay/status",
            headers={"X-Artifact-Relay-Key": service.config.create_token},
        )
        assert allowed_status.status_code == 200
        assert allowed_status.json()["strictP2P"] is False
