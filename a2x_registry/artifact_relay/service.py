"""Persistent, token-scoped artifact relay with sequential chunk uploads."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .config import ArtifactRelayConfig
from .errors import ArtifactRelayError


_TRANSFER_ID_RE = re.compile(r"^[a-f0-9]{32}$")
_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
_CONTENT_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")


def _utc_iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace(
        "+00:00",
        "Z",
    )


def _token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _safe_filename(value: str) -> str:
    leaf = Path(value.replace("\\", "/")).name
    leaf = leaf.replace("\r", "").replace("\n", "").strip()
    if not leaf:
        return "artifact.bin"
    return leaf[:255]


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class DownloadSlice:
    path: Path
    start: int
    end: int
    total: int
    filename: str
    sha256: str
    partial: bool

    @property
    def length(self) -> int:
        return self.end - self.start + 1


class ArtifactRelayService:
    def __init__(self, config: ArtifactRelayConfig):
        self.config = config
        self._locks: dict[str, asyncio.Lock] = {}
        self._cleanup_task: asyncio.Task[None] | None = None
        self._metrics = {
            "createdTransfers": 0,
            "completedTransfers": 0,
            "deletedTransfers": 0,
            "expiredTransfers": 0,
            "uploadedBytes": 0,
            "downloadedBytes": 0,
            "hashFailures": 0,
        }

    async def start(self) -> None:
        self.config.validate()
        await asyncio.to_thread(self._ensure_storage)
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    def require_create_token(self, token: str | None) -> None:
        if (
            not isinstance(token, str)
            or not hmac.compare_digest(token, self.config.create_token)
        ):
            raise ArtifactRelayError(
                401,
                "artifact_create_token_invalid",
                "Invalid artifact relay create token",
            )

    async def stop(self) -> None:
        task, self._cleanup_task = self._cleanup_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def _ensure_storage(self) -> None:
        self.config.storage_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.config.storage_dir, 0o700)

    def _metadata_path(self, transfer_id: str) -> Path:
        return self.config.storage_dir / f"{transfer_id}.json"

    def _partial_path(self, transfer_id: str) -> Path:
        return self.config.storage_dir / f"{transfer_id}.part"

    def _blob_path(self, transfer_id: str) -> Path:
        return self.config.storage_dir / f"{transfer_id}.blob"

    def _validate_transfer_id(self, transfer_id: str) -> None:
        if not _TRANSFER_ID_RE.fullmatch(transfer_id):
            raise ArtifactRelayError(
                404,
                "artifact_not_found",
                "Artifact transfer was not found",
            )

    def _lock_for(self, transfer_id: str) -> asyncio.Lock:
        return self._locks.setdefault(transfer_id, asyncio.Lock())

    def _read_metadata(self, transfer_id: str) -> dict[str, Any]:
        self._validate_transfer_id(transfer_id)
        try:
            metadata = json.loads(
                self._metadata_path(transfer_id).read_text(encoding="utf-8")
            )
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            raise ArtifactRelayError(
                404,
                "artifact_not_found",
                "Artifact transfer was not found",
            ) from exc
        if not isinstance(metadata, dict):
            raise ArtifactRelayError(
                500,
                "artifact_metadata_invalid",
                "Artifact transfer metadata is invalid",
            )
        return metadata

    def _write_metadata(self, transfer_id: str, metadata: dict[str, Any]) -> None:
        target = self._metadata_path(transfer_id)
        temporary = target.with_suffix(f".json.{uuid.uuid4().hex}.tmp")
        with temporary.open("x", encoding="utf-8") as handle:
            os.chmod(temporary, 0o600)
            json.dump(metadata, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)

    @staticmethod
    def _require_token(metadata: dict[str, Any], token: str, purpose: str) -> None:
        expected = metadata.get(f"{purpose}TokenSha256")
        if (
            not isinstance(token, str)
            or not token
            or not isinstance(expected, str)
            or not hmac.compare_digest(_token_digest(token), expected)
        ):
            raise ArtifactRelayError(
                401,
                "artifact_token_invalid",
                f"Invalid {purpose} token",
            )

    def _require_live(self, metadata: dict[str, Any]) -> None:
        if float(metadata.get("expiresAtEpoch", 0)) <= time.time():
            raise ArtifactRelayError(
                410,
                "artifact_expired",
                "Artifact transfer has expired",
            )

    async def create_transfer(
        self,
        *,
        filename: str,
        byte_length: int,
        sha256: str,
        media_type: str,
        ttl_seconds: int | None,
    ) -> dict[str, Any]:
        if (
            isinstance(byte_length, bool)
            or byte_length <= 0
            or byte_length > self.config.max_object_bytes
        ):
            raise ArtifactRelayError(
                400,
                "artifact_size_invalid",
                "byteLength must be within the configured object limit",
                {"maxObjectBytes": self.config.max_object_bytes},
            )
        if not isinstance(sha256, str) or not _SHA256_RE.fullmatch(sha256):
            raise ArtifactRelayError(
                400,
                "artifact_sha256_invalid",
                "sha256 must be 64 lowercase hexadecimal characters",
            )
        ttl = self.config.default_ttl_seconds if ttl_seconds is None else ttl_seconds
        if (
            isinstance(ttl, bool)
            or ttl <= 0
            or ttl > self.config.max_ttl_seconds
        ):
            raise ArtifactRelayError(
                400,
                "artifact_ttl_invalid",
                "ttlSeconds must be within the configured limit",
                {"maxTtlSeconds": self.config.max_ttl_seconds},
            )
        if not isinstance(media_type, str) or not media_type or len(media_type) > 255:
            raise ArtifactRelayError(
                400,
                "artifact_media_type_invalid",
                "mediaType must be a non-empty string of at most 255 characters",
            )

        transfer_id = uuid.uuid4().hex
        upload_token = secrets.token_urlsafe(32)
        download_token = secrets.token_urlsafe(32)
        uri_token = secrets.token_urlsafe(32)
        created_at = time.time()
        expires_at = created_at + ttl
        metadata = {
            "schemaVersion": "1.0",
            "transferId": transfer_id,
            "filename": _safe_filename(filename),
            "mediaType": media_type,
            "byteLength": byte_length,
            "sha256": sha256,
            "receivedBytes": 0,
            "state": "uploading",
            "createdAt": _utc_iso(created_at),
            "expiresAt": _utc_iso(expires_at),
            "expiresAtEpoch": expires_at,
            "uploadTokenSha256": _token_digest(upload_token),
            "downloadTokenSha256": _token_digest(download_token),
            "uriTokenSha256": _token_digest(uri_token),
        }
        await asyncio.to_thread(self._ensure_storage)
        await asyncio.to_thread(self._write_metadata, transfer_id, metadata)
        part_path = self._partial_path(transfer_id)
        await asyncio.to_thread(part_path.touch, 0o600, False)
        await asyncio.to_thread(os.chmod, part_path, 0o600)
        self._metrics["createdTransfers"] += 1

        base = self.config.public_base_url
        transfer_url = f"{base}/api/artifact-relay/transfers/{transfer_id}"
        return {
            "transferId": transfer_id,
            "state": "uploading",
            "byteLength": byte_length,
            "sha256": sha256,
            "chunkBytes": self.config.chunk_bytes,
            "expiresAt": metadata["expiresAt"],
            "upload": {
                "url": transfer_url,
                "token": upload_token,
                "method": "PUT",
            },
            "download": {
                "url": transfer_url,
                "token": download_token,
                "method": "GET",
                "signedUrl": (
                    f"{base}/api/artifact-relay/uri/{transfer_id}/{uri_token}"
                ),
            },
            "statusUrl": f"{transfer_url}/status",
        }

    @staticmethod
    def parse_content_range(value: str | None) -> tuple[int, int, int]:
        match = _CONTENT_RANGE_RE.fullmatch(value or "")
        if match is None:
            raise ArtifactRelayError(
                400,
                "artifact_content_range_invalid",
                "Content-Range must use bytes START-END/TOTAL",
            )
        start, end, total = (int(part) for part in match.groups())
        if start > end or end >= total:
            raise ArtifactRelayError(
                400,
                "artifact_content_range_invalid",
                "Content-Range bounds are invalid",
            )
        return start, end, total

    async def append_chunk(
        self,
        transfer_id: str,
        token: str,
        content_range: str | None,
        declared_length: int | None,
        chunks: AsyncIterator[bytes],
    ) -> dict[str, Any]:
        start, end, total = self.parse_content_range(content_range)
        expected_chunk_bytes = end - start + 1
        if expected_chunk_bytes > self.config.chunk_bytes:
            raise ArtifactRelayError(
                413,
                "artifact_chunk_too_large",
                "Artifact chunk exceeds the configured chunk limit",
                {"chunkBytes": self.config.chunk_bytes},
            )
        if declared_length is None or declared_length != expected_chunk_bytes:
            raise ArtifactRelayError(
                400,
                "artifact_length_mismatch",
                "Content-Length must match Content-Range",
            )

        async with self._lock_for(transfer_id):
            metadata = await asyncio.to_thread(self._read_metadata, transfer_id)
            self._require_live(metadata)
            self._require_token(metadata, token, "upload")
            if metadata.get("state") != "uploading":
                raise ArtifactRelayError(
                    409,
                    "artifact_not_uploading",
                    "Artifact transfer is not accepting uploads",
                )
            expected_total = int(metadata["byteLength"])
            expected_offset = int(metadata["receivedBytes"])
            if total != expected_total:
                raise ArtifactRelayError(
                    409,
                    "artifact_total_mismatch",
                    "Content-Range total does not match the declared object size",
                    {"expectedTotal": expected_total},
                )
            if start != expected_offset:
                raise ArtifactRelayError(
                    409,
                    "artifact_offset_mismatch",
                    "Chunk does not start at the next expected offset",
                    {"expectedOffset": expected_offset},
                )

            partial_path = self._partial_path(transfer_id)
            written = 0
            try:
                with partial_path.open("r+b") as handle:
                    handle.seek(start)
                    async for chunk in chunks:
                        written += len(chunk)
                        if written > expected_chunk_bytes:
                            raise ArtifactRelayError(
                                400,
                                "artifact_length_mismatch",
                                "Request body exceeded Content-Range",
                            )
                        handle.write(chunk)
                    if written != expected_chunk_bytes:
                        raise ArtifactRelayError(
                            400,
                            "artifact_length_mismatch",
                            "Request body was shorter than Content-Range",
                        )
                    handle.flush()
                    os.fsync(handle.fileno())
            except BaseException:
                with partial_path.open("r+b") as handle:
                    handle.truncate(start)
                raise

            metadata["receivedBytes"] = end + 1
            self._metrics["uploadedBytes"] += written
            if metadata["receivedBytes"] == expected_total:
                actual_sha256 = await asyncio.to_thread(_hash_file, partial_path)
                if actual_sha256 != metadata["sha256"]:
                    metadata["state"] = "failed"
                    metadata["failure"] = "sha256_mismatch"
                    await asyncio.to_thread(
                        self._write_metadata,
                        transfer_id,
                        metadata,
                    )
                    self._metrics["hashFailures"] += 1
                    raise ArtifactRelayError(
                        422,
                        "artifact_sha256_mismatch",
                        "Uploaded artifact did not match the declared SHA-256",
                        {
                            "expectedSha256": metadata["sha256"],
                            "actualSha256": actual_sha256,
                        },
                    )
                await asyncio.to_thread(
                    os.replace,
                    partial_path,
                    self._blob_path(transfer_id),
                )
                metadata["state"] = "ready"
                metadata["completedAt"] = _utc_iso(time.time())
                self._metrics["completedTransfers"] += 1
            await asyncio.to_thread(self._write_metadata, transfer_id, metadata)
            return self._public_metadata(metadata)

    def _public_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
        return {
            key: metadata[key]
            for key in (
                "transferId",
                "state",
                "filename",
                "mediaType",
                "byteLength",
                "sha256",
                "receivedBytes",
                "createdAt",
                "expiresAt",
                "completedAt",
                "failure",
            )
            if key in metadata
        }

    async def transfer_status(
        self,
        transfer_id: str,
        token: str,
    ) -> dict[str, Any]:
        metadata = await asyncio.to_thread(self._read_metadata, transfer_id)
        self._require_live(metadata)
        try:
            self._require_token(metadata, token, "upload")
        except ArtifactRelayError:
            self._require_token(metadata, token, "download")
        return self._public_metadata(metadata)

    async def prepare_download(
        self,
        transfer_id: str,
        token: str,
        range_header: str | None,
    ) -> DownloadSlice:
        return await self._prepare_download(
            transfer_id,
            token,
            range_header,
            token_purpose="download",
        )

    async def prepare_uri_download(
        self,
        transfer_id: str,
        token: str,
        range_header: str | None,
    ) -> DownloadSlice:
        return await self._prepare_download(
            transfer_id,
            token,
            range_header,
            token_purpose="uri",
        )

    async def _prepare_download(
        self,
        transfer_id: str,
        token: str,
        range_header: str | None,
        *,
        token_purpose: str,
    ) -> DownloadSlice:
        metadata = await asyncio.to_thread(self._read_metadata, transfer_id)
        self._require_live(metadata)
        self._require_token(metadata, token, token_purpose)
        if metadata.get("state") != "ready":
            raise ArtifactRelayError(
                409,
                "artifact_not_ready",
                "Artifact is not ready for download",
                {"state": metadata.get("state")},
            )
        total = int(metadata["byteLength"])
        start, end, partial = 0, total - 1, False
        if range_header:
            if not range_header.startswith("bytes=") or "," in range_header:
                raise ArtifactRelayError(
                    416,
                    "artifact_range_invalid",
                    "Only one byte range is supported",
                    {"byteLength": total},
                )
            value = range_header[6:]
            first, separator, last = value.partition("-")
            try:
                if not separator:
                    raise ValueError
                if first:
                    start = int(first)
                    end = int(last) if last else total - 1
                else:
                    suffix = int(last)
                    if suffix <= 0:
                        raise ValueError
                    start = max(total - suffix, 0)
                    end = total - 1
            except ValueError as exc:
                raise ArtifactRelayError(
                    416,
                    "artifact_range_invalid",
                    "Byte range is invalid",
                    {"byteLength": total},
                ) from exc
            if start < 0 or start >= total or end < start or end >= total:
                raise ArtifactRelayError(
                    416,
                    "artifact_range_unsatisfiable",
                    "Byte range is outside the artifact",
                    {"byteLength": total},
                )
            partial = True
        return DownloadSlice(
            path=self._blob_path(transfer_id),
            start=start,
            end=end,
            total=total,
            filename=metadata["filename"],
            sha256=metadata["sha256"],
            partial=partial,
        )

    async def stream_download(self, selection: DownloadSlice) -> AsyncIterator[bytes]:
        remaining = selection.length
        with selection.path.open("rb") as handle:
            handle.seek(selection.start)
            while remaining:
                chunk = await asyncio.to_thread(
                    handle.read,
                    min(self.config.chunk_bytes, remaining),
                )
                if not chunk:
                    raise RuntimeError("Artifact ended before the declared length")
                remaining -= len(chunk)
                self._metrics["downloadedBytes"] += len(chunk)
                yield chunk

    async def delete_transfer(self, transfer_id: str, token: str) -> None:
        async with self._lock_for(transfer_id):
            metadata = await asyncio.to_thread(self._read_metadata, transfer_id)
            try:
                self._require_token(metadata, token, "upload")
            except ArtifactRelayError:
                self._require_token(metadata, token, "download")
            await asyncio.to_thread(self._delete_files, transfer_id)
            self._locks.pop(transfer_id, None)
            self._metrics["deletedTransfers"] += 1

    def _delete_files(self, transfer_id: str) -> None:
        for path in (
            self._metadata_path(transfer_id),
            self._partial_path(transfer_id),
            self._blob_path(transfer_id),
        ):
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    async def cleanup_expired(self) -> int:
        now = time.time()
        expired = 0
        await asyncio.to_thread(self._ensure_storage)
        for metadata_path in self.config.storage_dir.glob("*.json"):
            transfer_id = metadata_path.stem
            if not _TRANSFER_ID_RE.fullmatch(transfer_id):
                continue
            try:
                metadata = await asyncio.to_thread(self._read_metadata, transfer_id)
            except ArtifactRelayError:
                continue
            if float(metadata.get("expiresAtEpoch", 0)) > now:
                continue
            async with self._lock_for(transfer_id):
                await asyncio.to_thread(self._delete_files, transfer_id)
                self._locks.pop(transfer_id, None)
                expired += 1
        self._metrics["expiredTransfers"] += expired
        return expired

    async def _cleanup_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.config.cleanup_interval_seconds)
                await self.cleanup_expired()
        except asyncio.CancelledError:
            pass

    async def status(self) -> dict[str, Any]:
        await asyncio.to_thread(self._ensure_storage)
        states: dict[str, int] = {}
        stored_bytes = 0
        for metadata_path in self.config.storage_dir.glob("*.json"):
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            state = str(metadata.get("state", "invalid"))
            states[state] = states.get(state, 0) + 1
            transfer_id = metadata_path.stem
            for path in (
                self._partial_path(transfer_id),
                self._blob_path(transfer_id),
            ):
                try:
                    stored_bytes += path.stat().st_size
                except FileNotFoundError:
                    pass
        return {
            "enabled": True,
            "mode": "store-and-forward-relay",
            "strictP2P": False,
            "storageDir": str(self.config.storage_dir),
            "maxObjectBytes": self.config.max_object_bytes,
            "chunkBytes": self.config.chunk_bytes,
            "states": states,
            "storedBytes": stored_bytes,
            **self._metrics,
        }

    @staticmethod
    def content_disposition(filename: str) -> str:
        fallback = "".join(
            character if 32 <= ord(character) < 127 and character not in {'"', "\\"}
            else "_"
            for character in filename
        )
        encoded = quote(filename, safe="")
        return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"
