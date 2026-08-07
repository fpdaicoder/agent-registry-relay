"""Run the standalone binary stream proxy."""

from __future__ import annotations

import uvicorn

from .app import create_app
from .config import StreamProxyConfig


def main() -> None:
    config = StreamProxyConfig.from_env()
    uvicorn.run(
        create_app(config),
        host=config.host,
        port=config.port,
        access_log=True,
        ws_max_size=config.max_chunk_bytes + 8,
    )


if __name__ == "__main__":
    main()
