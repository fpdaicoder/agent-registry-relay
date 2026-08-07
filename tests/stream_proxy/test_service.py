import asyncio

from a2x_registry.stream_proxy.config import StreamProxyConfig
from a2x_registry.stream_proxy.service import StreamProxyService


class _Socket:
    def __init__(self):
        self.sent = []

    async def send_json(self, payload):
        self.sent.append(payload)


def test_completed_peer_close_does_not_emit_false_disconnect():
    async def scenario():
        service = StreamProxyService(
            StreamProxyConfig(
                create_token="x" * 32,
                session_ttl_seconds=60,
                reconnect_grace_seconds=30,
            )
        )
        created = await service.create_session(
            filename="payload.bin",
            byte_length=4,
            sha256="a" * 64,
            ttl_seconds=60,
        )
        session = service.get_session(created["transferId"])
        sender = _Socket()
        receiver = _Socket()
        await service.attach(
            session,
            "sender",
            sender,
            resume_offset=0,
        )
        await service.attach(
            session,
            "receiver",
            receiver,
            resume_offset=0,
        )
        session.state = "completed"
        await service.detach(session, "receiver", receiver)
        assert sender.sent == []

    asyncio.run(scenario())
