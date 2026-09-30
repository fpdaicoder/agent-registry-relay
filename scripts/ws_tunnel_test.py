"""WebSocket tunnel integration test client.

Connects to the registry's WebSocket tunnel, sends a register frame and
waits for the registered response, then verifies the control channel with
a ping/pong round trip.

Usage:
    python ws_tunnel_test.py                          # defaults below
    python ws_tunnel_test.py --device-id probe-test-1
    python ws_tunnel_test.py --host 121.37.53.35 --port 8005 --token secret
"""

import argparse
import asyncio
import json
import sys

try:
    import websockets
except ImportError:
    sys.exit("missing dependency: pip install websockets")


async def run(host: str, port: int, device_id: str, token: str | None) -> int:
    uri = f"ws://{host}:{port}"
    print(f"[*] connecting to {uri} ...")

    try:
        async with websockets.connect(uri, open_timeout=10) as ws:
            print(f"[+] connected")

            # ---- 1. register ---------------------------------------------
            register: dict = {"type": "register", "device_id": device_id}
            if token:
                register["token"] = token
            try:
                await ws.send(json.dumps(register))
            except websockets.exceptions.ConnectionClosed as exc:
                print(f"[-] connection closed before sending register: code={exc.code} reason={exc.reason}")
                return 1
            print(f"[*] sent register: {register}")

            # ---- 2. wait for registered response -------------------------
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
            except asyncio.TimeoutError:
                print("[-] no response within 10s (registration timeout)")
                return 1
            except websockets.exceptions.ConnectionClosed as exc:
                print(f"[-] connection closed before response: code={exc.code} reason={exc.reason}")
                return 1

            try:
                response = json.loads(raw)
            except json.JSONDecodeError:
                print(f"[-] non-JSON response: {raw!r}")
                return 1

            print(f"[*] received: {json.dumps(response, ensure_ascii=False, indent=2)}")

            if response.get("type") == "registered":
                print(f"[+] registered as {response.get('device_id')}")
            elif response.get("type") == "error":
                print(f"[-] registration rejected: {response.get('error')}")
                return 1
            else:
                print(f"[-] unexpected response type: {response.get('type')}")
                return 1

            # ---- 3. ping/pong round trip ---------------------------------
            await ws.send(json.dumps({"type": "ping"}))
            print("[*] sent ping")
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                pong = json.loads(raw)
            except (asyncio.TimeoutError, json.JSONDecodeError) as exc:
                print(f"[-] ping/pong failed: {exc!r}")
                return 1
            if pong.get("type") == "pong":
                print("[+] pong received — control channel is alive")
            else:
                print(f"[-] expected pong, got: {pong}")
                return 1

            print("\n=== RESULT: PASS ===")
            print(f"device_id: {device_id}")
            print(f"registered: yes, control channel: yes")
            return 0
    except Exception as exc:
        print(f"[-] connect failed: {exc!r}")
        return 1


def main() -> None:
    parser = argparse.ArgumentParser(description="WS tunnel register test")
    parser.add_argument("--host", default="121.37.53.35")
    parser.add_argument("--port", type=int, default=8005)
    parser.add_argument("--device-id", default="probe-test-1")
    parser.add_argument("--token", default=None, help="tunnel shared token, if configured")
    args = parser.parse_args()

    sys.exit(asyncio.run(run(args.host, args.port, args.device_id, args.token)))


if __name__ == "__main__":
    main()
