"""Loopback integration test. Start uvicorn first, then run this script."""
import asyncio
import json
import urllib.request

import websockets

BASE = "http://127.0.0.1:17779"


def request(method, path, body=None, headers=None):
    payload = None if body is None else json.dumps(body).encode("utf-8")
    request_object = urllib.request.Request(
        BASE + path,
        data=payload,
        headers={"Content-Type": "application/json", **(headers or {})},
        method=method,
    )
    with urllib.request.urlopen(request_object, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def relay_frame(target, channel, payload):
    target_bytes = target.encode("utf-8")
    return b"UCH1" + bytes((len(target_bytes),)) + target_bytes + bytes((channel,)) + payload


def unframe(message):
    assert message[:4] == b"UCH1", message
    sender_size = message[4]
    channel_index = 5 + sender_size
    sender = message[5:channel_index].decode("utf-8")
    channel = message[channel_index]
    return sender, channel, message[channel_index + 1:]


async def relay_round_trip(room_id, token):
    host_url = f"ws://127.0.0.1:17779/ws/{room_id}?role=host&token={token}"
    client_url = f"ws://127.0.0.1:17779/ws/{room_id}?role=client"
    original_payload = bytes((0, 4, 255, 1, 0, 42))
    async with websockets.connect(host_url, proxy=None) as host:
        host_connected = json.loads(await asyncio.wait_for(host.recv(), 3))
        assert host_connected["type"] == "connected", host_connected
        async with websockets.connect(client_url, proxy=None) as client:
            client_connected = json.loads(await asyncio.wait_for(client.recv(), 3))
            assert client_connected["type"] == "connected", client_connected
            peer_joined = json.loads(await asyncio.wait_for(host.recv(), 3))
            assert peer_joined["type"] == "peerJoined", peer_joined
            assert client_connected["peerId"] == peer_joined["peerId"]
            live_listing = await asyncio.to_thread(request, "GET", "/api/servers")
            assert any(room["id"] == room_id and room["players"] == 2 for room in live_listing["servers"])
            await client.send(relay_frame("", 3, original_payload))
            sender, channel, received = unframe(await asyncio.wait_for(host.recv(), 3))
            assert sender == client_connected["peerId"]
            assert channel == 3 and received == original_payload
            await host.send(relay_frame(client_connected["peerId"], 2, original_payload))
            sender, channel, received = unframe(await asyncio.wait_for(client.recv(), 3))
            assert sender == host_connected["peerId"]
            assert channel == 2 and received == original_payload
        peer_left = json.loads(await asyncio.wait_for(host.recv(), 3))
        assert peer_left["type"] == "peerLeft", peer_left
        closed_listing = await asyncio.to_thread(request, "GET", "/api/servers")
        assert any(room["id"] == room_id and room["players"] == 1 for room in closed_listing["servers"])


async def client_first_round_trip(room_id, token):
    """Exercise the join-order race where the client WebSocket arrives first."""
    host_url = f"ws://127.0.0.1:17779/ws/{room_id}?role=host&token={token}"
    client_url = f"ws://127.0.0.1:17779/ws/{room_id}?role=client"
    payload = bytes((9, 0, 255, 6, 1, 0, 33))
    async with websockets.connect(client_url, proxy=None) as client:
        client_connected = json.loads(await asyncio.wait_for(client.recv(), 3))
        assert client_connected["type"] == "connected", client_connected
        async with websockets.connect(host_url, proxy=None) as host:
            host_connected = json.loads(await asyncio.wait_for(host.recv(), 3))
            assert host_connected["type"] == "connected", host_connected
            existing_peer = json.loads(await asyncio.wait_for(host.recv(), 3))
            assert existing_peer["type"] == "peerJoined", existing_peer
            assert existing_peer["peerId"] == client_connected["peerId"], existing_peer
            await client.send(relay_frame("", 5, payload))
            sender, channel, received = unframe(await asyncio.wait_for(host.recv(), 3))
            assert sender == client_connected["peerId"]
            assert channel == 5 and received == payload
            await host.send(relay_frame(client_connected["peerId"], 4, payload))
            sender, channel, received = unframe(await asyncio.wait_for(client.recv(), 3))
            assert sender == host_connected["peerId"]
            assert channel == 4 and received == payload


def main():
    registration = request(
        "POST",
        "/api/servers/register",
        {
            "name": "UCH loopback smoke",
            "address": "127.0.0.1",
            "port": 17778,
            "capacity": 4,
            "players": 1,
            "transport": "websocket",
        },
    )
    room_id = registration["server"]["id"]
    token = registration["token"]
    try:
        asyncio.run(relay_round_trip(room_id, token))
        asyncio.run(client_first_round_trip(room_id, token))
        heartbeat = request(
            "POST",
            f"/api/servers/{room_id}/heartbeat",
            {"players": 1, "state": "lobby"},
            {"X-Uch-Host-Token": token},
        )
        assert heartbeat["server"]["players"] == 1
        listing = request("GET", "/api/servers")
        assert any(room["id"] == room_id and room["players"] == 1 and room["transport"] == "websocket" for room in listing["servers"])
        request("DELETE", f"/api/servers/{room_id}", headers={"X-Uch-Host-Token": token})
        print("PASS: register, live-player count, heartbeat, host-first and client-first handshakes, bidirectional UCH1 payload relay, disconnect, delete")
    finally:
        try:
            request("DELETE", f"/api/servers/{room_id}", headers={"X-Uch-Host-Token": token})
        except Exception:
            pass


if __name__ == "__main__":
    main()
