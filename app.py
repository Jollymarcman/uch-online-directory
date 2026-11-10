"""UCH online directory and opaque WebSocket relay.

Compatibility service for the recovered game's client-authoritative network
messages. It does not simulate game rules or physics. Rooms are short-lived
leases, refreshed by the hosting client; the directory service itself remains
available as long as its hosting service is deployed.
"""

from __future__ import annotations

import asyncio
import os
import secrets
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

LEASE_SECONDS = int(os.getenv("UCH_ROOM_LEASE_SECONDS", "60"))
MAX_PLAYERS = 4


@dataclass
class Room:
    room_id: str
    token: str
    name: str
    address: str
    port: int
    capacity: int
    players: int
    transport: str
    created_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    state: str = "lobby"
    sockets: dict[str, WebSocket] = field(default_factory=dict)
    host_peer: str | None = None


rooms: dict[str, Room] = {}
rooms_lock = asyncio.Lock()
app = FastAPI(title="UCH Online Directory", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "X-Uch-Host-Token"],
)


class RegisterRoom(BaseModel):
    name: str = Field(min_length=1, max_length=48)
    address: str = Field(min_length=1, max_length=255)
    port: int = Field(ge=1, le=65535)
    capacity: int = Field(default=4, ge=2, le=MAX_PLAYERS)
    players: int = Field(default=1, ge=1, le=MAX_PLAYERS)
    transport: Literal["direct", "websocket"] = "direct"


class Heartbeat(BaseModel):
    players: int = Field(ge=1, le=MAX_PLAYERS)
    state: str = Field(default="lobby", max_length=20)


def public_room(room: Room) -> dict[str, Any]:
    return {
        "id": room.room_id,
        "name": room.name,
        "address": room.address,
        "port": room.port,
        "players": room.players,
        "capacity": room.capacity,
        "state": room.state,
        "transport": room.transport,
        "lastSeen": int(room.last_seen),
    }


async def prune_rooms() -> None:
    now = time.time()
    async with rooms_lock:
        stale = [rid for rid, room in rooms.items() if now - room.last_seen > LEASE_SECONDS]
        for rid in stale:
            room = rooms.pop(rid)
            for ws in list(room.sockets.values()):
                try:
                    await ws.close(code=1012, reason="Room lease expired")
                except Exception:
                    pass


@app.get("/health")
async def health() -> dict[str, Any]:
    await prune_rooms()
    return {"ok": True, "rooms": len(rooms), "leaseSeconds": LEASE_SECONDS}


@app.get("/api/servers")
async def list_servers() -> dict[str, Any]:
    await prune_rooms()
    async with rooms_lock:
        result = [public_room(room) for room in rooms.values()]
    result.sort(key=lambda item: (item["players"] >= item["capacity"], item["name"].casefold()))
    return {"servers": result, "leaseSeconds": LEASE_SECONDS}


@app.post("/api/servers/register")
async def register_server(request: RegisterRoom) -> dict[str, Any]:
    await prune_rooms()
    room_id = uuid.uuid4().hex[:12]
    token = secrets.token_urlsafe(24)
    room = Room(
        room_id=room_id,
        token=token,
        name=request.name.strip(),
        address=request.address.strip(),
        port=request.port,
        capacity=request.capacity,
        players=request.players,
        transport=request.transport,
    )
    async with rooms_lock:
        rooms[room_id] = room
    return {"server": public_room(room), "token": token, "leaseSeconds": LEASE_SECONDS}


def owned_room(room_id: str, token: str | None) -> Room:
    room = rooms.get(room_id)
    if room is None:
        raise HTTPException(status_code=404, detail="Server not found")
    if not token or not secrets.compare_digest(room.token, token):
        raise HTTPException(status_code=403, detail="Invalid host token")
    return room


@app.post("/api/servers/{room_id}/heartbeat")
async def heartbeat(room_id: str, request: Heartbeat, x_uch_host_token: str | None = Header(default=None)) -> dict[str, Any]:
    await prune_rooms()
    async with rooms_lock:
        room = owned_room(room_id, x_uch_host_token)
        room.players = len(room.sockets) if room.transport == "websocket" and room.sockets else request.players
        room.state = request.state
        room.last_seen = time.time()
        return {"server": public_room(room), "leaseSeconds": LEASE_SECONDS}


@app.delete("/api/servers/{room_id}")
async def delete_server(room_id: str, x_uch_host_token: str | None = Header(default=None)) -> dict[str, bool]:
    async with rooms_lock:
        room = owned_room(room_id, x_uch_host_token)
        del rooms[room_id]
    for ws in list(room.sockets.values()):
        try:
            await ws.close(code=1001, reason="Host closed the room")
        except Exception:
            pass
    return {"deleted": True}


@app.websocket("/ws/{room_id}")
async def relay(websocket: WebSocket, room_id: str, role: str = "client", token: str | None = None) -> None:
    await websocket.accept()
    await prune_rooms()
    peer_id = uuid.uuid4().hex[:12]
    async with rooms_lock:
        room = rooms.get(room_id)
        if room is None:
            await websocket.close(code=4404, reason="Server not found")
            return
        if role not in ("host", "client"):
            await websocket.close(code=4400, reason="Role must be host or client")
            return
        if role == "host":
            if not token or not secrets.compare_digest(room.token, token):
                await websocket.close(code=4403, reason="Invalid host token")
                return
            if room.host_peer is not None:
                await websocket.close(code=4409, reason="Host already connected")
                return
            room.host_peer = peer_id
        elif len(room.sockets) >= room.capacity:
            await websocket.close(code=4409, reason="Server is full")
            return
        room.sockets[peer_id] = websocket
        room.players = len(room.sockets)
        room.last_seen = time.time()

    await websocket.send_json({"type": "connected", "peerId": peer_id, "players": len(room.sockets), "capacity": room.capacity})
    async with rooms_lock:
        # If a client wins the connection race, the later host must learn all
        # existing client IDs so it can route the original UNet payloads.
        # For the normal host-first order, notify the already-connected host
        # when each client arrives.
        if role == "host":
            peers = [(pid, sock) for pid, sock in room.sockets.items() if pid != peer_id]
            initial_host_peers = [pid for pid, _ in peers]
            existing_player_count = len(room.sockets)
        elif room.host_peer and room.host_peer != peer_id:
            peers = [(room.host_peer, room.sockets[room.host_peer])]
            initial_host_peers = []
            existing_player_count = len(room.sockets)
        else:
            peers = []
            initial_host_peers = []
            existing_player_count = len(room.sockets)
    if role == "host":
        for existing_peer_id in initial_host_peers:
            await websocket.send_json({"type": "peerJoined", "peerId": existing_peer_id, "players": existing_player_count})
    else:
        for _, other in peers:
            try:
                await other.send_json({"type": "peerJoined", "peerId": peer_id, "players": existing_player_count})
            except Exception:
                pass

    try:
        async for message in websocket.iter_bytes():
            # Application transport frame: UCH1, target length, target UTF-8,
            # QoS channel byte, then an opaque original UNet payload.
            if len(message) < 6 or message[:4] != b"UCH1":
                await websocket.close(code=4400, reason="Invalid UCH relay frame")
                break
            target_size = message[4]
            header_end = 5 + target_size
            if len(message) < header_end + 1:
                await websocket.close(code=4400, reason="Truncated UCH relay frame")
                break
            requested_target = message[5:header_end].decode("utf-8", errors="strict")
            channel = message[header_end]
            payload = message[header_end + 1:]
            async with rooms_lock:
                current = rooms.get(room_id)
                if current is None:
                    break
                current.last_seen = time.time()
                if role == "host":
                    targets = [
                        (pid, sock) for pid, sock in current.sockets.items()
                        if pid != peer_id and (not requested_target or requested_target == pid)
                    ]
                elif current.host_peer is not None and current.host_peer in current.sockets:
                    targets = [(current.host_peer, current.sockets[current.host_peer])]
                else:
                    targets = []
            if targets:
                forwarded = b"UCH1" + bytes((len(peer_id.encode("utf-8")),)) + peer_id.encode("utf-8") + bytes((channel,)) + payload
                await asyncio.gather(*(sock.send_bytes(forwarded) for _, sock in targets), return_exceptions=True)
    except WebSocketDisconnect:
        pass
    finally:
        async with rooms_lock:
            current = rooms.get(room_id)
            if current is not None:
                current.sockets.pop(peer_id, None)
                if current.host_peer == peer_id:
                    current.host_peer = None
                live = len(current.sockets)
                current.players = live
                peers_to_notify = list(current.sockets.values())
            else:
                live = 0
                peers_to_notify = []
        for other in peers_to_notify:
            try:
                await other.send_json({"type": "peerLeft", "peerId": peer_id, "players": live})
            except Exception:
                pass

