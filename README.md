# UCH online directory and WebSocket relay

This service is isolated from recovered game code. It provides:

- `GET /health` and `GET /api/servers` for health and live room listing.
- `POST /api/servers/register` to create a room and return a host token.
- `POST /api/servers/{id}/heartbeat` and `DELETE /api/servers/{id}` for lease renewal and cleanup.
- `wss://<host>/ws/{id}?role=host&token=...` and `wss://<host>/ws/{id}?role=client` for opaque binary frame relay.

The relay does not simulate rules, movement, projectiles, or collision. It forwards original HLAPI payload bytes in an outer `UCH1` peer/channel envelope. The Unity compatibility adapter leaves gameplay message handlers and physics untouched. Room counts use connected WebSocket peers; direct-IP LAN rooms report the host's existing lobby count. Rooms renew a 60 second lease by default, so abandoned entries expire. The current registry is in memory and resets when the service restarts. Render's free service can sleep; active hosts re-register and heartbeat when it wakes. For persistent favorites or historical room records, add an external database; for a live server browser, expiry is intentional so offline hosts are not shown as joinable.

## Local run

```powershell
python -m pip install -r requirements.txt
$env:PORT = '8000'
uvicorn app:app --host 127.0.0.1 --port 8000
```

## Render

Deploy this directory as a Render web service using the included `render.yaml` or the build/start commands above. Render's public WebSocket endpoint must use `wss://`; local LAN tests use `ws://`. Configure the service base URL in the Unity compatibility panel. Do not commit game files, account credentials, or project assets to its deployment repository.

## Integration status

`Assets/Compatibility/UCHWebSocketTransport.cs` adapts the recovered Unity 2021 HLAPI `INetworkTransport` interface to this service. The Windows Editor path uses `ClientWebSocket`; the WebGL path uses `Assets/Plugins/WebGL/UCHWebSocket.jslib`. `UCHMultiplayerPanel` registers and selects WebSocket rooms while the LAN button continues to use the original direct-IP UNet transport. This is a compatibility layer around the original network message path, not a replacement gameplay simulation.

The service has passed a local bidirectional frame/peer-count smoke test. The Unity project compiles the adapter, but the recovered game's actual two-peer lobby/gameplay exchange has not yet been verified. WebGL compilation and Chromium multiplayer have not been tested. The original game's own authority and replication choices still apply; this relay does not make moving pieces or projectiles client-authoritative. Do not advertise a working multiplayer build until two original game clients complete a live round through this transport.
