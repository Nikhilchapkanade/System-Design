"""Real-Time Chat Application with WebSockets and Pub/Sub Broadcasting.

System Design Architecture:
- Full-duplex bidirectional streaming via WebSockets.
- Cross-node event dissemination via Pub/Sub broker.
- Strict message sequencing (seq_id) and persistence in SQLite WAL.
- Dual-client split-screen web UI for instant local testing.
"""

import time
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse

from app import config
from app.pubsub import InMemoryPubSubBroker
from app.storage import ChatStorage
from app.connection_manager import ConnectionManager

# Initialize shared components
broker = InMemoryPubSubBroker()
storage = ChatStorage(config.DATABASE_PATH)
manager = ConnectionManager(broker=broker, node_id=config.NODE_ID)

app = FastAPI(
    title="Real-Time Chat & Pub/Sub Service",
    description="Horizontally-scalable WebSocket chat engine with Pub/Sub message routing and monotonic sequencing.",
    version="1.0.0",
)


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "realtime-chat", "node_id": config.NODE_ID}


@app.get("/api/v1/rooms/{room_id}/history")
def get_room_history(
    room_id: str,
    limit: int = Query(50, ge=1, le=100),
    before_seq: Optional[int] = Query(None, description="Cursor for pagination"),
):
    """Retrieve chronologically ordered message history for a room."""
    messages = storage.get_history(room_id=room_id, limit=limit, before_seq=before_seq)
    return {"room_id": room_id, "count": len(messages), "messages": messages}


@app.get("/api/v1/rooms/{room_id}/presence")
def get_room_presence(room_id: str):
    """Inspect active users connected to this room."""
    users = manager.get_room_presence(room_id)
    return {"room_id": room_id, "active_users_count": len(users), "active_users": users}


@app.get("/api/v1/stats")
def get_service_stats():
    """System-level stats: stored messages and total rooms."""
    return storage.get_stats()


@app.websocket("/ws/{room_id}/{username}")
async def websocket_chat_endpoint(websocket: WebSocket, room_id: str, username: str):
    """WebSocket endpoint handling bi-directional chat streaming."""
    username = username.strip()
    room_id = room_id.strip()

    await manager.connect(room_id, username, websocket)

    # Broadcast user joined event via Pub/Sub
    join_event = {
        "type": "system",
        "action": "join",
        "username": username,
        "room_id": room_id,
        "timestamp": time.time(),
        "node_id": config.NODE_ID,
    }
    await broker.publish(f"chat:room:{room_id}", join_event)

    try:
        while True:
            data = await websocket.receive_text()
            text = data.strip()
            if not text:
                continue

            # 1. Persist message with monotonic sequence ID
            saved = storage.save_message(room_id=room_id, username=username, content=text)

            # 2. Publish to distributed message bus
            chat_event = {
                "type": "chat",
                "id": saved["id"],
                "seq_id": saved["seq_id"],
                "room_id": room_id,
                "username": username,
                "content": saved["content"],
                "timestamp": saved["timestamp"],
                "node_id": config.NODE_ID,
            }
            await broker.publish(f"chat:room:{room_id}", chat_event)

    except WebSocketDisconnect:
        await manager.disconnect(room_id, username, websocket)
        leave_event = {
            "type": "system",
            "action": "leave",
            "username": username,
            "room_id": room_id,
            "timestamp": time.time(),
            "node_id": config.NODE_ID,
        }
        await broker.publish(f"chat:room:{room_id}", leave_event)
    except Exception:
        await manager.disconnect(room_id, username, websocket)


@app.get("/", response_class=HTMLResponse)
def index():
    """Split-screen interactive multi-user chat client."""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Real-Time Chat & Pub/Sub Simulator</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px; background: #f0f2f5; color: #1c1e21; }
            h1 { color: #0084ff; text-align: center; margin-bottom: 5px; }
            p.sub { text-align: center; color: #65676b; margin-top: 0; }
            .split-container { display: flex; gap: 20px; max-width: 1200px; margin: 20px auto; }
            .chat-panel { flex: 1; background: white; border-radius: 10px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); display: flex; flex-direction: column; height: 550px; overflow: hidden; }
            .chat-header { background: #0084ff; color: white; padding: 12px 16px; font-weight: bold; display: flex; justify-content: space-between; align-items: center; }
            .messages-box { flex: 1; padding: 15px; overflow-y: auto; background: #f9f9f9; display: flex; flex-direction: column; gap: 8px; }
            .msg { padding: 8px 12px; border-radius: 8px; max-width: 75%; font-size: 14px; word-break: break-word; }
            .msg-mine { background: #0084ff; color: white; align-self: flex-end; }
            .msg-other { background: #e4e6eb; color: #050505; align-self: flex-start; }
            .msg-system { background: #fff3cd; color: #856404; align-self: center; font-size: 12px; font-style: italic; }
            .msg-meta { font-size: 10px; opacity: 0.8; margin-top: 3px; display: flex; justify-content: space-between; gap: 10px; }
            .chat-footer { padding: 10px; border-top: 1px solid #ddd; display: flex; gap: 8px; background: white; }
            input[type="text"] { flex: 1; padding: 10px; border: 1px solid #ccc; border-radius: 6px; font-size: 14px; }
            button { padding: 10px 16px; background: #0084ff; color: white; border: none; border-radius: 6px; font-weight: bold; cursor: pointer; }
            button:hover { background: #006bce; }
            .badge { background: rgba(255,255,255,0.3); padding: 2px 6px; border-radius: 4px; font-size: 11px; }
        </style>
    </head>
    <body>
        <h1>💬 Real-Time Chat & Pub/Sub Simulator</h1>
        <p class="sub">Simulating two separate users in room <b>#general</b> communicating over persistent WebSockets with Pub/Sub broadcast.</p>

        <div class="split-container">
            <!-- Client 1: Alice -->
            <div class="chat-panel" id="panel-alice">
                <div class="chat-header">
                    <span>User: <b>Alice</b> (#general)</span>
                    <span class="badge" id="status-alice">Connecting...</span>
                </div>
                <div class="messages-box" id="box-alice"></div>
                <div class="chat-footer">
                    <input type="text" id="input-alice" placeholder="Type a message as Alice..." onkeydown="if(event.key==='Enter') sendMsg('alice')" />
                    <button onclick="sendMsg('alice')">Send</button>
                </div>
            </div>

            <!-- Client 2: Bob -->
            <div class="chat-panel" id="panel-bob">
                <div class="chat-header" style="background: #28a745;">
                    <span>User: <b>Bob</b> (#general)</span>
                    <span class="badge" id="status-bob">Connecting...</span>
                </div>
                <div class="messages-box" id="box-bob"></div>
                <div class="chat-footer">
                    <input type="text" id="input-bob" placeholder="Type a message as Bob..." onkeydown="if(event.key==='Enter') sendMsg('bob')" />
                    <button style="background: #28a745;" onclick="sendMsg('bob')">Send</button>
                </div>
            </div>
        </div>

        <script>
            const sockets = {};
            const room = 'general';

            function setupClient(user) {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const wsUrl = `${protocol}//${window.location.host}/ws/${room}/${user}`;
                const ws = new WebSocket(wsUrl);
                sockets[user] = ws;

                ws.onopen = () => {
                    document.getElementById(`status-${user}`).textContent = 'Connected';
                };

                ws.onmessage = (event) => {
                    const data = JSON.parse(event.data);
                    renderMessage(user, data);
                };

                ws.onclose = () => {
                    document.getElementById(`status-${user}`).textContent = 'Disconnected';
                };
            }

            function renderMessage(clientUser, data) {
                const box = document.getElementById(`box-${clientUser}`);
                const div = document.createElement('div');

                if (data.type === 'system') {
                    div.className = 'msg msg-system';
                    div.textContent = `${data.username} ${data.action === 'join' ? 'joined' : 'left'} the room.`;
                } else {
                    const isMine = data.username === clientUser;
                    div.className = `msg ${isMine ? 'msg-mine' : 'msg-other'}`;
                    div.innerHTML = `
                        <b>${data.username}</b>: ${escapeHtml(data.content)}
                        <div class="msg-meta">
                            <span>seq #${data.seq_id}</span>
                            <span>${new Date(data.timestamp * 1000).toLocaleTimeString()}</span>
                        </div>
                    `;
                }
                box.appendChild(div);
                box.scrollTop = box.scrollHeight;
            }

            function sendMsg(user) {
                const input = document.getElementById(`input-${user}`);
                const text = input.value.trim();
                if (text && sockets[user] && sockets[user].readyState === WebSocket.OPEN) {
                    sockets[user].send(text);
                    input.value = '';
                }
            }

            function escapeHtml(text) {
                const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
                return text.replace(/[&<>"']/g, m => map[m]);
            }

            setupClient('Alice');
            setupClient('Bob');
        </script>
    </body>
    </html>
    """
