"""WebSocket Connection Manager and Local Socket Pool.

System Design Analysis:
Maintains the set of open, active WebSocket descriptors on THIS server instance.
Decouples local socket I/O from the distributed Pub/Sub broadcast layer.
"""

import asyncio
from typing import Dict, Set, Optional, List, Any
from fastapi import WebSocket

from app.pubsub import PubSubBroker


class ConnectionManager:
    """Manages active local WebSockets per room and subscribes to distributed Pub/Sub."""

    def __init__(self, broker: PubSubBroker, node_id: str = "node_1"):
        self.broker = broker
        self.node_id = node_id

        # room_id -> set of active local WebSocket connections
        self._rooms: Dict[str, Set[WebSocket]] = {}
        # room_id -> set of active usernames on this node
        self._users: Dict[str, Set[str]] = {}
        # room_id -> channel subscription active
        self._subscribed_channels: Set[str] = set()
        self._lock = asyncio.Lock()

    async def _on_pubsub_message(self, room_id: str, message: Dict[str, Any]) -> None:
        """Callback invoked when the distributed message broker receives a broadcast."""
        await self.broadcast_local(room_id, message)

    async def connect(self, room_id: str, username: str, websocket: WebSocket) -> None:
        """Register newly accepted WebSocket connection on this node."""
        await websocket.accept()

        async with self._lock:
            if room_id not in self._rooms:
                self._rooms[room_id] = set()
                self._users[room_id] = set()

            self._rooms[room_id].add(websocket)
            self._users[room_id].add(username)

            # Ensure this node is subscribed to the distributed channel for this room
            channel = f"chat:room:{room_id}"
            if channel not in self._subscribed_channels:
                self._subscribed_channels.add(channel)
                # Bind room_id into listener callback
                await self.broker.subscribe(
                    channel,
                    lambda msg, r=room_id: self._on_pubsub_message(r, msg)
                )

    async def disconnect(self, room_id: str, username: str, websocket: WebSocket) -> None:
        """Unregister closed WebSocket connection."""
        async with self._lock:
            if room_id in self._rooms:
                self._rooms[room_id].discard(websocket)
                if not self._rooms[room_id]:
                    del self._rooms[room_id]

            if room_id in self._users:
                self._users[room_id].discard(username)
                if not self._users[room_id]:
                    del self._users[room_id]

    async def broadcast_local(self, room_id: str, payload: Dict[str, Any]) -> None:
        """Push JSON payload to all WebSockets connected to this room on THIS node."""
        async with self._lock:
            sockets = list(self._rooms.get(room_id, set()))

        dead_sockets = []
        for ws in sockets:
            try:
                await ws.send_json(payload)
            except Exception:
                dead_sockets.append(ws)

        # Cleanup any broken pipes
        if dead_sockets:
            async with self._lock:
                if room_id in self._rooms:
                    for ws in dead_sockets:
                        self._rooms[room_id].discard(ws)

    def get_room_presence(self, room_id: str) -> List[str]:
        """Return usernames active on this node for given room."""
        return list(self._users.get(room_id, set()))
