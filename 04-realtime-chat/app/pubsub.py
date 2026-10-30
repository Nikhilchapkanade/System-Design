"""Distributed Pub/Sub Broker Interface and In-Memory Multi-Node Simulator.

System Design Analysis (Horizontal WebSocket Scaling):
WebSocket connections are persistent and stateful. A single node cannot handle
all connected clients at scale (due to file descriptor limits and RAM).
When traffic is partitioned across multiple server nodes, a centralized Pub/Sub bus
(e.g., Redis Pub/Sub, Kafka, or NATS) is required so that a message received on
Node 1 is broadcast to Node 2, where other room participants are connected.
"""

import asyncio
import json
from abc import ABC, abstractmethod
from typing import Callable, Dict, Set, Any, Awaitable


class PubSubBroker(ABC):
    """Abstract Message Broker for cross-node fan-out."""

    @abstractmethod
    async def publish(self, channel: str, message: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def subscribe(self, channel: str, listener: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        pass

    @abstractmethod
    async def unsubscribe(self, channel: str, listener: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        pass


class InMemoryPubSubBroker(PubSubBroker):
    """Async In-Memory Pub/Sub Message Bus simulating a shared distributed broker."""

    def __init__(self):
        # channel -> set of async callback listeners
        self._subscribers: Dict[str, Set[Callable[[Dict[str, Any]], Awaitable[None]]]] = {}
        self._lock = asyncio.Lock()

    async def publish(self, channel: str, message: Dict[str, Any]) -> None:
        """Broadcast message to all subscribers of the channel."""
        async with self._lock:
            listeners = list(self._subscribers.get(channel, set()))

        if listeners:
            # Deliver concurrently to all registered node listeners
            tasks = [asyncio.create_task(listener(message)) for listener in listeners]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def subscribe(self, channel: str, listener: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        """Register a node listener for a channel."""
        async with self._lock:
            if channel not in self._subscribers:
                self._subscribers[channel] = set()
            self._subscribers[channel].add(listener)

    async def unsubscribe(self, channel: str, listener: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        """Remove a node listener."""
        async with self._lock:
            if channel in self._subscribers:
                self._subscribers[channel].discard(listener)
                if not self._subscribers[channel]:
                    del self._subscribers[channel]
