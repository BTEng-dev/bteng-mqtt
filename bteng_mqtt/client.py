"""MQTT client manager — named connections, message routing, latest-value cache.

MQTT has no blocking read: values arrive when the broker feels like sending them.
Three pieces of state make that usable from a synchronous behavior tree:

* **refcounted subscriptions** — N nodes on one topic cause one broker SUBSCRIBE,
  and the last one to close it triggers the UNSUBSCRIBE.
* **per-subscriber queues** — a node's queue starts filling the moment it
  subscribes, so a message that arrives between two ticks is not lost.
* **a latest-value cache** per (client, topic) — lets a condition node answer
  "what is the value now?" without waiting for the next publish.

Nodes never touch a backend directly and never block the tick loop: they either
poll a :class:`Subscription` or poll a ``Future`` returned by :meth:`submit`.
"""
from __future__ import annotations

import contextlib
import logging
import queue
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Any

from bteng_mqtt.backend.base import BackendCallbacks, MqttBackend, PublishTicket
from bteng_mqtt.exceptions import ClientNotFoundError, ConnectionError
from bteng_mqtt.registry import BrokerConfig
from bteng_mqtt.topics import topic_matches

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Message:
    """One received MQTT message."""
    topic:     str
    payload:   bytes
    qos:       int   = 0
    retain:    bool  = False
    timestamp: float = 0.0   # time.time() when the router saw it

    @property
    def age(self) -> float:
        """Seconds since the message arrived."""
        return time.time() - self.timestamp


@dataclass
class _FilterState:
    """Broker-side subscription shared by several Subscriptions."""
    qos:   int
    count: int = 0


class Subscription:
    """A node's private view of one topic filter.

    Obtained from :meth:`MqttClientManager.subscribe` and closed by the node when
    it is done (``on_halted``, or after a non-persistent match).  Closing is
    idempotent and drops the broker subscription when no one else holds it.
    """

    def __init__(self, manager: MqttClientManager, client_id: str,
                 topic_filter: str, qos: int) -> None:
        self.client_id = client_id
        self.topic_filter = topic_filter
        self.qos = qos
        self._manager = manager
        self._queue: queue.SimpleQueue[Message] = queue.SimpleQueue()
        self._closed = False

    # ── Reading ───────────────────────────────────────────────────────────────

    def get_nowait(self) -> Message | None:
        """Oldest undelivered message, or None if the queue is empty."""
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return None

    def drain(self) -> list[Message]:
        """Every queued message, oldest first."""
        messages: list[Message] = []
        while (message := self.get_nowait()) is not None:
            messages.append(message)
        return messages

    def clear(self) -> None:
        """Discard queued messages — used when a node re-activates and only
        cares about what happens from now on."""
        self.drain()

    @property
    def empty(self) -> bool:
        return self._queue.empty()

    @property
    def closed(self) -> bool:
        return self._closed

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def matches(self, topic: str) -> bool:
        return topic_matches(self.topic_filter, topic)

    def deliver(self, message: Message) -> None:
        """Called by the manager's router thread."""
        if not self._closed:
            self._queue.put_nowait(message)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._manager._release_subscription(self)

    def __enter__(self) -> Subscription:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class MqttClientManager:
    """Manages named MQTT client connections and everything they receive."""

    _instance: MqttClientManager | None = None
    _class_lock = threading.Lock()
    #: Factory used when no backend is passed in. None → paho.  Set it to hand
    #: every future manager a MockBackend, which is how a tree runs offline:
    #: run_trees() resets the singleton between trees, so injecting one instance
    #: with set_instance() would not survive.
    _backend_factory: Callable[[], MqttBackend] | None = None

    def __init__(self, backend: MqttBackend | None = None) -> None:
        self._backend: MqttBackend = backend or self._default_backend()
        self._lock = threading.RLock()
        self._clients: dict[str, Any] = {}
        self._configs: dict[str, BrokerConfig] = {}
        self._subscriptions: dict[str, list[Subscription]] = {}
        self._filters: dict[tuple[str, str], _FilterState] = {}
        self._latest: dict[tuple[str, str], Message] = {}
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="bteng-mqtt")

    # ── Singleton ─────────────────────────────────────────────────────────────

    @classmethod
    def get_instance(cls) -> MqttClientManager:
        with cls._class_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    @classmethod
    def set_instance(cls, manager: MqttClientManager) -> None:
        """Inject a custom manager (e.g. with a MockBackend) in tests."""
        with cls._class_lock:
            cls._instance = manager

    @classmethod
    def reset_instance(cls) -> None:
        """Disconnect everything and drop the singleton (tests, end of a run)."""
        with cls._class_lock:
            instance, cls._instance = cls._instance, None
        if instance is not None:
            instance.close()

    def close(self) -> None:
        """Disconnect every client and stop the worker pool."""
        for client_id in self.active_clients():
            with contextlib.suppress(Exception):
                self.disconnect(client_id)
        self._executor.shutdown(wait=False)

    @property
    def backend(self) -> MqttBackend:
        return self._backend

    # ── Thread bridge ─────────────────────────────────────────────────────────

    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> Future:
        """Run a blocking call on a worker thread; return a pollable Future."""
        return self._executor.submit(fn, *args, **kwargs)

    # ── Connection lifecycle ──────────────────────────────────────────────────

    def connect(self, client_id: str, config: BrokerConfig,
                timeout: float | None = None) -> None:
        """Connect and register a client, blocking until CONNACK.

        No-op when the client id is already connected.  Raises ConnectionError on
        refusal or timeout.  Call it through :meth:`submit` (or use
        :meth:`connect_async`) from a BT node so the tick loop keeps running.
        """
        with self._lock:
            if client_id in self._clients:
                return

        connack: queue.SimpleQueue[int] = queue.SimpleQueue()
        callbacks = BackendCallbacks(
            on_message=partial(self._on_message, client_id),
            on_connect=partial(self._on_connect, client_id, connack),
            on_disconnect=partial(self._on_disconnect, client_id),
        )

        try:
            client = self._backend.connect(config, callbacks)
        except ConnectionError:
            raise
        except Exception as exc:
            raise ConnectionError(
                f"Failed to connect client '{client_id}' to {config.address}: {exc}"
            ) from exc

        wait = config.timeout if timeout is None else timeout
        try:
            reason_code = connack.get(timeout=wait)
        except queue.Empty:
            with contextlib.suppress(Exception):
                self._backend.disconnect(client)
            raise ConnectionError(
                f"Client '{client_id}': no CONNACK from {config.address} within {wait}s"
            ) from None

        if reason_code != 0:
            with contextlib.suppress(Exception):
                self._backend.disconnect(client)
            raise ConnectionError(
                f"Client '{client_id}': broker {config.address} refused the "
                f"connection (reason code {reason_code})"
            )

        with self._lock:
            self._clients[client_id] = client
            self._configs[client_id] = config
        logger.info("client %r connected to %s", client_id, config.address)

        # A reconnect keeps its Subscriptions, but the broker forgot them.
        self._resubscribe(client_id)

    def connect_async(self, client_id: str, config: BrokerConfig,
                      timeout: float | None = None) -> Future:
        """:meth:`connect` on a worker thread — what BT nodes poll."""
        return self.submit(self.connect, client_id, config, timeout)

    def disconnect(self, client_id: str) -> None:
        """Disconnect, deregister, and forget the config (no reconnect after this).

        Use :meth:`unregister_client` when reconnect must stay available.
        """
        with self._lock:
            client = self._clients.pop(client_id, None)
            self._configs.pop(client_id, None)
            self._drop_client_state(client_id)
        if client is None:
            return
        with contextlib.suppress(Exception):
            self._backend.disconnect(client)
        logger.info("client %r disconnected", client_id)

    def unregister_client(self, client_id: str) -> Any | None:
        """Disconnect but keep the saved config so :meth:`reconnect` still works.

        This is what the MqttDisconnect node uses.
        """
        with self._lock:
            client = self._clients.pop(client_id, None)
            self._drop_client_state(client_id, keep_subscriptions=True)
        if client is not None:
            with contextlib.suppress(Exception):
                self._backend.disconnect(client)
        return client

    def reconnect(self, client_id: str, timeout: float | None = None) -> None:
        """Re-establish a client from its saved config."""
        with self._lock:
            config = self._configs.get(client_id)
        if config is None:
            raise ConnectionError(
                f"No saved config for client '{client_id}' — connect first"
            )
        with self._lock:
            client = self._clients.pop(client_id, None)
        if client is not None:
            with contextlib.suppress(Exception):
                self._backend.disconnect(client)
        self.connect(client_id, config, timeout)

    # ── Lookup ────────────────────────────────────────────────────────────────

    def get_client(self, client_id: str) -> Any:
        """The handle for a client id. Raises ClientNotFoundError if absent."""
        with self._lock:
            client = self._clients.get(client_id)
        if client is None:
            raise ClientNotFoundError(
                f"Client '{client_id}' not found. Active clients: {self.active_clients()}"
            )
        return client

    def is_connected(self, client_id: str) -> bool:
        with self._lock:
            return client_id in self._clients

    def active_clients(self) -> list[str]:
        with self._lock:
            return list(self._clients)

    def get_client_config(self, client_id: str) -> BrokerConfig | None:
        with self._lock:
            return self._configs.get(client_id)

    # ── Publish ───────────────────────────────────────────────────────────────

    def publish(self, client_id: str, topic: str, payload: bytes,
                qos: int = 0, retain: bool = False) -> PublishTicket:
        """Queue a publish; the returned ticket says when it is acknowledged."""
        client = self.get_client(client_id)
        logger.debug("publish %s qos=%d retain=%s (%d bytes)", topic, qos, retain, len(payload))
        return self._backend.publish(client, topic, payload, qos=qos, retain=retain)

    # ── Subscribe ─────────────────────────────────────────────────────────────

    def subscribe(self, client_id: str, topic_filter: str, qos: int = 0) -> Subscription:
        """Start receiving a topic filter and return a private Subscription.

        The broker SUBSCRIBE happens only for the first holder of a given
        (client, filter) pair; later holders share it.  When an existing broker
        subscription has a lower QoS than requested, it is upgraded.
        """
        client = self.get_client(client_id)
        subscription = Subscription(self, client_id, topic_filter, qos)
        key = (client_id, topic_filter)

        with self._lock:
            state = self._filters.get(key)
            needs_subscribe = state is None or qos > state.qos
            if state is None:
                state = _FilterState(qos=qos)
                self._filters[key] = state
            state.qos = max(state.qos, qos)
            state.count += 1
            self._subscriptions.setdefault(client_id, []).append(subscription)
            effective_qos = state.qos

        if needs_subscribe:
            try:
                self._backend.subscribe(client, topic_filter, effective_qos)
            except Exception:
                subscription.close()
                raise
        return subscription

    def _release_subscription(self, subscription: Subscription) -> None:
        """Called by Subscription.close(); unsubscribes when the last holder goes."""
        key = (subscription.client_id, subscription.topic_filter)
        with self._lock:
            holders = self._subscriptions.get(subscription.client_id)
            if holders is not None and subscription in holders:
                holders.remove(subscription)
            state = self._filters.get(key)
            if state is None:
                return
            state.count -= 1
            if state.count > 0:
                return
            del self._filters[key]
            client = self._clients.get(subscription.client_id)

        if client is not None:
            with contextlib.suppress(Exception):
                self._backend.unsubscribe(client, subscription.topic_filter)

    def unsubscribe_all(self, client_id: str, topic_filter: str) -> int:
        """Close every Subscription on a filter; returns how many were closed.

        A node cannot reach the Subscription another node holds, so this is how
        ``MqttUnsubscribe`` drops a subscription that an earlier
        ``MqttSubscribe persistent="true"`` opened.  The broker UNSUBSCRIBE
        follows from the refcount hitting zero, as usual.
        """
        with self._lock:
            holders = [
                subscription for subscription in self._subscriptions.get(client_id, ())
                if subscription.topic_filter == topic_filter
            ]
        for subscription in holders:  # close() takes the lock itself
            subscription.close()
        return len(holders)

    def subscriber_count(self, client_id: str, topic_filter: str) -> int:
        """How many Subscriptions share one broker subscription (introspection)."""
        with self._lock:
            state = self._filters.get((client_id, topic_filter))
            return state.count if state else 0

    # ── Latest-value cache ────────────────────────────────────────────────────

    def latest(self, client_id: str, topic: str) -> Message | None:
        """The most recent message seen on an exact topic, or None."""
        with self._lock:
            return self._latest.get((client_id, topic))

    def latest_matching(self, client_id: str, topic_filter: str) -> Message | None:
        """The most recent message on any topic matching a filter, or None."""
        with self._lock:
            candidates = [
                message for (cid, topic), message in self._latest.items()
                if cid == client_id and topic_matches(topic_filter, topic)
            ]
        return max(candidates, key=lambda m: m.timestamp) if candidates else None

    def clear_latest(self, client_id: str | None = None) -> None:
        with self._lock:
            if client_id is None:
                self._latest.clear()
            else:
                for key in [k for k in self._latest if k[0] == client_id]:
                    del self._latest[key]

    # ── Backend callbacks (run on the network thread) ──────────────────────────

    def _on_message(self, client_id: str, topic: str, payload: bytes,
                    qos: int, retain: bool) -> None:
        message = Message(topic=topic, payload=payload, qos=qos, retain=retain,
                          timestamp=time.time())
        with self._lock:
            self._latest[(client_id, topic)] = message
            subscriptions = list(self._subscriptions.get(client_id, ()))
        # Deliver outside the lock: a queue put must never wait on manager state.
        for subscription in subscriptions:
            if subscription.matches(topic):
                subscription.deliver(message)

    def _on_connect(self, client_id: str, connack: queue.SimpleQueue,
                    reason_code: int) -> None:
        connack.put_nowait(reason_code)
        # paho reconnects on its own; the broker forgot our subscriptions.
        if reason_code == 0 and self.is_connected(client_id):
            self._resubscribe(client_id)

    def _on_disconnect(self, client_id: str, reason_code: int) -> None:
        if reason_code != 0:
            logger.warning("client %r lost its connection (reason code %d)",
                           client_id, reason_code)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _resubscribe(self, client_id: str) -> None:
        """Re-issue every filter this client holds, after a (re)connect."""
        with self._lock:
            client = self._clients.get(client_id)
            filters = [(f, state.qos) for (cid, f), state in self._filters.items()
                       if cid == client_id]
        if client is None:
            return
        for topic_filter, qos in filters:
            with contextlib.suppress(Exception):
                self._backend.subscribe(client, topic_filter, qos)

    def _drop_client_state(self, client_id: str, keep_subscriptions: bool = False) -> None:
        """Forget cached values and (optionally) subscriptions. Caller holds the lock."""
        for key in [k for k in self._latest if k[0] == client_id]:
            del self._latest[key]
        if keep_subscriptions:
            return
        for subscription in self._subscriptions.pop(client_id, []):
            subscription._closed = True
        for key in [k for k in self._filters if k[0] == client_id]:
            del self._filters[key]

    @classmethod
    def set_default_backend_factory(cls, factory: Callable[[], MqttBackend] | None) -> None:
        """Choose the backend every future manager builds; None restores paho."""
        cls._backend_factory = factory

    def _default_backend(self) -> MqttBackend:
        factory = type(self)._backend_factory
        if factory is not None:
            return factory()
        from bteng_mqtt.backend.paho_backend import PahoBackend
        return PahoBackend()
