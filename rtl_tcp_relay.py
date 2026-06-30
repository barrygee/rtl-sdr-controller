#!/usr/bin/env python3
"""rtl_tcp fan-out relay.

Holds a single upstream connection to a real (single-client) rtl_tcp dongle
server and re-serves its IQ stream to any number of downstream rtl_tcp clients,
so several consumers (e.g. two Sentinel instances) can share one dongle and a
client that dies abruptly never locks the dongle for the others.

Why this exists
---------------
rtl_tcp serves exactly ONE client and sets no keepalive or idle timeout on that
client socket. A consumer whose host sleeps, crashes, or drops off the network
therefore leaves rtl_tcp holding a dead, half-open connection indefinitely,
locking every other consumer out until rtl_tcp is restarted. This relay is the
dongle's only client, so the dongle slot is always held by one healthy local
connection. Client churn happens HERE instead, where dead clients are reaped
via TCP keepalive plus a bounded, drop-oldest send queue (a stalled client can
never back up memory or stall the fan-out to healthy clients).

Protocol
--------
rtl_tcp sends a 12-byte magic header once on connect (`RTL0` + tuner-type +
tuner-gain-count), then a continuous stream of interleaved 8-bit I/Q pairs from
server to client. The client sends 5-byte commands back: one command byte then a
big-endian uint32 argument (retune, set sample rate, set gain, ...). The relay
caches the upstream header and replays it to each new downstream client, fans the
IQ stream to all clients, and forwards every client command upstream. There is
one physical tuner, so concurrent clients share tuning (last writer wins) — which
is correct for one-at-a-time use and unavoidable for simultaneous use.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s rtl_tcp_relay: %(message)s",
)
logger = logging.getLogger("rtl_tcp_relay")

# Upstream = the real rtl_tcp serving the USB dongle, kept loopback-only.
UPSTREAM_HOST = os.environ.get("RELAY_UPSTREAM_HOST", "127.0.0.1")
UPSTREAM_PORT = int(os.environ.get("RELAY_UPSTREAM_PORT", "1235"))
# Downstream = the public rtl_tcp endpoint the Sentinels connect to.
LISTEN_HOST = os.environ.get("RELAY_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("RELAY_LISTEN_PORT", "1234"))

MAGIC_HEADER_BYTES = 12
COMMAND_BYTES = 5
# Bytes pulled from upstream per fan-out iteration. Small enough for low latency,
# large enough to avoid per-syscall overhead at multi-megabyte sample rates.
UPSTREAM_READ_BYTES = 64 * 1024
# A streaming dongle delivers IQ continuously, so any gap this long means the
# upstream rtl_tcp has gone silent (USB re-enumerated / wedged without closing).
# Time the read out and reconnect rather than block forever on a dead stream.
UPSTREAM_READ_TIMEOUT_S = 10.0
# Per-client send buffer (chunks). A client that falls this far behind is treated
# as too slow / dead: its oldest chunk is dropped, and a fully stalled writer is
# disconnected by the drain timeout below rather than allowed to wedge.
CLIENT_QUEUE_MAX_CHUNKS = 32
# A healthy LAN client drains a chunk near-instantly; this much delay means the
# peer is gone (sleeping host, pulled cable), so drop it and free its resources.
CLIENT_DRAIN_TIMEOUT_S = 5.0
# Reconnect-to-dongle backoff while downstream clients are waiting.
UPSTREAM_BACKOFF_START_S = 1.0
UPSTREAM_BACKOFF_MAX_S = 10.0

# Watchdog. rtl_tcp can survive a USB re-enumeration as a live process that
# accepts connections but never streams (the classic "wedged but not exited"
# state), which restart:unless-stopped can't catch because the process never
# dies. When set, the relay restarts this container via the Docker API after a
# run of no-data cycles, with a cooldown so it can never restart-loop. Empty =
# watchdog disabled (the relay just keeps reconnecting).
WATCHDOG_CONTAINER = os.environ.get("RELAY_RESTART_CONTAINER", "")
WATCHDOG_DOCKER_SOCK = os.environ.get("RELAY_DOCKER_SOCK", "/var/run/docker.sock")
WATCHDOG_RESTART_AFTER_FAILURES = int(os.environ.get("RELAY_RESTART_AFTER_FAILURES", "3"))
WATCHDOG_COOLDOWN_S = float(os.environ.get("RELAY_RESTART_COOLDOWN_S", "60"))


class RelayState:
    """Shared state linking the single upstream reader to all downstream clients.

    Holds the cached upstream magic header (replayed to each new client), the
    current upstream writer (so client commands can be forwarded to the dongle),
    and the set of per-client IQ queues the upstream pump fans samples into.
    """

    def __init__(self) -> None:
        self.magic_header: bytes | None = None
        self.header_ready: asyncio.Event = asyncio.Event()
        self.upstream_writer: asyncio.StreamWriter | None = None
        self.client_queues: set[asyncio.Queue[bytes | None]] = set()

    def fan_out_iq(self, iq_chunk: bytes) -> None:
        """Push one IQ chunk to every client queue, dropping the oldest if full.

        Dropping rather than blocking is deliberate: one slow client must never
        stall the upstream read loop or the other clients. A client that keeps
        overflowing is also failing its drain timeout and will be disconnected.
        """
        for queue in self.client_queues:
            _put_dropping_oldest(queue, iq_chunk)


def _put_dropping_oldest(queue: asyncio.Queue, item: object) -> None:
    """Enqueue item; if the queue is full, discard its oldest entry first."""
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        try:
            queue.get_nowait()
            queue.put_nowait(item)
        except (asyncio.QueueEmpty, asyncio.QueueFull):
            pass


def _enable_tcp_keepalive(writer: asyncio.StreamWriter) -> None:
    """Enable aggressive TCP keepalive so a vanished client is detected fast.

    Without this, a downstream client whose host disappears leaves an ESTABLISHED
    socket here for the OS default (~2 hours) before erroring — exactly the lock
    we are trying to prevent. Keepalive makes the kernel probe and fail a dead
    peer within a few seconds. The per-idle/interval/count knobs are Linux-only;
    each is guarded so the relay still runs (with plain SO_KEEPALIVE) elsewhere.
    """
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 2)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 1)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
    except OSError as error:
        logger.debug("Could not set keepalive on client socket: %s", error)


class UpstreamWatchdog:
    """Restarts the dongle container when the upstream is wedged-but-alive.

    A streaming dongle resets the unhealthy counter; cycles that connect but
    deliver no data increment it. Once enough consecutive no-data cycles pile up
    (and the cooldown since the last restart has elapsed), the watchdog asks the
    Docker Engine to restart the rtl_tcp container — recovering the USB-wedge
    state that a process-liveness restart policy cannot. Disabled (a no-op) when
    no container name is configured.
    """

    def __init__(
        self,
        container_name: str,
        docker_sock_path: str,
        restart_after_failures: int,
        cooldown_s: float,
    ) -> None:
        self.container_name = container_name
        self.docker_sock_path = docker_sock_path
        self.restart_after_failures = restart_after_failures
        self.cooldown_s = cooldown_s
        self.consecutive_unhealthy = 0
        self.last_restart_monotonic = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self.container_name)

    def note_healthy(self) -> None:
        """Record that the upstream delivered data — clears the failure run."""
        self.consecutive_unhealthy = 0

    async def note_unhealthy(self) -> None:
        """Record a no-data upstream cycle; restart the container past threshold."""
        self.consecutive_unhealthy += 1
        if not self.enabled:
            return
        if self.consecutive_unhealthy < self.restart_after_failures:
            return
        if time.monotonic() - self.last_restart_monotonic < self.cooldown_s:
            return
        await self._restart_container()
        self.last_restart_monotonic = time.monotonic()
        self.consecutive_unhealthy = 0

    async def _restart_container(self) -> None:
        """POST /containers/<name>/restart to the Docker Engine over its socket."""
        logger.warning(
            "Upstream wedged (%d no-data cycles) — restarting container '%s'",
            self.consecutive_unhealthy, self.container_name,
        )
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(self.docker_sock_path), timeout=5.0
            )
        except (OSError, asyncio.TimeoutError) as error:
            logger.error("Watchdog cannot reach Docker at %s: %s", self.docker_sock_path, error)
            return
        try:
            request = (
                f"POST /containers/{self.container_name}/restart?t=5 HTTP/1.1\r\n"
                "Host: docker\r\n"
                "Content-Length: 0\r\n"
                "Connection: close\r\n"
                "\r\n"
            )
            writer.write(request.encode("ascii"))
            await writer.drain()
            status_line = await asyncio.wait_for(reader.readline(), timeout=20.0)
            await reader.read()  # drain remainder so the socket closes cleanly
            status = status_line.decode("ascii", "replace").strip()
            if " 204" in status or " 200" in status:
                logger.info("Watchdog restarted '%s' (%s)", self.container_name, status)
            else:
                logger.error("Watchdog restart of '%s' failed: %s", self.container_name, status)
        except (OSError, asyncio.TimeoutError) as error:
            logger.error("Watchdog restart request failed: %s", error)
        finally:
            writer.close()


async def pump_upstream(state: RelayState, watchdog: UpstreamWatchdog) -> None:
    """Maintain the single dongle connection and fan its IQ to all clients.

    Connects to the real rtl_tcp, caches its magic header, then reads IQ forever
    and fans each chunk to every client queue. On any drop it clears shared
    state, signals clients, and reconnects with capped backoff — so the dongle is
    re-acquired the instant it is free, without ever exposing more than one
    connection to the single-client server.
    """
    backoff_s = UPSTREAM_BACKOFF_START_S
    while True:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(UPSTREAM_HOST, UPSTREAM_PORT),
                timeout=5.0,
            )
        except (OSError, asyncio.TimeoutError) as error:
            logger.warning(
                "Upstream rtl_tcp unreachable at %s:%d (%s) — retrying in %.0fs",
                UPSTREAM_HOST, UPSTREAM_PORT, error, backoff_s,
            )
            await asyncio.sleep(backoff_s)
            backoff_s = min(UPSTREAM_BACKOFF_MAX_S, backoff_s * 2)
            continue

        try:
            magic_header = await asyncio.wait_for(
                reader.readexactly(MAGIC_HEADER_BYTES), timeout=5.0
            )
        except (asyncio.IncompleteReadError, asyncio.TimeoutError) as error:
            logger.warning("Upstream sent no magic header (%s) — reconnecting", error)
            writer.close()
            await watchdog.note_unhealthy()
            await asyncio.sleep(backoff_s)
            backoff_s = min(UPSTREAM_BACKOFF_MAX_S, backoff_s * 2)
            continue

        state.magic_header = magic_header
        state.upstream_writer = writer
        state.header_ready.set()
        backoff_s = UPSTREAM_BACKOFF_START_S
        logger.info("Upstream rtl_tcp connected (%s:%d)", UPSTREAM_HOST, UPSTREAM_PORT)

        streamed_any = False
        try:
            while True:
                iq_chunk = await asyncio.wait_for(
                    reader.read(UPSTREAM_READ_BYTES), timeout=UPSTREAM_READ_TIMEOUT_S
                )
                if not iq_chunk:  # EOF: dongle rebooted / rtl_tcp restarted
                    raise ConnectionError("upstream closed")
                if not streamed_any:
                    watchdog.note_healthy()  # a live stream clears the failure run
                    streamed_any = True
                state.fan_out_iq(iq_chunk)
        except asyncio.TimeoutError:
            logger.warning("Upstream went silent — reconnecting to recover stream")
            await watchdog.note_unhealthy()
        except (OSError, ConnectionError) as error:
            logger.warning("Upstream stream dropped (%s) — reconnecting", error)
            if not streamed_any:  # dropped before any data → treat as a wedge cycle
                await watchdog.note_unhealthy()
        finally:
            state.header_ready.clear()
            state.upstream_writer = None
            writer.close()


async def forward_commands(
    state: RelayState, client_reader: asyncio.StreamReader, peer: str
) -> None:
    """Read 5-byte rtl_tcp commands from one client and forward them upstream.

    Tuning/gain commands from any client are relayed to the shared tuner; with
    one dongle this is necessarily last-writer-wins. Returns when the client
    stops sending (disconnect/EOF), which lets the caller tear the client down.
    """
    while True:
        try:
            command = await client_reader.readexactly(COMMAND_BYTES)
        except (asyncio.IncompleteReadError, OSError):
            return  # client closed its half of the connection
        upstream_writer = state.upstream_writer
        if upstream_writer is None:
            continue  # dongle momentarily down; drop the command rather than queue
        try:
            upstream_writer.write(command)
            await upstream_writer.drain()
        except OSError as error:
            logger.debug("Failed forwarding command from %s: %s", peer, error)


async def handle_client(
    state: RelayState,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> None:
    """Serve one downstream rtl_tcp client: replay header, fan IQ, relay commands.

    Registers a per-client queue the upstream pump feeds, streams it to the
    socket with a drain timeout (a stalled/dead peer is dropped, never wedged),
    and concurrently forwards the client's commands upstream. All resources are
    released on disconnect so a vanished client leaves nothing behind.
    """
    peer = "?"
    peer_info = client_writer.get_extra_info("peername")
    if peer_info:
        peer = f"{peer_info[0]}:{peer_info[1]}"
    logger.info("Client connected: %s", peer)
    _enable_tcp_keepalive(client_writer)

    try:
        await asyncio.wait_for(state.header_ready.wait(), timeout=15.0)
    except asyncio.TimeoutError:
        logger.warning("Dropping %s: upstream dongle not ready", peer)
        client_writer.close()
        return

    assert state.magic_header is not None  # guaranteed once header_ready is set
    client_writer.write(state.magic_header)
    try:
        await client_writer.drain()
    except OSError:
        client_writer.close()
        return

    client_queue: asyncio.Queue[bytes | None] = asyncio.Queue(
        maxsize=CLIENT_QUEUE_MAX_CHUNKS
    )
    state.client_queues.add(client_queue)
    command_task = asyncio.create_task(
        forward_commands(state, client_reader, peer), name=f"relay-cmd-{peer}"
    )
    try:
        while True:
            iq_chunk = await client_queue.get()
            if iq_chunk is None:  # reserved shutdown sentinel
                break
            client_writer.write(iq_chunk)
            await asyncio.wait_for(
                client_writer.drain(), timeout=CLIENT_DRAIN_TIMEOUT_S
            )
    except asyncio.TimeoutError:
        logger.info("Dropping stalled/dead client %s (drain timed out)", peer)
    except (OSError, ConnectionError):
        pass  # ordinary client disconnect
    finally:
        state.client_queues.discard(client_queue)
        command_task.cancel()
        client_writer.close()
        logger.info("Client disconnected: %s", peer)


async def main() -> None:
    """Start the downstream server and the upstream pump, and run forever."""
    state = RelayState()
    watchdog = UpstreamWatchdog(
        container_name=WATCHDOG_CONTAINER,
        docker_sock_path=WATCHDOG_DOCKER_SOCK,
        restart_after_failures=WATCHDOG_RESTART_AFTER_FAILURES,
        cooldown_s=WATCHDOG_COOLDOWN_S,
    )
    if watchdog.enabled:
        logger.info(
            "Watchdog enabled: restart '%s' after %d no-data cycles (cooldown %.0fs)",
            watchdog.container_name, watchdog.restart_after_failures, watchdog.cooldown_s,
        )
    upstream_task = asyncio.create_task(
        pump_upstream(state, watchdog), name="relay-upstream"
    )

    server = await asyncio.start_server(
        lambda reader, writer: handle_client(state, reader, writer),
        host=LISTEN_HOST,
        port=LISTEN_PORT,
    )
    logger.info(
        "Relay listening on %s:%d, fanning out %s:%d",
        LISTEN_HOST, LISTEN_PORT, UPSTREAM_HOST, UPSTREAM_PORT,
    )
    async with server:
        try:
            await server.serve_forever()
        finally:
            upstream_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
