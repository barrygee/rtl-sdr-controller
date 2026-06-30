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
caches the upstream header and replays it to each new downstream client and fans
the IQ stream to all clients.

Tuning ownership
----------------
There is one physical tuner, so simultaneous clients cannot each tune freely. A
separate NDJSON control channel (see ``handle_control_client``, default port
``LISTEN_PORT + 2``) coordinates a single tuning owner: a client ``claim``s the
token, sends semantic ``set`` requests (centre frequency / sample rate / gain),
and the relay — the sole writer of commands to the dongle while a token is held —
translates them to 5-byte frames and broadcasts the resulting tuner ``state`` to
every control client, so non-owners (read-only followers) always display the real
tuning instead of a stale guess. The token is released on ``release`` or when the
owning control connection drops. While a token is held, raw 5-byte commands on the
IQ sockets are ignored. With no token held the relay falls back to forwarding raw
IQ-socket commands last-writer-wins, so direct/legacy ``rtl_tcp`` clients still work.
"""

from __future__ import annotations

import asyncio
import json
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
# Control = the NDJSON tuning-ownership channel (claim/release/set/get + state push).
# Defaults to two above the IQ port so a single env var is rarely needed; the
# Sentinel backend derives the same offset (settings.sdr_relay_control_port_offset).
CONTROL_PORT = int(os.environ.get("RELAY_CONTROL_PORT", str(LISTEN_PORT + 2)))

MAGIC_HEADER_BYTES = 12
COMMAND_BYTES = 5

# rtl_tcp command bytes (one byte + big-endian uint32 argument).
CMD_SET_FREQUENCY = 0x01  # centre frequency, Hz
CMD_SET_SAMPLE_RATE = 0x02  # sample rate, Hz
CMD_SET_GAIN_MODE = 0x03  # 1 = manual, 0 = auto
CMD_SET_GAIN = 0x04  # tuner gain, tenths of a dB
CMD_SET_AGC_MODE = 0x08  # 1 = AGC on, 0 = AGC off

# Tuner state the relay applies on first upstream connect (and re-applies on every
# reconnect, so a dongle that re-enumerates comes back on the owner's tuning rather
# than rtl_tcp's raw defaults). Mirrors the Sentinel backend's connect-time defaults.
DEFAULT_CENTER_HZ = 100_000_000
DEFAULT_SAMPLE_RATE = 2_048_000
DEFAULT_GAIN_DB = 30.0
DEFAULT_GAIN_AUTO = True
# A control client that falls this far behind on state pushes is treated as dead.
CONTROL_QUEUE_MAX_MESSAGES = 64
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
        # Tuning-ownership coordination (control channel).
        self.center_hz: int = DEFAULT_CENTER_HZ
        self.sample_rate: int = DEFAULT_SAMPLE_RATE
        self.gain_db: float = DEFAULT_GAIN_DB
        self.gain_auto: bool = DEFAULT_GAIN_AUTO
        self.current_owner: ControlSession | None = None
        self.control_sessions: set[ControlSession] = set()

    def tuner_state(self) -> dict[str, object]:
        """The current authoritative tuner state (no per-client ``owner`` flag)."""
        return {
            "center_hz": self.center_hz,
            "sample_rate": self.sample_rate,
            "gain_db": self.gain_db,
            "gain_auto": self.gain_auto,
        }

    def state_message(self, session: ControlSession | None) -> dict[str, object]:
        """Build the ``state`` event for one recipient.

        ``owner`` is whether *this* session holds the token; ``locked`` is whether
        *anyone* holds it — a follower uses ``locked`` to tell "another instance is
        tuning" (read-only) apart from "the token is free" (a tune attempt may take
        it over), so it is never stuck read-only after the owner leaves.
        """
        return {
            "event": "state",
            "owner": session is not None and session is self.current_owner,
            "locked": self.current_owner is not None,
            **self.tuner_state(),
        }

    def broadcast_state(self) -> None:
        """Push the current tuner state to every control client.

        Each client receives its own ``owner`` flag (whether it holds the token),
        so a follower can render read-only while the owner renders live controls.
        """
        for session in self.control_sessions:
            session.enqueue(self.state_message(session))

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


# ── Tuning-ownership control channel ──────────────────────────────────────────


def _build_command(command_byte: int, value: int) -> bytes:
    """Encode one 5-byte rtl_tcp command: [command_byte][big-endian uint32 value]."""
    return bytes([command_byte]) + (value & 0xFFFFFFFF).to_bytes(4, "big")


async def apply_set(state: RelayState, fields: dict) -> None:
    """Translate an owner's semantic ``set`` into 5-byte rtl_tcp commands.

    Updates the relay's authoritative tuner state first (so a state broadcast and a
    later reconnect-replay reflect the request even if the dongle is momentarily
    down), then writes the corresponding command frames upstream when connected.
    Any subset of ``center_hz`` / ``sample_rate`` / ``gain_db`` / ``gain_auto`` may
    be present.
    """
    commands: list[bytes] = []
    if fields.get("sample_rate") is not None:
        state.sample_rate = int(fields["sample_rate"])
        commands.append(_build_command(CMD_SET_SAMPLE_RATE, state.sample_rate))
    if fields.get("center_hz") is not None:
        state.center_hz = int(fields["center_hz"])
        commands.append(_build_command(CMD_SET_FREQUENCY, state.center_hz))
    if fields.get("gain_auto") is True:
        state.gain_auto = True
        commands.append(_build_command(CMD_SET_GAIN_MODE, 0))  # auto
        commands.append(_build_command(CMD_SET_AGC_MODE, 1))  # AGC on
    elif fields.get("gain_db") is not None or fields.get("gain_auto") is False:
        state.gain_auto = False
        if fields.get("gain_db") is not None:
            state.gain_db = float(fields["gain_db"])
        commands.append(_build_command(CMD_SET_GAIN_MODE, 1))  # manual
        commands.append(_build_command(CMD_SET_AGC_MODE, 0))  # AGC off
        commands.append(_build_command(CMD_SET_GAIN, max(0, round(state.gain_db * 10))))

    upstream_writer = state.upstream_writer
    if upstream_writer is None:
        return  # dongle down; state is updated and re-applied on reconnect
    try:
        for command in commands:
            upstream_writer.write(command)
        await upstream_writer.drain()
    except OSError as error:
        logger.debug("Failed applying owner set upstream: %s", error)


def track_forwarded_command(state: RelayState, command: bytes) -> None:
    """Keep the relay's tuner state in step with a forwarded legacy IQ-socket command.

    Only runs in the no-owner fallback path (raw rtl_tcp clients), so the state
    broadcast to any read-only control followers still reflects reality even when a
    legacy client is the one tuning.
    """
    command_byte = command[0]
    value = int.from_bytes(command[1:5], "big")
    if command_byte == CMD_SET_FREQUENCY:
        state.center_hz = value
    elif command_byte == CMD_SET_SAMPLE_RATE:
        state.sample_rate = value
    else:
        return  # gain/other commands are not tracked for follower display
    state.broadcast_state()


class ControlSession:
    """One control-channel client: a bounded outbound queue plus its socket writer.

    Ownership is identified by the session object itself (``state.current_owner is
    session``), so it needs no separately minted token.
    """

    def __init__(self, writer: asyncio.StreamWriter) -> None:
        self.writer = writer
        self.queue: asyncio.Queue[dict | None] = asyncio.Queue(maxsize=CONTROL_QUEUE_MAX_MESSAGES)

    def enqueue(self, message: dict | None) -> None:
        """Queue one outbound message, dropping the oldest if the client is behind."""
        _put_dropping_oldest(self.queue, message)


async def _drain_control_session(session: ControlSession) -> None:
    """Write queued NDJSON messages to one control client until told to stop."""
    try:
        while True:
            message = await session.queue.get()
            if message is None:  # shutdown sentinel
                break
            session.writer.write((json.dumps(message) + "\n").encode("utf-8"))
            await asyncio.wait_for(session.writer.drain(), timeout=CLIENT_DRAIN_TIMEOUT_S)
    except (OSError, asyncio.TimeoutError, ConnectionError):
        pass  # dead/stalled control client; the read side will tear it down


async def _handle_control_op(state: RelayState, session: ControlSession, message: dict) -> None:
    """Apply one parsed control-channel operation from a client."""
    operation = message.get("op")
    if operation == "claim":
        if state.current_owner is None:
            state.current_owner = session
            state.broadcast_state()  # everyone learns ownership changed (claimer gets owner=True)
        else:
            session.enqueue(state.state_message(session))
    elif operation == "release":
        if state.current_owner is session:
            state.current_owner = None
            state.broadcast_state()
        else:
            session.enqueue(state.state_message(session))
    elif operation == "set":
        if state.current_owner is session:
            await apply_set(state, message)
            state.broadcast_state()
        else:
            # Reject silently but reflect the real state so a non-owner stays truthful.
            session.enqueue(state.state_message(session))
    elif operation == "get":
        session.enqueue(state.state_message(session))


async def handle_control_client(
    state: RelayState,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """Serve one NDJSON tuning-ownership client (one JSON object per line).

    Pushes the current tuner state on connect, then processes claim/release/set/get
    ops. Ownership held by this connection is released when it drops, and the other
    control clients are notified so a follower can take over a freed token.
    """
    peer = "?"
    peer_info = writer.get_extra_info("peername")
    if peer_info:
        peer = f"{peer_info[0]}:{peer_info[1]}"
    logger.info("Control client connected: %s", peer)
    _enable_tcp_keepalive(writer)

    session = ControlSession(writer)
    state.control_sessions.add(session)
    session.enqueue(state.state_message(session))
    drain_task = asyncio.create_task(_drain_control_session(session), name=f"relay-ctrl-{peer}")
    try:
        while True:
            line = await reader.readline()
            if not line:  # client closed its half of the connection
                break
            try:
                message = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue  # ignore malformed lines rather than dropping the client
            if isinstance(message, dict):
                await _handle_control_op(state, session, message)
    except (OSError, ConnectionError):
        pass
    finally:
        state.control_sessions.discard(session)
        released = state.current_owner is session
        if released:
            state.current_owner = None
        session.enqueue(None)  # stop the drain task
        drain_task.cancel()
        if released:
            state.broadcast_state()  # tell remaining clients the token is free
        writer.close()
        logger.info("Control client disconnected: %s", peer)


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
        # Apply the tracked tuner state to the freshly connected dongle so a
        # re-enumerated device resumes on the owner's tuning (and a healthy sample
        # rate) instead of rtl_tcp's raw defaults.
        await apply_set(state, state.tuner_state())

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

    Only the no-owner fallback path: while a control-channel client holds the
    tuning token the relay is the sole writer of commands to the dongle, so raw
    IQ-socket commands are ignored (they cannot fight the owner). With no token
    held, commands are forwarded last-writer-wins so direct/legacy rtl_tcp clients
    still tune. Returns when the client stops sending (disconnect/EOF).
    """
    while True:
        try:
            command = await client_reader.readexactly(COMMAND_BYTES)
        except (asyncio.IncompleteReadError, OSError):
            return  # client closed its half of the connection
        if state.current_owner is not None:
            continue  # control channel owns tuning; ignore legacy IQ-socket commands
        upstream_writer = state.upstream_writer
        if upstream_writer is None:
            continue  # dongle momentarily down; drop the command rather than queue
        track_forwarded_command(state, command)
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
    control_server = await asyncio.start_server(
        lambda reader, writer: handle_control_client(state, reader, writer),
        host=LISTEN_HOST,
        port=CONTROL_PORT,
    )
    logger.info(
        "Relay listening on %s:%d (control %s:%d), fanning out %s:%d",
        LISTEN_HOST, LISTEN_PORT, LISTEN_HOST, CONTROL_PORT, UPSTREAM_HOST, UPSTREAM_PORT,
    )
    async with server, control_server:
        try:
            await asyncio.gather(server.serve_forever(), control_server.serve_forever())
        finally:
            upstream_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
