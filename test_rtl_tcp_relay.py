"""Tests for the tuning-ownership control channel in rtl_tcp_relay.py.

The relay coordinates a single tuning owner over an NDJSON control channel so
several consumers (e.g. two Sentinel instances) share one dongle without fighting
over tuning: the owner drives the dongle, non-owners are read-only followers, and
with no owner the relay falls back to forwarding raw IQ-socket commands.

Run with:  uv run --with pytest pytest test_rtl_tcp_relay.py
"""

from __future__ import annotations

import asyncio
import json

import rtl_tcp_relay as relay


class FakeWriter:
    """Records bytes written; stands in for an upstream/downstream StreamWriter."""

    def __init__(self) -> None:
        self.buf = bytearray()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.buf += data

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True

    def get_extra_info(self, _name: str):
        return None

    async def wait_closed(self) -> None:
        pass


def _session() -> relay.ControlSession:
    return relay.ControlSession(FakeWriter())


# ── Command encoding / apply_set ──────────────────────────────────────────────


def test_build_command_encodes_big_endian():
    assert relay._build_command(0x01, 100_000_000) == bytes([1]) + (100_000_000).to_bytes(4, "big")


def test_apply_set_writes_frames_and_updates_state():
    async def run():
        state = relay.RelayState()
        writer = FakeWriter()
        state.upstream_writer = writer
        await relay.apply_set(state, {"sample_rate": 2_400_000, "center_hz": 101_000_000})
        assert state.sample_rate == 2_400_000
        assert state.center_hz == 101_000_000
        # Sample rate (0x02) is applied before centre (0x01).
        assert bytes(writer.buf) == (
            bytes([2]) + (2_400_000).to_bytes(4, "big") + bytes([1]) + (101_000_000).to_bytes(4, "big")
        )

    asyncio.run(run())


def test_apply_set_gain_auto_frames():
    async def run():
        state = relay.RelayState()
        writer = FakeWriter()
        state.upstream_writer = writer
        await relay.apply_set(state, {"gain_auto": True})
        assert state.gain_auto is True
        # gain mode auto (0x03,0) then AGC on (0x08,1).
        assert bytes(writer.buf) == bytes([3, 0, 0, 0, 0]) + bytes([8, 0, 0, 0, 1])

    asyncio.run(run())


def test_apply_set_gain_manual_frames():
    async def run():
        state = relay.RelayState()
        writer = FakeWriter()
        state.upstream_writer = writer
        await relay.apply_set(state, {"gain_auto": False, "gain_db": 10.0})
        assert state.gain_auto is False
        assert state.gain_db == 10.0
        # manual (0x03,1), AGC off (0x08,0), gain 100 tenths (0x04,100).
        assert bytes(writer.buf) == (
            bytes([3, 0, 0, 0, 1]) + bytes([8, 0, 0, 0, 0]) + bytes([4]) + (100).to_bytes(4, "big")
        )

    asyncio.run(run())


def test_apply_set_without_upstream_updates_state_only():
    async def run():
        state = relay.RelayState()
        state.upstream_writer = None
        await relay.apply_set(state, {"center_hz": 99_000_000})
        assert state.center_hz == 99_000_000  # tracked for reconnect-replay + broadcast

    asyncio.run(run())


# ── Legacy forwarded-command tracking ─────────────────────────────────────────


def test_track_forwarded_command_updates_freq_and_rate():
    state = relay.RelayState()
    relay.track_forwarded_command(state, relay._build_command(relay.CMD_SET_FREQUENCY, 91_000_000))
    assert state.center_hz == 91_000_000
    relay.track_forwarded_command(state, relay._build_command(relay.CMD_SET_SAMPLE_RATE, 1_800_000))
    assert state.sample_rate == 1_800_000


def test_track_forwarded_command_ignores_other_commands():
    state = relay.RelayState()
    before = state.center_hz
    relay.track_forwarded_command(state, relay._build_command(relay.CMD_SET_GAIN, 100))
    assert state.center_hz == before  # gain isn't tracked for follower display


# ── State messages / broadcast ────────────────────────────────────────────────


def test_state_message_owner_and_locked_flags():
    state = relay.RelayState()
    owner, follower = _session(), _session()
    state.control_sessions.update({owner, follower})
    state.current_owner = owner
    owner_msg = state.state_message(owner)
    follower_msg = state.state_message(follower)
    assert owner_msg["owner"] is True and owner_msg["locked"] is True
    assert follower_msg["owner"] is False and follower_msg["locked"] is True
    state.current_owner = None
    assert state.state_message(follower)["locked"] is False


def test_broadcast_state_enqueues_tailored_message_per_session():
    state = relay.RelayState()
    owner, follower = _session(), _session()
    state.control_sessions.update({owner, follower})
    state.current_owner = owner
    state.broadcast_state()
    assert owner.queue.get_nowait()["owner"] is True
    assert follower.queue.get_nowait()["owner"] is False


def test_control_session_enqueue_drops_oldest_when_full():
    session = relay.ControlSession(FakeWriter())
    for index in range(relay.CONTROL_QUEUE_MAX_MESSAGES + 5):
        session.enqueue({"n": index})
    assert session.queue.qsize() == relay.CONTROL_QUEUE_MAX_MESSAGES
    # The oldest were dropped, so the newest survives.
    drained = [session.queue.get_nowait()["n"] for _ in range(session.queue.qsize())]
    assert drained[-1] == relay.CONTROL_QUEUE_MAX_MESSAGES + 4


# ── _handle_control_op ────────────────────────────────────────────────────────


def test_claim_grants_when_free():
    async def run():
        state = relay.RelayState()
        session = _session()
        state.control_sessions.add(session)
        await relay._handle_control_op(state, session, {"op": "claim"})
        assert state.current_owner is session
        message = session.queue.get_nowait()
        assert message["owner"] is True and message["locked"] is True

    asyncio.run(run())


def test_claim_denied_when_already_owned():
    async def run():
        state = relay.RelayState()
        owner, other = _session(), _session()
        state.control_sessions.update({owner, other})
        state.current_owner = owner
        await relay._handle_control_op(state, other, {"op": "claim"})
        assert state.current_owner is owner
        message = other.queue.get_nowait()
        assert message["owner"] is False and message["locked"] is True

    asyncio.run(run())


def test_set_from_owner_applies_and_broadcasts():
    async def run():
        state = relay.RelayState()
        writer = FakeWriter()
        state.upstream_writer = writer
        owner = _session()
        state.control_sessions.add(owner)
        state.current_owner = owner
        await relay._handle_control_op(state, owner, {"op": "set", "center_hz": 102_000_000})
        assert state.center_hz == 102_000_000
        assert bytes(writer.buf) == bytes([1]) + (102_000_000).to_bytes(4, "big")
        assert owner.queue.get_nowait()["center_hz"] == 102_000_000

    asyncio.run(run())


def test_set_from_non_owner_ignored():
    async def run():
        state = relay.RelayState()
        writer = FakeWriter()
        state.upstream_writer = writer
        owner, other = _session(), _session()
        state.control_sessions.update({owner, other})
        state.current_owner = owner
        await relay._handle_control_op(state, other, {"op": "set", "center_hz": 5})
        assert state.center_hz != 5
        assert bytes(writer.buf) == b""  # hardware untouched
        assert other.queue.get_nowait()["owner"] is False  # reflected the truth

    asyncio.run(run())


def test_release_frees_token_and_get_returns_state():
    async def run():
        state = relay.RelayState()
        owner = _session()
        state.control_sessions.add(owner)
        state.current_owner = owner
        await relay._handle_control_op(state, owner, {"op": "release"})
        assert state.current_owner is None
        owner.queue.get_nowait()  # broadcast after release

        other = _session()
        state.control_sessions.add(other)
        await relay._handle_control_op(state, other, {"op": "release"})  # non-owner release: no-op
        await relay._handle_control_op(state, other, {"op": "get"})
        assert other.queue.get_nowait()["event"] == "state"  # release-branch reply
        assert other.queue.get_nowait()["event"] == "state"  # get reply

    asyncio.run(run())


# ── forward_commands gating ───────────────────────────────────────────────────


def _reader(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


def test_forward_commands_gated_while_owned():
    async def run():
        state = relay.RelayState()
        state.current_owner = _session()  # a control client owns the tuner
        upstream = FakeWriter()
        state.upstream_writer = upstream
        await relay.forward_commands(state, _reader(relay._build_command(0x01, 5)), "peer")
        assert bytes(upstream.buf) == b""  # legacy IQ-socket command ignored

    asyncio.run(run())


def test_forward_commands_forwards_and_tracks_when_free():
    async def run():
        state = relay.RelayState()
        upstream = FakeWriter()
        state.upstream_writer = upstream
        command = relay._build_command(0x01, 92_000_000)
        await relay.forward_commands(state, _reader(command), "peer")
        assert bytes(upstream.buf) == command
        assert state.center_hz == 92_000_000

    asyncio.run(run())


def test_forward_commands_drops_when_upstream_down():
    async def run():
        state = relay.RelayState()
        state.upstream_writer = None  # no dongle
        await relay.forward_commands(state, _reader(relay._build_command(0x01, 7)), "peer")
        # No crash and nothing to assert on the (absent) upstream; state untracked.

    asyncio.run(run())


# ── handle_control_client (integration over real sockets) ─────────────────────


def test_control_client_claim_then_disconnect_releases():
    async def run():
        state = relay.RelayState()
        server = await asyncio.start_server(
            lambda reader, writer: relay.handle_control_client(state, reader, writer), "127.0.0.1", 0
        )
        port = server.sockets[0].getsockname()[1]
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            initial = json.loads(await reader.readline())
            assert initial["owner"] is False and initial["locked"] is False
            writer.write(b'{"op":"claim"}\n')
            await writer.drain()
            granted = json.loads(await reader.readline())
            assert granted["owner"] is True and granted["locked"] is True
            writer.close()
            await writer.wait_closed()
            # The owning connection dropping frees the token.
            for _ in range(50):
                if state.current_owner is None:
                    break
                await asyncio.sleep(0.01)
            assert state.current_owner is None
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())


def test_owner_release_notifies_other_followers():
    async def run():
        state = relay.RelayState()
        server = await asyncio.start_server(
            lambda reader, writer: relay.handle_control_client(state, reader, writer), "127.0.0.1", 0
        )
        port = server.sockets[0].getsockname()[1]
        try:
            owner_reader, owner_writer = await asyncio.open_connection("127.0.0.1", port)
            await owner_reader.readline()  # initial
            owner_writer.write(b'{"op":"claim"}\n')
            await owner_writer.drain()
            await owner_reader.readline()  # owner=True

            follower_reader, follower_writer = await asyncio.open_connection("127.0.0.1", port)
            follower_initial = json.loads(await follower_reader.readline())
            assert follower_initial["owner"] is False and follower_initial["locked"] is True

            owner_writer.close()  # owner leaves → token frees → followers notified
            await owner_writer.wait_closed()
            freed = json.loads(await follower_reader.readline())
            assert freed["locked"] is False  # a follower can now take over

            follower_writer.close()
            await follower_writer.wait_closed()
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())
