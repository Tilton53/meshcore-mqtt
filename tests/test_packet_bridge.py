"""Focused tests for raw packet bridge wire and durable state."""

import time
from pathlib import Path

import cbor2
import pytest

from meshcore_mqtt.packet_bridge import (
    BridgeEnvelope,
    BridgeEnvelopeError,
    bridge_topic,
    build_send_raw_packet_command,
    packet_sha256,
)
from meshcore_mqtt.packet_dedup import PacketDedupStore


def test_envelope_round_trip_preserves_packet_bytes() -> None:
    packet = bytes(range(1, 40))
    envelope = BridgeEnvelope.create("a", packet, 30_000, ["a"])

    decoded = BridgeEnvelope.decode(envelope.encode(), now_ms=envelope.created)

    assert decoded == envelope
    assert decoded.packet == packet


def test_envelope_rejects_bad_version_and_types() -> None:
    envelope = BridgeEnvelope.create("a", b"packet", 1000, ["a"])
    raw = envelope.as_map()
    raw["v"] = 2
    with pytest.raises(BridgeEnvelopeError, match="unsupported"):
        BridgeEnvelope.decode(cbor2.dumps(raw))

    raw = envelope.as_map()
    raw["packet"] = "not bytes"
    with pytest.raises(BridgeEnvelopeError, match="bytestring"):
        BridgeEnvelope.decode(cbor2.dumps(raw))


def test_envelope_expiry_future_and_trace_checks() -> None:
    now = int(time.time() * 1000)
    expired = BridgeEnvelope.create("a", b"packet", 10, ["a"], created=now - 20)
    with pytest.raises(BridgeEnvelopeError, match="expired"):
        expired.validated(now_ms=now)

    future = BridgeEnvelope.create("a", b"packet", 1000, ["a"], created=now + 6000)
    with pytest.raises(BridgeEnvelopeError, match="future"):
        future.validated(now_ms=now)

    with pytest.raises(BridgeEnvelopeError, match="local endpoint"):
        BridgeEnvelope.create("b", b"packet", 1000, ["a", "b"]).validated(
            local_endpoint="a"
        )


def test_topic_and_command_contract() -> None:
    assert bridge_topic("meshcore/bridge", "backhaul-1", "a") == (
        "meshcore/bridge/v1/backhaul-1/a/tx"
    )
    with pytest.raises(BridgeEnvelopeError):
        bridge_topic("meshcore/bridge", "bad/segment", "a")
    with pytest.raises(BridgeEnvelopeError):
        bridge_topic("meshcore/+", "backhaul-1", "a")
    assert build_send_raw_packet_command(b"abc", 7) == b"\x41\x07abc"
    assert packet_sha256(b"abc") == (
        "ba7816bf8f01cfea414140de5dae2223" "b00361a396177a9cb410ff61f20015ad"
    )


def test_dedup_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "dedup.sqlite3"
    envelope_id = bytes(range(16))
    packet = b"packet-bytes"

    store = PacketDedupStore(path, ttl_ms=1000)
    assert store.remember_envelope(envelope_id, now_ms=1000)
    assert not store.remember_envelope(envelope_id, now_ms=1001)
    assert store.remember_packet(packet, now_ms=1000)
    assert not store.remember_packet(packet, now_ms=1001)
    store.close()

    reopened = PacketDedupStore(path, ttl_ms=1000)
    assert not reopened.remember_envelope(envelope_id, now_ms=1002)
    assert not reopened.remember_packet(packet, now_ms=1002)
    assert reopened.remember_envelope(envelope_id, now_ms=3001)
    reopened.close()
