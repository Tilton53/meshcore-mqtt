"""Worker-level packet bridge behavior tests."""

from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import paho.mqtt.client as mqtt
import pytest
from meshcore.events import Event, EventType

from meshcore_mqtt.config import (
    Config,
    ConnectionType,
    MeshCoreConfig,
    MQTTConfig,
    PacketBridgeConfig,
)
from meshcore_mqtt.meshcore_worker import MeshCoreWorker
from meshcore_mqtt.message_queue import Message, MessageType, reset_message_bus
from meshcore_mqtt.mqtt_worker import MQTTWorker
from meshcore_mqtt.packet_bridge import BridgeEnvelope


class FakePublishInfo:
    """Minimal successful paho publish result for worker tests."""

    rc = mqtt.MQTT_ERR_SUCCESS

    def wait_for_publish(self, timeout: float | None = None) -> None:
        del timeout


class FakeMQTTClient:
    """Minimal connected MQTT client for bridge publication tests."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, int, bool]] = []

    def is_connected(self) -> bool:
        return True

    def publish(
        self, topic: str, payload: bytes, *, qos: int, retain: bool
    ) -> FakePublishInfo:
        self.published.append((topic, payload, qos, retain))
        return FakePublishInfo()


def bridge_config(tmp_path: Path, *, min_delay: int = 0, max_delay: int = 0) -> Config:
    return Config(
        mqtt=MQTTConfig(broker="localhost", retain=True, qos=0),
        meshcore=MeshCoreConfig(
            connection_type=ConnectionType.SERIAL, address="/dev/fake"
        ),
        packet_bridge=PacketBridgeConfig(
            enabled=True,
            topic_root="raw-bridge",
            link_id="link",
            endpoint_id="a",
            peer_ids=["b"],
            dedup_db=str(tmp_path / "bridge.sqlite3"),
            tx_delay_min_ms=min_delay,
            tx_delay_max_ms=max_delay,
        ),
    )


@pytest.mark.asyncio
async def test_local_raw_packet_publishes_to_dedicated_topic(tmp_path: Path) -> None:
    reset_message_bus()
    config = bridge_config(tmp_path)
    worker = MQTTWorker(config)
    client = FakeMQTTClient()
    worker.client = cast(mqtt.Client, client)
    worker._connected = True

    packet = b"serialized"
    envelope = BridgeEnvelope.create("a", packet, 30_000, ["a"])
    await worker._handle_meshcore_raw_packet(
        Message.create(
            MessageType.MESHCORE_RAW_PACKET,
            "meshcore",
            "mqtt",
            {"envelope": envelope},
        )
    )

    assert len(client.published) == 1
    topic, payload, qos, retain = client.published[0]
    assert topic == "raw-bridge/v1/link/a/tx"
    assert BridgeEnvelope.decode(payload).packet == packet
    assert qos == 1
    assert retain is False
    await worker.stop()


@pytest.mark.asyncio
async def test_rx_log_event_reaches_mqtt_publication(tmp_path: Path) -> None:
    reset_message_bus()
    config = bridge_config(tmp_path)
    meshcore_worker = MeshCoreWorker(config)
    mqtt_worker = MQTTWorker(config)
    client = FakeMQTTClient()
    mqtt_worker.client = cast(mqtt.Client, client)
    mqtt_worker._connected = True

    meshcore_worker._handle_bridge_rx_event(
        Event(EventType.RX_LOG_DATA, {"payload": "73657269616c697a6564"})
    )
    message = await mqtt_worker.inbox.get(timeout=1.0)
    assert message is not None
    await mqtt_worker._handle_inbox_message(message)

    assert len(client.published) == 1
    assert client.published[0][0] == "raw-bridge/v1/link/a/tx"
    assert meshcore_worker._bridge_stats["invalid_local"] == 0
    await mqtt_worker.stop()
    await meshcore_worker.stop()


@pytest.mark.asyncio
async def test_raw_packet_command_uses_exact_wire_bytes(tmp_path: Path) -> None:
    reset_message_bus()
    worker = MeshCoreWorker(bridge_config(tmp_path))
    sender = AsyncMock()
    worker.meshcore = SimpleNamespace(
        commands=SimpleNamespace(send=sender),
    )

    await worker._send_raw_packet_command(b"serialized", priority=9)

    sender.assert_awaited_once()
    assert sender.await_args is not None
    assert sender.await_args.args[0] == b"\x41\x09serialized"
    assert sender.await_args.args[1] == [EventType.OK, EventType.ERROR]
    await worker.stop()


@pytest.mark.asyncio
async def test_remote_packet_schedules_then_matching_rf_cancels(
    tmp_path: Path,
) -> None:
    reset_message_bus()
    worker = MeshCoreWorker(bridge_config(tmp_path, min_delay=3000, max_delay=5000))
    envelope = BridgeEnvelope.create("b", b"serialized", 30_000, ["b"])

    await worker._handle_mqtt_raw_packet(
        Message.create(
            MessageType.MQTT_RAW_PACKET,
            "mqtt",
            "meshcore",
            {"envelope": envelope},
        )
    )
    assert len(worker._pending_injections) == 1
    assert (
        3.0
        <= next(iter(worker._pending_injections.values())).deadline
        - __import__("time").monotonic()
        <= 5.0
    )

    worker._handle_bridge_rx_event(
        Event(EventType.RX_LOG_DATA, {"payload": "73657269616c697a6564"})
    )
    assert not worker._pending_injections
    assert worker._bridge_stats["rf_cancelled"] == 1
    await worker.stop()
