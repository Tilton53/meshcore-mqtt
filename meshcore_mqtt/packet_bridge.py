"""Wire protocol helpers for raw MeshCore packet bridging."""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, cast

import cbor2

from .config import MQTT_PATH_SEGMENT_INVALID, PACKET_BRIDGE_MAX_PACKET_SIZE

ENVELOPE_VERSION = 1
MIN_PACKET_SIZE = 1
MAX_PACKET_SIZE = PACKET_BRIDGE_MAX_PACKET_SIZE
FUTURE_TIMESTAMP_TOLERANCE_MS = 5_000
COMMAND_SEND_RAW_PACKET = 0x41


class BridgeEnvelopeError(ValueError):
    """Raised when a bridge envelope fails wire validation."""


def validate_path_segment(value: Any, name: str = "segment") -> str:
    """Validate one MQTT topic path segment."""
    if not isinstance(value, str) or not value:
        raise BridgeEnvelopeError(f"{name} must be a non-empty string")
    if any(char in value for char in MQTT_PATH_SEGMENT_INVALID):
        raise BridgeEnvelopeError(f"{name} contains an MQTT wildcard or separator")
    if any(char.isspace() for char in value):
        raise BridgeEnvelopeError(f"{name} cannot contain whitespace")
    return value


def bridge_topic(topic_root: str, link_id: str, endpoint_id: str) -> str:
    """Construct an exact directional topic below the dedicated bridge root."""
    validate_path_segment(link_id, "link_id")
    validate_path_segment(endpoint_id, "endpoint_id")
    if not isinstance(topic_root, str) or not topic_root.strip():
        raise BridgeEnvelopeError("topic_root must be non-empty")
    topic_root = topic_root.strip().strip("/")
    if not topic_root or any(char in {"+", "#"} for char in topic_root):
        raise BridgeEnvelopeError("topic_root contains an MQTT wildcard")
    segments = topic_root.split("/")
    if any(not segment for segment in segments):
        raise BridgeEnvelopeError("topic_root contains an empty path segment")
    if any(any(char.isspace() for char in segment) for segment in segments):
        raise BridgeEnvelopeError("topic_root cannot contain whitespace")
    return f"{topic_root}/v1/{link_id}/{endpoint_id}/tx"


def packet_sha256(packet: bytes) -> str:
    """Return SHA-256 identity for complete serialized packet bytes."""
    if not isinstance(packet, bytes):
        raise TypeError("packet must be bytes")
    return hashlib.sha256(packet).hexdigest()


def build_send_raw_packet_command(packet: bytes, priority: int = 0) -> bytes:
    """Build companion command 65 without changing packet bytes."""
    if not isinstance(packet, bytes):
        raise TypeError("packet must be bytes")
    if not MIN_PACKET_SIZE <= len(packet) <= MAX_PACKET_SIZE:
        raise ValueError("packet size is outside companion frame bounds")
    if not isinstance(priority, int) or isinstance(priority, bool):
        raise TypeError("priority must be an integer")
    if not 0 <= priority <= 255:
        raise ValueError("priority must fit one command byte")
    return bytes((COMMAND_SEND_RAW_PACKET, priority)) + packet


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class BridgeEnvelope:
    """Versioned CBOR transport envelope."""

    id: bytes
    src: str
    created: int
    ttl: int
    trace: tuple[str, ...]
    packet: bytes
    v: int = ENVELOPE_VERSION

    @classmethod
    def create(
        cls,
        src: str,
        packet: bytes,
        ttl: int,
        trace: Iterable[str],
        envelope_id: Optional[bytes] = None,
        created: Optional[int] = None,
    ) -> "BridgeEnvelope":
        """Create and validate a new envelope."""
        return cls(
            id=envelope_id or uuid.uuid4().bytes,
            src=src,
            created=int(time.time() * 1000) if created is None else created,
            ttl=ttl,
            trace=tuple(trace),
            packet=packet,
        ).validated()

    def as_map(self) -> dict[str, Any]:
        """Return exact CBOR map representation."""
        return {
            "v": self.v,
            "id": self.id,
            "src": self.src,
            "created": self.created,
            "ttl": self.ttl,
            "trace": list(self.trace),
            "packet": self.packet,
        }

    def validated(
        self,
        *,
        now_ms: Optional[int] = None,
        max_bridge_hops: Optional[int] = None,
        local_endpoint: Optional[str] = None,
    ) -> "BridgeEnvelope":
        """Validate field types, lifetime, trace, and packet bounds."""
        if self.v != ENVELOPE_VERSION or not _is_int(self.v):
            raise BridgeEnvelopeError("unsupported envelope version")
        if not isinstance(self.id, bytes) or len(self.id) != 16:
            raise BridgeEnvelopeError("id must be a 16-byte bytestring")
        validate_path_segment(self.src, "src")
        if not _is_int(self.created) or self.created < 0:
            raise BridgeEnvelopeError("created must be a non-negative integer")
        if not _is_int(self.ttl) or self.ttl <= 0:
            raise BridgeEnvelopeError("ttl must be a positive integer")
        if not isinstance(self.trace, (tuple, list)) or not self.trace:
            raise BridgeEnvelopeError("trace must be a non-empty text array")
        for index, endpoint in enumerate(self.trace):
            validate_path_segment(endpoint, f"trace[{index}]")
        if len(set(self.trace)) != len(self.trace):
            raise BridgeEnvelopeError("trace cannot contain duplicate endpoints")
        if max_bridge_hops is not None:
            if max_bridge_hops < 1 or len(self.trace) > max_bridge_hops:
                raise BridgeEnvelopeError("envelope exceeds maximum bridge hops")
        if local_endpoint is not None:
            validate_path_segment(local_endpoint, "local_endpoint")
            if local_endpoint in self.trace:
                raise BridgeEnvelopeError("envelope trace contains local endpoint")
        if not isinstance(self.packet, bytes):
            raise BridgeEnvelopeError("packet must be a bytestring")
        if not MIN_PACKET_SIZE <= len(self.packet) <= MAX_PACKET_SIZE:
            raise BridgeEnvelopeError(
                f"packet size must be between {MIN_PACKET_SIZE} and {MAX_PACKET_SIZE}"
            )

        if now_ms is not None:
            if self.created > now_ms + FUTURE_TIMESTAMP_TOLERANCE_MS:
                raise BridgeEnvelopeError("envelope timestamp is too far in the future")
            if self.created + self.ttl <= now_ms:
                raise BridgeEnvelopeError("envelope has expired")
        return self

    def encode(self) -> bytes:
        """Encode envelope as canonical CBOR."""
        return cast(bytes, cbor2.dumps(self.validated().as_map(), canonical=True))

    @classmethod
    def decode(
        cls,
        payload: bytes,
        *,
        now_ms: Optional[int] = None,
        max_bridge_hops: Optional[int] = None,
        local_endpoint: Optional[str] = None,
    ) -> "BridgeEnvelope":
        """Decode and validate CBOR envelope."""
        if not isinstance(payload, bytes):
            raise BridgeEnvelopeError("CBOR payload must be bytes")
        try:
            decoded = cbor2.loads(payload)
        except Exception as exc:
            raise BridgeEnvelopeError(f"invalid CBOR envelope: {exc}") from exc
        if not isinstance(decoded, Mapping):
            raise BridgeEnvelopeError("envelope must be a CBOR map")
        required = {"v", "id", "src", "created", "ttl", "trace", "packet"}
        if set(decoded) != required:
            raise BridgeEnvelopeError("envelope keys do not match version 1 contract")
        trace = decoded["trace"]
        if not isinstance(trace, list):
            raise BridgeEnvelopeError("trace must be a CBOR array")
        envelope = cls(
            v=decoded["v"],
            id=decoded["id"],
            src=decoded["src"],
            created=decoded["created"],
            ttl=decoded["ttl"],
            trace=tuple(trace),
            packet=decoded["packet"],
        )
        return envelope.validated(
            now_ms=now_ms,
            max_bridge_hops=max_bridge_hops,
            local_endpoint=local_endpoint,
        )


def validate_topic_source(
    topic: str, topic_root: str, link_id: str, source: str
) -> None:
    """Require envelope source to match exact peer topic endpoint."""
    expected = bridge_topic(topic_root, link_id, source)
    if topic != expected:
        raise BridgeEnvelopeError("envelope source does not match MQTT topic")
