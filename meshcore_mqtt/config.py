"""Configuration system for MeshCore MQTT Bridge."""

import json
import os
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

MQTT_PATH_SEGMENT_INVALID = {"/", "+", "#"}
PACKET_BRIDGE_MAX_PACKET_SIZE = 176


class ConnectionType(str, Enum):
    """Supported MeshCore connection types."""

    SERIAL = "serial"
    BLE = "ble"
    TCP = "tcp"


class MQTTConfig(BaseModel):
    """MQTT broker configuration."""

    broker: str = Field(..., description="MQTT broker address")
    port: int = Field(default=1883, description="MQTT broker port")
    username: Optional[str] = Field(default=None, description="MQTT username")
    password: Optional[str] = Field(default=None, description="MQTT password")
    topic_prefix: str = Field(default="meshcore", description="MQTT topic prefix")
    qos: int = Field(default=0, ge=0, le=2, description="Quality of Service level")
    retain: bool = Field(default=False, description="Message retention flag")

    # TLS configuration
    tls_enabled: bool = Field(default=False, description="Enable TLS/SSL connection")
    tls_ca_cert: Optional[str] = Field(
        default=None, description="Path to CA certificate file"
    )
    tls_client_cert: Optional[str] = Field(
        default=None, description="Path to client certificate file"
    )
    tls_client_key: Optional[str] = Field(
        default=None, description="Path to client private key file"
    )
    tls_insecure: bool = Field(
        default=False, description="Disable certificate verification"
    )

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """Validate MQTT port number."""
        if not 1 <= v <= 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v

    @field_validator("tls_ca_cert", "tls_client_cert", "tls_client_key")
    @classmethod
    def validate_tls_files(cls, v: Optional[str], info: Any) -> Optional[str]:
        """Validate TLS certificate and key file paths."""
        if v is not None and v.strip():
            file_path = Path(v.strip())
            if not file_path.exists():
                raise ValueError(f"TLS file not found: {v}")
            if not file_path.is_file():
                raise ValueError(f"TLS path is not a file: {v}")
        return v


class MeshCoreConfig(BaseModel):
    """MeshCore device configuration."""

    connection_type: ConnectionType = Field(..., description="Connection type")
    address: str = Field(..., description="Device address")
    port: Optional[int] = Field(default=None, description="Device port for TCP")
    baudrate: int = Field(default=115200, description="Baudrate for serial connections")
    timeout: int = Field(default=5, gt=0, description="Operation timeout in seconds")
    auto_fetch_restart_delay: int = Field(
        default=5,
        ge=1,
        le=60,
        description="Delay in seconds before restarting auto-fetch after NO_MORE_MSGS",
    )
    events: List[str] = Field(
        default=[
            "CONTACT_MSG_RECV",
            "CHANNEL_MSG_RECV",
            "DEVICE_INFO",
            "BATTERY",
            "NEW_CONTACT",
            "ADVERTISEMENT",
            "TRACE_DATA",
            "TELEMETRY_RESPONSE",
            "CHANNEL_INFO",
        ],
        description="List of MeshCore event types to subscribe to",
    )
    message_retry_count: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Number of times to retry sending a message on failure",
    )
    message_retry_delay: float = Field(
        default=2.0,
        ge=0.5,
        le=30.0,
        description=(
            "Base delay in seconds between message retries (exponential backoff)"
        ),
    )
    reset_path_on_failure: bool = Field(
        default=True,
        description="Reset routing path after max retries and try once more",
    )
    message_initial_delay: float = Field(
        default=15.0,
        ge=0.0,
        le=60.0,
        description="Initial delay in seconds before sending the first message",
    )
    message_send_delay: float = Field(
        default=15.0,
        ge=0.0,
        le=60.0,
        description="Delay in seconds between consecutive message sends",
    )

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: Optional[int], info: Any) -> Optional[int]:
        """Validate port is provided for TCP connections."""
        # Get connection_type from the validation context
        connection_type = info.data.get("connection_type") if info.data else None

        # Set default port for TCP if None provided
        if connection_type == ConnectionType.TCP and v is None:
            v = 5000

        if v is not None and not 1 <= v <= 65535:
            raise ValueError("Port must be between 1 and 65535")

        return v

    @field_validator("events")
    @classmethod
    def validate_events(cls, v: List[str]) -> List[str]:
        """Validate event types are valid MeshCore EventType names."""
        # Normalize to uppercase for case-insensitive validation
        normalized_events = [event.upper() for event in v]

        # Valid MeshCore event types (based on actual EventType enum)
        valid_events = {
            "CONTACT_MSG_RECV",
            "CHANNEL_MSG_RECV",
            "CONNECTED",
            "DISCONNECTED",
            "LOGIN_SUCCESS",
            "LOGIN_FAILED",
            "MESSAGES_WAITING",
            "DEVICE_INFO",
            "BATTERY",
            "NEW_CONTACT",
            "TRACE_DATA",
            "ADVERTISEMENT",
            "TELEMETRY_RESPONSE",
            "CONTACTS",
            "SELF_INFO",
            "CHANNEL_INFO",
            "RX_LOG_DATA",
        }

        invalid_events = [
            event for event in normalized_events if event not in valid_events
        ]
        if invalid_events:
            raise ValueError(
                f"Invalid event types: {invalid_events}. "
                f"Valid events: {sorted(valid_events)}"
            )

        return normalized_events


class PacketBridgeConfig(BaseModel):
    """Optional raw MeshCore packet bridge configuration."""

    enabled: bool = Field(default=True, description="Enable packet bridging")
    link_id: str = Field(default="", description="Shared bridge link identifier")
    endpoint_id: str = Field(default="", description="Local bridge endpoint identifier")
    peer_ids: List[str] = Field(default_factory=list)
    envelope_ttl_ms: int = Field(default=30_000)
    dedup_ttl_ms: int = Field(default=120_000)
    dedup_db: str = Field(default="packet-bridge.sqlite3")
    max_queue: int = Field(default=128)
    max_bridge_hops: int = Field(default=2)
    transmit_priority: int = Field(default=0)
    tx_delay_min_ms: int = Field(default=3_000)
    tx_delay_max_ms: int = Field(default=5_000)

    @field_validator("link_id", "endpoint_id", mode="before")
    @classmethod
    def normalize_segment(cls, value: str) -> str:
        """Normalize and validate one MQTT path segment."""
        if not isinstance(value, str):
            raise ValueError("bridge path segments must be strings")
        value = value.strip()
        if not value:
            return value
        if any(char in value for char in MQTT_PATH_SEGMENT_INVALID):
            raise ValueError("bridge path segments cannot contain '/', '+' or '#'")
        if any(char.isspace() for char in value):
            raise ValueError("bridge path segments cannot contain whitespace")
        return value

    @field_validator("peer_ids", mode="before")
    @classmethod
    def normalize_peer_ids(cls, value: Any) -> List[str]:
        """Normalize comma-separated or list peer identifiers."""
        if value is None:
            return []
        if isinstance(value, str):
            value = [item.strip() for item in value.split(",") if item.strip()]
        if not isinstance(value, (list, tuple, set)):
            raise ValueError("peer_ids must be a list or comma-separated string")
        return [cls.normalize_segment(item) for item in value]

    @field_validator("envelope_ttl_ms")
    @classmethod
    def validate_envelope_ttl(cls, value: int) -> int:
        if not 1 <= value <= 86_400_000:
            raise ValueError("envelope_ttl_ms must be between 1 and 86400000")
        return value

    @field_validator("dedup_ttl_ms")
    @classmethod
    def validate_dedup_ttl(cls, value: int) -> int:
        if not 1 <= value <= 604_800_000:
            raise ValueError("dedup_ttl_ms must be between 1 and 604800000")
        return value

    @field_validator("max_queue")
    @classmethod
    def validate_queue_size(cls, value: int) -> int:
        if not 1 <= value <= 10_000:
            raise ValueError("max_queue must be between 1 and 10000")
        return value

    @field_validator("max_bridge_hops")
    @classmethod
    def validate_hops(cls, value: int) -> int:
        if not 1 <= value <= 32:
            raise ValueError("max_bridge_hops must be between 1 and 32")
        return value

    @field_validator("transmit_priority")
    @classmethod
    def validate_priority(cls, value: int) -> int:
        if not 0 <= value <= 255:
            raise ValueError("transmit_priority must be between 0 and 255")
        return value

    @field_validator("tx_delay_min_ms", "tx_delay_max_ms")
    @classmethod
    def validate_delay(cls, value: int) -> int:
        if not 0 <= value <= 3_600_000:
            raise ValueError("transmit delays must be between 0 and 3600000")
        return value

    @model_validator(mode="after")
    def validate_relationships(self) -> "PacketBridgeConfig":
        if self.enabled:
            if not self.link_id:
                raise ValueError("link_id must not be empty when bridge is enabled")
            if not self.endpoint_id:
                raise ValueError("endpoint_id must not be empty when bridge is enabled")
        if len(self.peer_ids) != len(set(self.peer_ids)):
            raise ValueError("peer_ids must be unique")
        if self.endpoint_id and self.endpoint_id in self.peer_ids:
            raise ValueError("endpoint_id cannot occur in peer_ids")
        if self.tx_delay_min_ms > self.tx_delay_max_ms:
            raise ValueError("tx_delay_min_ms must be <= tx_delay_max_ms")
        if self.dedup_db.strip() == "":
            raise ValueError("dedup_db must not be empty")
        return self


class Config(BaseModel):
    """Main application configuration."""

    mqtt: MQTTConfig
    meshcore: MeshCoreConfig
    packet_bridge: Optional[PacketBridgeConfig] = None
    log_level: str = Field(default="INFO", description="Logging level")

    @model_validator(mode="after")
    def validate_packet_bridge(self) -> "Config":
        """Apply cross-section bridge requirements."""
        if self.packet_bridge and self.packet_bridge.enabled:
            if self.meshcore.connection_type != ConnectionType.SERIAL:
                raise ValueError(
                    "packet bridging requires meshcore connection_type: serial"
                )
            if "RX_LOG_DATA" not in self.meshcore.events:
                self.meshcore.events.append("RX_LOG_DATA")
        return self

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate logging level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v.upper()

    @classmethod
    def from_file(cls, config_path: Union[str, Path]) -> "Config":
        """Load configuration from a file (JSON or YAML)."""
        config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r") as f:
            if config_path.suffix.lower() in [".yaml", ".yml"]:
                try:
                    data = yaml.safe_load(f)
                except yaml.YAMLError as e:
                    raise ValueError(f"Invalid YAML configuration: {e}")
            else:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON configuration: {e}")

        return cls(**data)

    @classmethod
    def parse_events_string(cls, events_str: str) -> List[str]:
        """Parse comma-separated event string into list."""
        if not events_str or events_str.strip() == "":
            return []
        return [
            event.strip().upper() for event in events_str.split(",") if event.strip()
        ]

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        mqtt_config = MQTTConfig(
            broker=os.getenv("MQTT_BROKER", ""),
            port=int(os.getenv("MQTT_PORT", "1883")),
            username=os.getenv("MQTT_USERNAME"),
            password=os.getenv("MQTT_PASSWORD"),
            topic_prefix=os.getenv("MQTT_TOPIC_PREFIX", "meshcore"),
            qos=int(os.getenv("MQTT_QOS", "0")),
            retain=os.getenv("MQTT_RETAIN", "false").lower() == "true",
            tls_enabled=os.getenv("MQTT_TLS_ENABLED", "false").lower() == "true",
            tls_ca_cert=os.getenv("MQTT_TLS_CA_CERT"),
            tls_client_cert=os.getenv("MQTT_TLS_CLIENT_CERT"),
            tls_client_key=os.getenv("MQTT_TLS_CLIENT_KEY"),
            tls_insecure=os.getenv("MQTT_TLS_INSECURE", "false").lower() == "true",
        )

        # Parse events from environment variable if provided
        events_env = os.getenv("MESHCORE_EVENTS")
        events = cls.parse_events_string(events_env) if events_env else None

        meshcore_config = MeshCoreConfig(
            connection_type=ConnectionType(os.getenv("MESHCORE_CONNECTION", "tcp")),
            address=os.getenv("MESHCORE_ADDRESS", ""),
            port=(
                int(os.getenv("MESHCORE_PORT", "5000"))
                if os.getenv("MESHCORE_PORT")
                else None
            ),
            baudrate=int(os.getenv("MESHCORE_BAUDRATE", "115200")),
            timeout=int(os.getenv("MESHCORE_TIMEOUT", "5")),
            auto_fetch_restart_delay=int(
                os.getenv("MESHCORE_AUTO_FETCH_RESTART_DELAY", "5")
            ),
            events=(
                events
                if events is not None
                else MeshCoreConfig.model_fields["events"].default
            ),
            message_retry_count=int(os.getenv("MESHCORE_MESSAGE_RETRY_COUNT", "3")),
            message_retry_delay=float(os.getenv("MESHCORE_MESSAGE_RETRY_DELAY", "2.0")),
            reset_path_on_failure=os.getenv(
                "MESHCORE_RESET_PATH_ON_FAILURE", "true"
            ).lower()
            == "true",
            message_initial_delay=float(
                os.getenv("MESHCORE_MESSAGE_INITIAL_DELAY", "15.0")
            ),
            message_send_delay=float(os.getenv("MESHCORE_MESSAGE_SEND_DELAY", "15.0")),
        )

        bridge_enabled_env = os.getenv("PACKET_BRIDGE_ENABLED")
        peer_ids_env = os.getenv("PACKET_BRIDGE_PEER_IDS")
        packet_bridge = None
        if bridge_enabled_env is not None:
            packet_bridge = PacketBridgeConfig(
                enabled=bridge_enabled_env.lower() in {"1", "true", "yes", "on"},
                link_id=os.getenv("PACKET_BRIDGE_LINK_ID", ""),
                endpoint_id=os.getenv("PACKET_BRIDGE_ENDPOINT_ID", ""),
                peer_ids=peer_ids_env.split(",") if peer_ids_env else [],
                envelope_ttl_ms=int(
                    os.getenv("PACKET_BRIDGE_ENVELOPE_TTL_MS", "30000")
                ),
                dedup_ttl_ms=int(os.getenv("PACKET_BRIDGE_DEDUP_TTL_MS", "120000")),
                dedup_db=os.getenv("PACKET_BRIDGE_DEDUP_DB", "packet-bridge.sqlite3"),
                max_queue=int(os.getenv("PACKET_BRIDGE_MAX_QUEUE", "128")),
                max_bridge_hops=int(os.getenv("PACKET_BRIDGE_MAX_HOPS", "2")),
                transmit_priority=int(
                    os.getenv("PACKET_BRIDGE_TRANSMIT_PRIORITY", "0")
                ),
                tx_delay_min_ms=int(os.getenv("PACKET_BRIDGE_TX_DELAY_MIN_MS", "3000")),
                tx_delay_max_ms=int(os.getenv("PACKET_BRIDGE_TX_DELAY_MAX_MS", "5000")),
            )

        return cls(
            mqtt=mqtt_config,
            meshcore=meshcore_config,
            packet_bridge=packet_bridge,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
