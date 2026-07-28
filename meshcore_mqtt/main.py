"""Main entry point for MeshCore MQTT Bridge."""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

import click

from .config import (
    PACKET_BRIDGE_DEFAULT_TOPIC_ROOT,
    Config,
    ConnectionType,
    PacketBridgeConfig,
)


def setup_logging(level: str) -> None:
    """Set up logging configuration."""
    log_level = getattr(logging, level)

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,  # Override any existing configuration
    )

    # Ensure MeshCore and other third-party libraries respect our log level
    # Set common third-party library loggers
    third_party_loggers = [
        "meshcore",
        "paho",
        "paho.mqtt",
        "paho.mqtt.client",
        "asyncio",
    ]

    for logger_name in third_party_loggers:
        logging.getLogger(logger_name).setLevel(log_level)

    # Set urllib3 and requests to WARNING to reduce noise unless we're in DEBUG mode
    if level != "DEBUG":
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
    else:
        # In DEBUG mode, let urllib3 and requests use the same log level
        logging.getLogger("urllib3").setLevel(log_level)
        logging.getLogger("requests").setLevel(log_level)


@click.command()
@click.option(
    "--config-file",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file (JSON or YAML)",
)
@click.option(
    "--mqtt-broker",
    help="MQTT broker address",
)
@click.option(
    "--mqtt-port",
    type=int,
    default=1883,
    help="MQTT broker port (default: 1883)",
)
@click.option(
    "--mqtt-username",
    help="MQTT username",
)
@click.option(
    "--mqtt-password",
    help="MQTT password",
)
@click.option(
    "--mqtt-topic-prefix",
    default="meshcore",
    help="MQTT topic prefix (default: meshcore)",
)
@click.option(
    "--mqtt-qos",
    type=click.IntRange(0, 2),
    default=0,
    help="MQTT QoS level (default: 0)",
)
@click.option(
    "--mqtt-retain/--no-mqtt-retain",
    default=False,
    help="Enable MQTT message retention (default: disabled)",
)
@click.option(
    "--mqtt-tls/--no-mqtt-tls",
    default=False,
    help="Enable MQTT TLS/SSL connection (default: disabled)",
)
@click.option(
    "--mqtt-tls-ca-cert",
    help="Path to CA certificate file for TLS",
)
@click.option(
    "--mqtt-tls-client-cert",
    help="Path to client certificate file for TLS",
)
@click.option(
    "--mqtt-tls-client-key",
    help="Path to client private key file for TLS",
)
@click.option(
    "--mqtt-tls-insecure/--no-mqtt-tls-insecure",
    default=False,
    help="Disable TLS certificate verification (default: disabled)",
)
@click.option(
    "--meshcore-connection",
    type=click.Choice([conn.value for conn in ConnectionType]),
    help="MeshCore connection type",
)
@click.option(
    "--meshcore-address",
    help="MeshCore device address",
)
@click.option(
    "--meshcore-port",
    type=int,
    help="MeshCore device port (for TCP connections)",
)
@click.option(
    "--meshcore-baudrate",
    type=int,
    default=115200,
    help="MeshCore baudrate for serial connections (default: 115200)",
)
@click.option(
    "--meshcore-timeout",
    type=int,
    default=5,
    help="MeshCore operation timeout in seconds (default: 5)",
)
@click.option(
    "--meshcore-auto-fetch-restart-delay",
    type=click.IntRange(1, 60),
    default=5,
    help="Delay in seconds before restarting auto-fetch after NO_MORE_MSGS "
    "(default: 5)",
)
@click.option(
    "--meshcore-events",
    help="Comma-separated list of MeshCore event types to subscribe to",
)
@click.option(
    "--packet-bridge-enabled/--no-packet-bridge",
    default=None,
    help="Enable raw packet bridging",
)
@click.option("--packet-bridge-link-id", default=None, help="Packet bridge link ID")
@click.option(
    "--packet-bridge-topic-root",
    default=None,
    help="Dedicated MQTT root topic for raw packet bridge traffic",
)
@click.option(
    "--packet-bridge-endpoint-id", default=None, help="Local packet bridge endpoint ID"
)
@click.option(
    "--packet-bridge-peer-ids",
    default=None,
    help="Comma-separated packet bridge peer endpoint IDs",
)
@click.option(
    "--packet-bridge-envelope-ttl-ms",
    type=int,
    default=None,
    help="Bridge envelope TTL",
)
@click.option(
    "--packet-bridge-dedup-ttl-ms", type=int, default=None, help="Bridge dedup TTL"
)
@click.option(
    "--packet-bridge-dedup-db", default=None, help="Bridge SQLite database path"
)
@click.option(
    "--packet-bridge-max-queue", type=int, default=None, help="Bridge queue capacity"
)
@click.option(
    "--packet-bridge-max-hops", type=int, default=None, help="Maximum bridge hops"
)
@click.option(
    "--packet-bridge-transmit-priority",
    type=int,
    default=None,
    help="Companion raw packet transmit priority",
)
@click.option(
    "--packet-bridge-tx-delay-min-ms",
    type=int,
    default=None,
    help="Minimum MQTT-to-radio delay",
)
@click.option(
    "--packet-bridge-tx-delay-max-ms",
    type=int,
    default=None,
    help="Maximum MQTT-to-radio delay",
)
@click.option(
    "--meshcore-message-retry-count",
    type=click.IntRange(0, 10),
    default=3,
    help="Number of times to retry sending a message on failure (default: 3)",
)
@click.option(
    "--meshcore-message-retry-delay",
    type=click.FloatRange(0.5, 30.0),
    default=2.0,
    help="Base delay in seconds between message retries (default: 2.0)",
)
@click.option(
    "--meshcore-reset-path-on-failure/--no-meshcore-reset-path-on-failure",
    default=True,
    help="Reset routing path after max retries and try once more (default: enabled)",
)
@click.option(
    "--meshcore-message-initial-delay",
    type=click.FloatRange(0.0, 60.0),
    default=5.0,
    help="Initial delay in seconds before sending the first message (default: 5.0)",
)
@click.option(
    "--meshcore-message-send-delay",
    type=click.FloatRange(0.0, 60.0),
    default=10.0,
    help="Delay in seconds between consecutive message sends (default: 10.0)",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
    help="Logging level (default: INFO)",
)
@click.option(
    "--env",
    is_flag=True,
    help="Load configuration from environment variables",
)
def main(
    config_file: Optional[Path],
    mqtt_broker: Optional[str],
    mqtt_port: int,
    mqtt_username: Optional[str],
    mqtt_password: Optional[str],
    mqtt_topic_prefix: str,
    mqtt_qos: int,
    mqtt_retain: bool,
    mqtt_tls: bool,
    mqtt_tls_ca_cert: Optional[str],
    mqtt_tls_client_cert: Optional[str],
    mqtt_tls_client_key: Optional[str],
    mqtt_tls_insecure: bool,
    meshcore_connection: Optional[str],
    meshcore_address: Optional[str],
    meshcore_port: Optional[int],
    meshcore_baudrate: int,
    meshcore_timeout: int,
    meshcore_auto_fetch_restart_delay: int,
    meshcore_events: Optional[str],
    packet_bridge_enabled: Optional[bool],
    packet_bridge_link_id: Optional[str],
    packet_bridge_topic_root: Optional[str],
    packet_bridge_endpoint_id: Optional[str],
    packet_bridge_peer_ids: Optional[str],
    packet_bridge_envelope_ttl_ms: Optional[int],
    packet_bridge_dedup_ttl_ms: Optional[int],
    packet_bridge_dedup_db: Optional[str],
    packet_bridge_max_queue: Optional[int],
    packet_bridge_max_hops: Optional[int],
    packet_bridge_transmit_priority: Optional[int],
    packet_bridge_tx_delay_min_ms: Optional[int],
    packet_bridge_tx_delay_max_ms: Optional[int],
    meshcore_message_retry_count: int,
    meshcore_message_retry_delay: float,
    meshcore_reset_path_on_failure: bool,
    meshcore_message_initial_delay: float,
    meshcore_message_send_delay: float,
    log_level: str,
    env: bool,
) -> None:
    """MeshCore to MQTT Bridge.

    Bridge messages between MeshCore devices and MQTT brokers.
    Configuration can be provided via command-line arguments,
    configuration file, or environment variables.
    """
    try:
        # Load configuration in order of precedence:
        # 1. Command line arguments (highest priority)
        # 2. Configuration file
        # 3. Environment variables (lowest priority)
        click_context = click.get_current_context()
        packet_bridge_enabled_provided = (
            click_context.get_parameter_source("packet_bridge_enabled")
            != click.core.ParameterSource.DEFAULT
        )

        if config_file:
            config = Config.from_file(config_file)
        elif env:
            config = Config.from_env()
        else:
            # Build config from command line arguments
            if not mqtt_broker or not meshcore_connection or not meshcore_address:
                click.echo(
                    "Error: --mqtt-broker, --meshcore-connection, and "
                    "--meshcore-address are required when not using a config file",
                    err=True,
                )
                sys.exit(1)

            from .config import MeshCoreConfig, MQTTConfig

            mqtt_config = MQTTConfig(
                broker=mqtt_broker,
                port=mqtt_port,
                username=mqtt_username,
                password=mqtt_password,
                topic_prefix=mqtt_topic_prefix,
                qos=mqtt_qos,
                retain=mqtt_retain,
                tls_enabled=mqtt_tls,
                tls_ca_cert=mqtt_tls_ca_cert,
                tls_client_cert=mqtt_tls_client_cert,
                tls_client_key=mqtt_tls_client_key,
                tls_insecure=mqtt_tls_insecure,
            )

            # Parse events if provided
            events = (
                Config.parse_events_string(meshcore_events) if meshcore_events else None
            )

            meshcore_config = MeshCoreConfig(
                connection_type=ConnectionType(meshcore_connection),
                address=meshcore_address,
                port=meshcore_port,
                baudrate=meshcore_baudrate,
                timeout=meshcore_timeout,
                auto_fetch_restart_delay=meshcore_auto_fetch_restart_delay,
                events=(
                    events
                    if events is not None
                    else MeshCoreConfig.model_fields["events"].default
                ),
                message_retry_count=meshcore_message_retry_count,
                message_retry_delay=meshcore_message_retry_delay,
                reset_path_on_failure=meshcore_reset_path_on_failure,
                message_initial_delay=meshcore_message_initial_delay,
                message_send_delay=meshcore_message_send_delay,
            )

            config = Config(
                mqtt=mqtt_config,
                meshcore=meshcore_config,
                log_level=log_level,
            )

            if packet_bridge_enabled_provided:
                assert packet_bridge_enabled is not None
                config.packet_bridge = PacketBridgeConfig(
                    enabled=packet_bridge_enabled,
                    topic_root=packet_bridge_topic_root
                    or PACKET_BRIDGE_DEFAULT_TOPIC_ROOT,
                    link_id=packet_bridge_link_id or "",
                    endpoint_id=packet_bridge_endpoint_id or "",
                    peer_ids=(
                        packet_bridge_peer_ids.split(",")
                        if packet_bridge_peer_ids
                        else []
                    ),
                    envelope_ttl_ms=packet_bridge_envelope_ttl_ms or 30_000,
                    dedup_ttl_ms=packet_bridge_dedup_ttl_ms or 120_000,
                    dedup_db=packet_bridge_dedup_db or "packet-bridge.sqlite3",
                    max_queue=packet_bridge_max_queue or 128,
                    max_bridge_hops=packet_bridge_max_hops or 2,
                    transmit_priority=packet_bridge_transmit_priority or 0,
                    tx_delay_min_ms=(
                        packet_bridge_tx_delay_min_ms
                        if packet_bridge_tx_delay_min_ms is not None
                        else 3_000
                    ),
                    tx_delay_max_ms=(
                        packet_bridge_tx_delay_max_ms
                        if packet_bridge_tx_delay_max_ms is not None
                        else 5_000
                    ),
                )

        # Override config with any provided command line arguments
        if mqtt_broker:
            config.mqtt.broker = mqtt_broker
        if mqtt_username:
            config.mqtt.username = mqtt_username
        if mqtt_password:
            config.mqtt.password = mqtt_password
        if meshcore_connection:
            config.meshcore.connection_type = ConnectionType(meshcore_connection)
        if meshcore_address:
            config.meshcore.address = meshcore_address
        if meshcore_port:
            config.meshcore.port = meshcore_port
        if meshcore_baudrate != 115200:  # Only override if different from default
            config.meshcore.baudrate = meshcore_baudrate
        if meshcore_timeout != 5:  # Only override if different from default
            config.meshcore.timeout = meshcore_timeout
        if (
            meshcore_auto_fetch_restart_delay != 5
        ):  # Only override if different from default
            config.meshcore.auto_fetch_restart_delay = meshcore_auto_fetch_restart_delay
        if meshcore_events:
            config.meshcore.events = Config.parse_events_string(meshcore_events)

        bridge_options_provided = any(
            option_provided
            for option_provided in (
                packet_bridge_enabled_provided,
                packet_bridge_link_id is not None,
                packet_bridge_topic_root is not None,
                packet_bridge_endpoint_id is not None,
                packet_bridge_peer_ids is not None,
                packet_bridge_envelope_ttl_ms is not None,
                packet_bridge_dedup_ttl_ms is not None,
                packet_bridge_dedup_db is not None,
                packet_bridge_max_queue is not None,
                packet_bridge_max_hops is not None,
                packet_bridge_transmit_priority is not None,
                packet_bridge_tx_delay_min_ms is not None,
                packet_bridge_tx_delay_max_ms is not None,
            )
        )
        if bridge_options_provided:
            current = config.packet_bridge
            if packet_bridge_enabled_provided:
                assert packet_bridge_enabled is not None
                effective_bridge_enabled = packet_bridge_enabled
            else:
                effective_bridge_enabled = current.enabled if current else True
            config.packet_bridge = PacketBridgeConfig(
                enabled=effective_bridge_enabled,
                topic_root=packet_bridge_topic_root
                or (
                    current.topic_root if current else PACKET_BRIDGE_DEFAULT_TOPIC_ROOT
                ),
                link_id=packet_bridge_link_id or (current.link_id if current else ""),
                endpoint_id=packet_bridge_endpoint_id
                or (current.endpoint_id if current else ""),
                peer_ids=(
                    packet_bridge_peer_ids.split(",")
                    if packet_bridge_peer_ids
                    else (current.peer_ids if current else [])
                ),
                envelope_ttl_ms=(
                    packet_bridge_envelope_ttl_ms
                    if packet_bridge_envelope_ttl_ms is not None
                    else (current.envelope_ttl_ms if current else 30_000)
                ),
                dedup_ttl_ms=(
                    packet_bridge_dedup_ttl_ms
                    if packet_bridge_dedup_ttl_ms is not None
                    else (current.dedup_ttl_ms if current else 120_000)
                ),
                dedup_db=packet_bridge_dedup_db
                or (current.dedup_db if current else "packet-bridge.sqlite3"),
                max_queue=(
                    packet_bridge_max_queue
                    if packet_bridge_max_queue is not None
                    else (current.max_queue if current else 128)
                ),
                max_bridge_hops=(
                    packet_bridge_max_hops
                    if packet_bridge_max_hops is not None
                    else (current.max_bridge_hops if current else 2)
                ),
                transmit_priority=(
                    packet_bridge_transmit_priority
                    if packet_bridge_transmit_priority is not None
                    else (current.transmit_priority if current else 0)
                ),
                tx_delay_min_ms=(
                    packet_bridge_tx_delay_min_ms
                    if packet_bridge_tx_delay_min_ms is not None
                    else (current.tx_delay_min_ms if current else 3_000)
                ),
                tx_delay_max_ms=(
                    packet_bridge_tx_delay_max_ms
                    if packet_bridge_tx_delay_max_ms is not None
                    else (current.tx_delay_max_ms if current else 5_000)
                ),
            )
        if config.packet_bridge and config.packet_bridge.enabled:
            if config.meshcore.connection_type != ConnectionType.SERIAL:
                raise ValueError(
                    "packet bridging requires meshcore connection_type: serial"
                )
            if "RX_LOG_DATA" not in config.meshcore.events:
                config.meshcore.events.append("RX_LOG_DATA")

        # Set up logging
        setup_logging(config.log_level)
        logger = logging.getLogger(__name__)

        logger.info("Starting MeshCore MQTT Bridge")
        logger.info(f"MQTT Broker: {config.mqtt.broker}:{config.mqtt.port}")
        logger.info(
            f"MeshCore: {config.meshcore.connection_type.value}://"
            f"{config.meshcore.address}"
        )

        # Run the bridge application
        asyncio.run(run_bridge(config))

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


async def run_bridge(config: Config) -> None:
    """Run the MeshCore MQTT bridge."""
    from .bridge_coordinator import BridgeCoordinator

    logger = logging.getLogger(__name__)
    bridge = BridgeCoordinator(config)

    try:
        # Start the bridge
        await bridge.start()

        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("Bridge interrupted by user")
    except Exception as e:
        logger.error(f"Bridge error: {e}")
        raise
    finally:
        # Clean shutdown
        await bridge.stop()


if __name__ == "__main__":
    main()
