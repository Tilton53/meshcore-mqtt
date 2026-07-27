"""Durable, thread-safe deduplication for bridge envelopes and packets."""

from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional, Union


class PacketDedupStore:
    """SQLite WAL store with atomic remember-if-new operations."""

    def __init__(self, path: Union[str, Path], ttl_ms: int) -> None:
        if ttl_ms <= 0:
            raise ValueError("ttl_ms must be positive")
        self.path = str(path)
        self.ttl_ms = ttl_ms
        self._lock = threading.RLock()
        self._closed = False
        parent = Path(self.path).parent
        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level="IMMEDIATE"
        )
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS bridge_envelopes (
                envelope_id BLOB PRIMARY KEY,
                seen_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bridge_packets (
                packet_hash TEXT PRIMARY KEY,
                seen_at_ms INTEGER NOT NULL
            );
            """
        )
        self._connection.commit()

    @staticmethod
    def hash_packet(packet: bytes) -> str:
        """Compute stable packet identity."""
        if not isinstance(packet, bytes):
            raise TypeError("packet must be bytes")
        return hashlib.sha256(packet).hexdigest()

    def _remember(self, table: str, key_column: str, key: object, now_ms: int) -> bool:
        with self._lock:
            self._ensure_open()
            self._prune_locked(now_ms)
            cursor = self._connection.execute(
                f"INSERT OR IGNORE INTO {table} ({key_column}, seen_at_ms) VALUES (?, ?)",
                (key, now_ms),
            )
            self._connection.commit()
            return cursor.rowcount == 1

    def remember_envelope(
        self, envelope_id: bytes, now_ms: Optional[int] = None
    ) -> bool:
        """Remember UUID; return True only for first observation."""
        if not isinstance(envelope_id, bytes) or len(envelope_id) != 16:
            raise ValueError("envelope_id must be 16 bytes")
        return self._remember(
            "bridge_envelopes",
            "envelope_id",
            envelope_id,
            _now_ms() if now_ms is None else now_ms,
        )

    def remember_packet(
        self, packet: Union[bytes, str], now_ms: Optional[int] = None
    ) -> bool:
        """Remember packet bytes or an already calculated hash."""
        packet_hash = self.hash_packet(packet) if isinstance(packet, bytes) else packet
        if not isinstance(packet_hash, str) or len(packet_hash) != 64:
            raise ValueError("packet hash must be a 64-character string")
        return self._remember(
            "bridge_packets",
            "packet_hash",
            packet_hash,
            _now_ms() if now_ms is None else now_ms,
        )

    def _prune_locked(self, now_ms: int) -> int:
        cutoff = now_ms - self.ttl_ms
        deleted = 0
        for table in ("bridge_envelopes", "bridge_packets"):
            cursor = self._connection.execute(
                f"DELETE FROM {table} WHERE seen_at_ms < ?", (cutoff,)
            )
            deleted += cursor.rowcount
        if deleted:
            self._connection.commit()
        return deleted

    def prune(self, now_ms: Optional[int] = None) -> int:
        """Remove entries older than configured TTL."""
        with self._lock:
            self._ensure_open()
            deleted = self._prune_locked(_now_ms() if now_ms is None else now_ms)
            self._connection.commit()
            return deleted

    def counts(self) -> dict[str, int]:
        """Return non-sensitive store counts."""
        with self._lock:
            self._ensure_open()
            return {
                "envelopes": int(
                    self._connection.execute(
                        "SELECT COUNT(*) FROM bridge_envelopes"
                    ).fetchone()[0]
                ),
                "packets": int(
                    self._connection.execute(
                        "SELECT COUNT(*) FROM bridge_packets"
                    ).fetchone()[0]
                ),
            }

    def close(self) -> None:
        """Close SQLite connection explicitly."""
        with self._lock:
            if not self._closed:
                self._connection.commit()
                self._connection.close()
                self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("deduplication store is closed")

    def __enter__(self) -> "PacketDedupStore":
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()


def _now_ms() -> int:
    return int(time.time() * 1000)


DurableDedupStore = PacketDedupStore
