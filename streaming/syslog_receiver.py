"""Structured syslog ingest pipeline — RFC 5424 / RFC 3164 receiver.

Syslog is universally available on every network device that has ever been
manufactured. It is the lowest-common-denominator event stream. In DoD networks,
syslog is often the only real-time signal available from legacy or classified
devices that cannot run gNMI agents.

This receiver:
1. Listens for syslog messages on UDP 514 (default) or TCP 514/6514
2. Parses RFC 5424 structured syslog and RFC 3164 BSD syslog
3. Normalizes messages into a structured dict
4. Passes to SyslogClassifier to produce ChangeEvents

DoD-specific considerations:
- TLS syslog (RFC 5425) should be used where available to prevent
  message injection or traffic analysis on syslog streams
- This receiver supports a pre-shared device mapping so log source IPs
  are immediately resolved to canonical device IDs
- High-volume sites may need a dedicated syslog concentrator; this receiver
  is designed for single-site or lab deployment
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from netobserv.streaming.classifier import SyslogClassifier
from netobserv.streaming.adapter_helpers import AdapterObservation, observation_from_change_event, publish_observation
from netobserv.streaming.evidence_mapper import evidence_record_from_change_event
from netobserv.streaming.events import ChangeEvent, ChangeKind, ChangeSeverity, EventSource

logger = logging.getLogger("streaming.syslog")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# RFC 5424 parser
# ---------------------------------------------------------------------------

# RFC 5424: <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID [SD] MSG
_RFC5424_RE = re.compile(
    r"^<(\d{1,3})>"          # priority
    r"(\d+)\s+"              # version
    r"(\S+)\s+"              # timestamp
    r"(\S+)\s+"              # hostname
    r"(\S+)\s+"              # app-name
    r"(\S+)\s+"              # procid
    r"(\S+)\s+"              # msgid
    r"(?:\[.*?\]\s*)?"       # structured-data (optional, simplified)
    r"(.*)"                  # message
)

# RFC 3164 BSD: <PRI>Mmm DD HH:MM:SS HOSTNAME PROGRAM[PID]: MSG
_RFC3164_RE = re.compile(
    r"^<(\d{1,3})>"
    r"([A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(\S+)\s+"              # hostname
    r"([^:\[]+)(?:\[(\d+)\])?:\s*"   # program[pid]
    r"(.*)"                  # message
)


def parse_syslog_line(line: str, source_ip: str = "unknown") -> dict[str, Any]:
    """Parse a syslog line into a structured dict."""
    line = line.strip()

    m = _RFC5424_RE.match(line)
    if m:
        priority = int(m.group(1))
        facility = priority >> 3
        severity = priority & 0x07
        return {
            "raw": line,
            "source_ip": source_ip,
            "hostname": m.group(4) if m.group(4) != "-" else source_ip,
            "program": m.group(5) if m.group(5) != "-" else "",
            "timestamp": m.group(3),
            "facility": facility,
            "severity": severity,
            "message": m.group(8),
        }

    m = _RFC3164_RE.match(line)
    if m:
        priority = int(m.group(1))
        facility = priority >> 3
        severity = priority & 0x07
        return {
            "raw": line,
            "source_ip": source_ip,
            "hostname": m.group(3),
            "program": m.group(4).strip(),
            "timestamp": m.group(2),
            "facility": facility,
            "severity": severity,
            "message": m.group(6),
        }

    # Fallback: treat entire line as message
    return {
        "raw": line,
        "source_ip": source_ip,
        "hostname": source_ip,
        "program": "",
        "timestamp": _utc_now().isoformat(),
        "facility": 0,
        "severity": 6,
        "message": line,
    }


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class SyslogReceiverConfig:
    listen_host: str = "0.0.0.0"
    udp_port: int = 1514           # 514 requires root
    tcp_port: int | None = 1514    # Also listen on TCP for reliable delivery
    max_message_bytes: int = 8192
    # Source filter
    allowed_sources: list[str] = field(default_factory=list)
    # IP → device_id mapping
    source_ip_to_device_id: dict[str, str] = field(default_factory=dict)
    # Drop events below this syslog severity level (0=emergency, 7=debug)
    # Default 6 = informational — drop debug (7)
    min_severity_level: int = 6
    # Rate limiting per source IP: max messages per second
    rate_limit_per_source: int = 200


# ---------------------------------------------------------------------------
# Async UDP Protocol
# ---------------------------------------------------------------------------


class _SyslogUdpProtocol(asyncio.DatagramProtocol):
    def __init__(self, receiver: "SyslogReceiver") -> None:
        self._receiver = receiver

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        source_ip = addr[0]
        try:
            line = data.decode("utf-8", errors="replace")
            asyncio.ensure_future(self._receiver._handle_message(line, source_ip))
        except Exception as exc:
            logger.debug("Syslog UDP decode error from %s: %s", source_ip, exc)


class _SyslogTcpProtocol(asyncio.Protocol):
    def __init__(self, receiver: "SyslogReceiver") -> None:
        self._receiver = receiver
        self._transport: asyncio.Transport | None = None
        self._buf = b""
        self._peer_ip = "unknown"

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport  # type: ignore[assignment]
        peer = transport.get_extra_info("peername")
        self._peer_ip = peer[0] if peer else "unknown"

    def data_received(self, data: bytes) -> None:
        self._buf += data
        while b"\n" in self._buf:
            line_bytes, self._buf = self._buf.split(b"\n", 1)
            try:
                line = line_bytes.decode("utf-8", errors="replace")
                asyncio.ensure_future(self._receiver._handle_message(line, self._peer_ip))
            except Exception as exc:
                logger.debug("Syslog TCP decode error from %s: %s", self._peer_ip, exc)


# ---------------------------------------------------------------------------
# Syslog Receiver
# ---------------------------------------------------------------------------


class SyslogReceiver:
    """UDP + TCP syslog listener that classifies messages into ChangeEvents."""

    def __init__(
        self,
        config: SyslogReceiverConfig,
        event_bus: Any,
    ) -> None:
        self._config = config
        self._event_bus = event_bus
        self._classifier = SyslogClassifier()
        self._udp_transport: Any = None
        self._tcp_server: Any = None
        self._message_count = 0
        self._dropped_count = 0
        self._rate_counters: dict[str, int] = {}

    def _to_observation(self, event: ChangeEvent) -> AdapterObservation:
        """Return the primary adapter output."""
        return observation_from_change_event(event)

    async def start(self) -> None:
        loop = asyncio.get_event_loop()

        # UDP listener
        self._udp_transport, _ = await loop.create_datagram_endpoint(
            lambda: _SyslogUdpProtocol(self),
            local_addr=(self._config.listen_host, self._config.udp_port),
        )
        logger.info(
            "Syslog UDP listener started",
            extra={"host": self._config.listen_host, "port": self._config.udp_port},
        )

        # TCP listener (optional)
        if self._config.tcp_port:
            self._tcp_server = await loop.create_server(
                lambda: _SyslogTcpProtocol(self),
                self._config.listen_host,
                self._config.tcp_port,
            )
            logger.info(
                "Syslog TCP listener started",
                extra={"host": self._config.listen_host, "port": self._config.tcp_port},
            )

    async def stop(self) -> None:
        if self._udp_transport:
            self._udp_transport.close()
        if self._tcp_server:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
        logger.info("Syslog receiver stopped")

    async def _handle_message(self, line: str, source_ip: str) -> None:
        """Parse, filter, and classify one syslog line."""
        self._message_count += 1

        # Source filter
        if self._config.allowed_sources and source_ip not in self._config.allowed_sources:
            return

        # Rate limiting per source
        self._rate_counters[source_ip] = self._rate_counters.get(source_ip, 0) + 1
        if self._rate_counters[source_ip] > self._config.rate_limit_per_source:
            self._dropped_count += 1
            return

        # Parse
        parsed = parse_syslog_line(line, source_ip)

        # Severity gate
        if parsed["severity"] > self._config.min_severity_level:
            return

        # Resolve device_id
        device_id = self._config.source_ip_to_device_id.get(source_ip, parsed["hostname"])
        parsed["device_id"] = device_id

        # Classify
        event = self._classifier.classify(parsed)
        if event:
            asyncio.ensure_future(self._emit(event))

    async def _emit(self, event: ChangeEvent) -> None:
        """Publish through event bus using evidence-first contract."""
        evidence = evidence_record_from_change_event(event)
        try:
            await self._event_bus.publish_adapter_observation(
                evidence=evidence,
                source_event=event,
                fallback_event=event,
            )
        except Exception as exc:
            self._dropped_count += 1
            logger.warning("Syslog event publish failed: %s", exc)

    def reset_rate_counters(self) -> None:
        """Call this once per second to reset rate limit windows."""
        self._rate_counters.clear()

    @property
    def stats(self) -> dict[str, int]:
        return {
            "messages_received": self._message_count,
            "messages_dropped": self._dropped_count,
        }
