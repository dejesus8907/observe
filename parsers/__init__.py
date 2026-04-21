"""Parsers — extract structured records from raw collection results."""

from netobserv.parsers.base import SourceParser, ParsedRecord
from netobserv.parsers.generic import GenericParser
from netobserv.parsers.eos import EOSParser
from netobserv.parsers.ios import IOSParser
from netobserv.parsers.netbox import NetBoxIntendedStateParser

__all__ = [
    "SourceParser",
    "ParsedRecord",
    "GenericParser",
    "EOSParser",
    "IOSParser",
    "NetBoxIntendedStateParser",
]
