"""Inventory source providers — resolve discovery targets."""

from netobserv.inventory_sources.base import InventorySource, InventoryTarget
from netobserv.inventory_sources.static import StaticFileInventorySource
from netobserv.inventory_sources.netbox import NetBoxInventorySource
from netobserv.inventory_sources.ip_range import IPRangeInventorySource

__all__ = [
    "InventorySource",
    "InventoryTarget",
    "StaticFileInventorySource",
    "NetBoxInventorySource",
    "IPRangeInventorySource",
]
