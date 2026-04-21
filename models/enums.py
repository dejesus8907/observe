"""Enumerations for the canonical model."""

from enum import Enum


class DeviceStatus(str, Enum):
    ACTIVE = "active"
    PLANNED = "planned"
    STAGED = "staged"
    FAILED = "failed"
    DECOMMISSIONING = "decommissioning"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


class InterfaceAdminState(str, Enum):
    UP = "up"
    DOWN = "down"
    UNKNOWN = "unknown"


class InterfaceOperState(str, Enum):
    UP = "up"
    DOWN = "down"
    TESTING = "testing"
    UNKNOWN = "unknown"
    DORMANT = "dormant"
    NOT_PRESENT = "not_present"
    LOWER_LAYER_DOWN = "lower_layer_down"


class InterfaceType(str, Enum):
    ETHERNET = "ethernet"
    LOOPBACK = "loopback"
    TUNNEL = "tunnel"
    LAG = "lag"
    VIRTUAL = "virtual"
    MANAGEMENT = "management"
    SUBINTERFACE = "subinterface"
    OTHER = "other"
    UNKNOWN = "unknown"


class VLANStatus(str, Enum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    UNKNOWN = "unknown"


class RouteProtocol(str, Enum):
    CONNECTED = "connected"
    STATIC = "static"
    OSPF = "ospf"
    BGP = "bgp"
    ISIS = "isis"
    RIP = "rip"
    EIGRP = "eigrp"
    AGGREGATE = "aggregate"
    OTHER = "other"
    UNKNOWN = "unknown"


class RouteType(str, Enum):
    IPV4 = "ipv4"
    IPV6 = "ipv6"
    UNKNOWN = "unknown"


class EdgeType(str, Enum):
    PHYSICAL = "physical"
    L2_ADJACENCY = "l2_adjacency"
    L3_ADJACENCY = "l3_adjacency"
    BGP_PEER = "bgp_peer"
    OSPF_ADJACENCY = "ospf_adjacency"
    LAG_MEMBER = "lag_member"
    VLAN_TRUNK = "vlan_trunk"
    INFERRED = "inferred"
    MANUAL_OVERRIDE = "manual_override"


class EdgeConfidence(str, Enum):
    HIGH = "high"        # Direct bidirectional evidence
    MEDIUM = "medium"    # Single-side evidence
    LOW = "low"          # Inferred from indirect sources
    CONFLICT = "conflict"  # Contradictory evidence


class DriftSeverity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class MismatchType(str, Enum):
    MISSING_IN_ACTUAL = "missing_in_actual"
    MISSING_IN_INTENDED = "missing_in_intended"
    STATE_MISMATCH = "state_mismatch"
    IDENTITY_MISMATCH = "identity_mismatch"
    RELATIONSHIP_MISMATCH = "relationship_mismatch"
    UNRESOLVED_DEPENDENCY = "unresolved_dependency"
    DUPLICATE_CANDIDATE = "duplicate_candidate"
    UNSUPPORTED_MAPPING = "unsupported_mapping"
    AMBIGUOUS_MATCH = "ambiguous_match"


class WorkflowStage(str, Enum):
    TARGET_RESOLUTION = "TARGET_RESOLUTION"
    CREDENTIAL_BINDING = "CREDENTIAL_BINDING"
    COLLECTION = "COLLECTION"
    NORMALIZATION = "NORMALIZATION"
    SNAPSHOT_PERSIST = "SNAPSHOT_PERSIST"
    TOPOLOGY_BUILD = "TOPOLOGY_BUILD"
    VALIDATION = "VALIDATION"
    SYNC_PLAN = "SYNC_PLAN"
    REPORTING = "REPORTING"
    COMPLETE = "COMPLETE"


class WorkflowStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PARTIAL = "partial"


class SyncMode(str, Enum):
    VALIDATION_ONLY = "validation_only"
    DRY_RUN = "dry_run"
    APPROVED = "approved"
    SELECTIVE = "selective"


class SyncActionType(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    NOOP = "noop"
    SKIP = "skip"


class SnapshotStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class BGPSessionState(str, Enum):
    IDLE = "idle"
    CONNECT = "connect"
    ACTIVE = "active"
    OPENSENT = "opensent"
    OPENCONFIRM = "openconfirm"
    ESTABLISHED = "established"
    UNKNOWN = "unknown"


class OSPFState(str, Enum):
    DOWN = "down"
    ATTEMPT = "attempt"
    INIT = "init"
    TWO_WAY = "2-way"
    EXSTART = "exstart"
    EXCHANGE = "exchange"
    LOADING = "loading"
    FULL = "full"
    UNKNOWN = "unknown"


class InventorySourceType(str, Enum):
    STATIC_FILE = "static_file"
    NETBOX = "netbox"
    IP_RANGE = "ip_range"
    HOST_LIST = "host_list"
    PLUGIN = "plugin"
    DYNAMIC = "dynamic"


class ConnectorType(str, Enum):
    SSH = "ssh"
    REST = "rest"
    SNMP = "snmp"
    VENDOR_SDK = "vendor_sdk"


class NormalizationWarning(str, Enum):
    MISSING_FIELD = "missing_field"
    ENUM_TRANSLATION_FALLBACK = "enum_translation_fallback"
    UNRESOLVED_REFERENCE = "unresolved_reference"
    PARSE_ERROR = "parse_error"
    CONFIDENCE_LOW = "confidence_low"
    FRESHNESS_STALE = "freshness_stale"
