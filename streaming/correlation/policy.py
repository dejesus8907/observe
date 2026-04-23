from dataclasses import dataclass
@dataclass(slots=True)
class CorrelationPolicy:
    max_window_seconds: float = 30.0
    cause_effect_window_seconds: float = 12.0
    same_subject_bonus: float = 0.30
    same_device_bonus: float = 0.18
    same_interface_bonus: float = 0.30
    topology_neighbor_bonus: float = 0.16
    cause_effect_bonus: float = 0.22
    derivative_penalty: float = 0.10
    minimum_link_confidence: float = 0.55
    root_threshold: float = 0.82
    derivative_threshold: float = 0.68
DefaultCorrelationPolicy = CorrelationPolicy()
