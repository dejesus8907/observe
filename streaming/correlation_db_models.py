from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, Float, Index, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from netobserv.streaming.db_models import StreamingBase

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

class CorrelationClusterModel(StreamingBase):
    __tablename__ = "streaming_correlation_clusters"
    __table_args__ = (
        Index("ix_corr_clusters_state_updated", "state", "updated_at"),
        Index("ix_corr_clusters_root_event", "root_event_id"),
        Index("ix_corr_clusters_root_assertion", "root_assertion_id"),
        Index("ix_corr_clusters_updated", "updated_at"),
    )
    cluster_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    state: Mapped[str] = mapped_column(String(64), nullable=False)
    root_event_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    root_assertion_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    root_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    cluster_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    summary_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    device_ids_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    subject_keys_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    kind_set_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    metadata_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

class CorrelationMembershipModel(StreamingBase):
    __tablename__ = "streaming_correlation_memberships"
    __table_args__ = (
        Index("ix_corr_members_cluster_joined", "cluster_id", "joined_at"),
        Index("ix_corr_members_event", "event_id"),
        Index("ix_corr_members_assertion", "assertion_id"),
        Index("ix_corr_members_role", "role", "cluster_id"),
    )
    membership_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    cluster_id: Mapped[str] = mapped_column(String(128), nullable=False)
    event_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    assertion_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    role: Mapped[str] = mapped_column(String(64), nullable=False)
    joined_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    explanation_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

class CorrelationLinkModel(StreamingBase):
    __tablename__ = "streaming_correlation_links"
    __table_args__ = (
        Index("ix_corr_links_cluster_created", "cluster_id", "created_at"),
        Index("ix_corr_links_source_event", "source_event_id"),
        Index("ix_corr_links_target_event", "target_event_id"),
        Index("ix_corr_links_type_created", "correlation_type", "created_at"),
    )
    link_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    cluster_id: Mapped[str] = mapped_column(String(128), nullable=False)
    source_event_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    target_event_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    source_assertion_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    target_assertion_id: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    correlation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    reason: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utc_now)
