from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, String, Text, func, text as sa_text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, new_uuid

if TYPE_CHECKING:
    from app.models.project import Project


class ProvisionerKind(StrEnum):
    DOCKER_COMPOSE = "docker_compose"


class ProjectDeployment(Base):
    __tablename__ = "project_deployments"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_uuid)
    project_id: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    provisioner_kind: Mapped[ProvisionerKind] = mapped_column(
        Enum(
            ProvisionerKind,
            name="project_provisioner_kind",
            values_callable=lambda enum: [member.value for member in enum],
        ),
        nullable=False,
        default=ProvisionerKind.DOCKER_COMPOSE,
        server_default=sa_text("'docker_compose'"),
    )
    template_version: Mapped[str] = mapped_column(String(64), nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    compose_project_name: Mapped[str] = mapped_column(String(63), nullable=False, unique=True)
    airflow_external_url: Mapped[str] = mapped_column(Text, nullable=False)
    airflow_db_name: Mapped[str] = mapped_column(String(63), nullable=False, unique=True)
    airflow_db_role: Mapped[str] = mapped_column(String(63), nullable=False, unique=True)
    airflow_db_password_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    airflow_admin_user: Mapped[str] = mapped_column(String(128), nullable=False)
    airflow_admin_password_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    airflow_dev_user: Mapped[str] = mapped_column(String(128), nullable=False)
    airflow_dev_password_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    airflow_viewer_user: Mapped[str] = mapped_column(String(128), nullable=False)
    airflow_viewer_password_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    airflow_integration_user: Mapped[str] = mapped_column(String(128), nullable=False)
    airflow_integration_password_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    # Legacy deployments remain inspectable/retriable.  New deployments always
    # set these deterministic, private warehouse values.
    warehouse_db_name: Mapped[str | None] = mapped_column(String(63), nullable=True, unique=True)
    warehouse_db_role: Mapped[str | None] = mapped_column(String(63), nullable=True, unique=True)
    warehouse_db_password_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    warehouse_schema: Mapped[str | None] = mapped_column(String(63), nullable=True)
    parameters: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=sa_text("'{}'::jsonb"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    project: Mapped[Project] = relationship(back_populates="deployment")
