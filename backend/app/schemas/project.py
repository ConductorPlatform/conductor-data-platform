from __future__ import annotations

import re
from datetime import datetime

from pydantic import BaseModel, field_validator

from app.models.project import ProjectLifecycleStatus
from app.models.project_lifecycle_job import LifecycleJobStatus, LifecycleOperation


class ProjectCreateRequest(BaseModel):
    name: str
    slug: str | None = None
    description: str | None = None

    @field_validator("slug")
    @classmethod
    def validate_slug(cls, v: str | None) -> str | None:
        if v and not re.match(r"^[a-z0-9]+(-[a-z0-9]+)*$", v):
            raise ValueError("slug must be lowercase alphanumeric with hyphens")
        return v


class ProjectUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None


class ProjectResponse(BaseModel):
    id: str
    name: str
    slug: str
    description: str | None
    self_approve_enabled: bool
    lifecycle_status: ProjectLifecycleStatus
    created_at: datetime
    updated_at: datetime
    member_count: int = 0
    role: str | None = None  # Current user's role in this project


class ProjectOperationResponse(BaseModel):
    id: str
    operation: LifecycleOperation
    status: LifecycleJobStatus


class ProjectOperationStatusResponse(ProjectOperationResponse):
    project_status: ProjectLifecycleStatus
    current_step: str | None
    attempt: int
    max_attempts: int
    error_code: str | None
    error_message: str | None


class ProjectCreateResponse(BaseModel):
    project: ProjectResponse
    operation: ProjectOperationResponse
