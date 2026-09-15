from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class DAGSummary(BaseModel):
    dag_id: str
    description: str | None
    is_paused: bool
    latest_run_state: str | None
    latest_run_start: datetime | None
    latest_run_end: datetime | None
    next_dagrun: datetime | None


class DAGRunInfo(BaseModel):
    run_id: str
    state: str
    execution_date: datetime
    start_date: datetime | None
    end_date: datetime | None
    duration: float | None
    commit_sha: str | None = None
    error_summary: str | None = None
    logs_url: str | None = None
    artifacts: list["DAGRunArtifact"] = Field(default_factory=list)


class DAGRunArtifact(BaseModel):
    name: Literal["manifest.json", "run_results.json"]
    download_url: str