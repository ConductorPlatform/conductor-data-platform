from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from app.services.dbt_artifact_reader import (
    ArtifactIntegrityError,
    ArtifactUnavailableError,
    read_run_artifact,
)


PROJECT_ID = "0123456789abcdef0123456789abcdef"
COMMIT = "a" * 40


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _write_attempt(root: Path, *, status: str = "stored", attempt: int = 1) -> Path:
    directory = root / PROJECT_ID / "3" / _digest("dag") / _digest("run") / COMMIT / str(attempt)
    directory.mkdir(parents=True)
    content = b'{"metadata": "immutable"}\n'
    if status == "stored":
        (directory / "manifest.json").write_bytes(content)
        metadata: dict[str, object] = {
            "status": "stored", "size": len(content), "sha256": hashlib.sha256(content).hexdigest()
        }
    else:
        metadata = {"status": status}
    (directory / "index.json").write_text(
        json.dumps(
            {
                "project_id": PROJECT_ID,
                "generation": "3",
                "dag_id": "dag",
                "dag_run_id": "run",
                "bundle_commit_sha": COMMIT,
                "files": {"manifest.json": metadata, "run_results.json": {"status": "missing"}},
            }
        )
    )
    return directory


def test_reader_serves_only_indexed_immutable_run_artifacts(tmp_path: Path) -> None:
    _write_attempt(tmp_path)

    artifact = read_run_artifact(
        artifact_root=tmp_path,
        project_id=PROJECT_ID,
        generation=3,
        dag_id="dag",
        run_id="run",
        bundle_commit_sha=COMMIT,
        artifact_name="manifest.json",
    )

    assert artifact.filename == "manifest.json"
    assert artifact.content == b'{"metadata": "immutable"}\n'


@pytest.mark.parametrize("status", ["missing", "oversize"])
def test_reader_reports_known_unstored_artifact_status(tmp_path: Path, status: str) -> None:
    _write_attempt(tmp_path, status=status)

    with pytest.raises(ArtifactUnavailableError):
        read_run_artifact(
            artifact_root=tmp_path,
            project_id=PROJECT_ID,
            generation=3,
            dag_id="dag",
            run_id="run",
            bundle_commit_sha=COMMIT,
            artifact_name="manifest.json",
        )


def test_reader_rejects_symlinked_or_tampered_artifacts(tmp_path: Path) -> None:
    directory = _write_attempt(tmp_path)
    (directory / "manifest.json").write_text("tampered")

    with pytest.raises(ArtifactIntegrityError):
        read_run_artifact(
            artifact_root=tmp_path,
            project_id=PROJECT_ID,
            generation=3,
            dag_id="dag",
            run_id="run",
            bundle_commit_sha=COMMIT,
            artifact_name="manifest.json",
        )
