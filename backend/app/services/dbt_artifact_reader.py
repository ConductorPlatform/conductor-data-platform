"""Project-scoped, integrity-checked retrieval for immutable dbt run artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

_ARTIFACT_NAMES = frozenset({"manifest.json", "run_results.json"})
_MAX_ARTIFACT_BYTES = 5 * 1024 * 1024
_SHA = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")


class ArtifactNotFoundError(RuntimeError):
    pass


class ArtifactUnavailableError(RuntimeError):
    pass


class ArtifactIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True)
class StoredArtifact:
    content: bytes
    filename: str


def read_run_artifact(
    *,
    artifact_root: Path,
    project_id: str,
    generation: int,
    dag_id: str,
    run_id: str,
    bundle_commit_sha: str,
    artifact_name: str,
) -> StoredArtifact:
    """Read only a completed, indexed artifact under its immutable run key."""

    if artifact_name not in _ARTIFACT_NAMES:
        raise ArtifactNotFoundError("artifact name is not supported")
    commit = bundle_commit_sha.lower()
    if not _SHA.fullmatch(commit):
        raise ArtifactNotFoundError("run has no immutable bundle version")
    root = artifact_root / project_id / str(generation)
    attempt_root = root / _digest(dag_id) / _digest(run_id) / commit
    _require_directory(root)
    _require_directory(attempt_root)
    candidates: list[tuple[int, Path]] = []
    for attempt in attempt_root.iterdir():
        if attempt.is_symlink() or not attempt.is_dir() or not attempt.name.isdigit():
            continue
        candidates.append((int(attempt.name), attempt))
    for _number, attempt in sorted(candidates, reverse=True):
        try:
            index = _load_index(attempt)
            _require_matching_index(index, project_id, generation, dag_id, run_id, commit)
            _require_execution_invariant(index, attempt)
            files = index.get("files")
            if not isinstance(files, dict) or artifact_name not in files:
                raise ArtifactIntegrityError("artifact index is invalid")
            status = files[artifact_name]
            if not isinstance(status, dict):
                raise ArtifactIntegrityError("artifact index is invalid")
            if status.get("status") in {"missing", "oversize"}:
                raise ArtifactUnavailableError("artifact was not stored")
            if status.get("status") != "stored":
                raise ArtifactIntegrityError("artifact index has an invalid status")
            return StoredArtifact(_read_verified_file(attempt / artifact_name, status), artifact_name)
        except ArtifactNotFoundError:
            continue
    raise ArtifactNotFoundError("no completed artifact attempt exists")


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _require_directory(path: Path) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError as exc:
        raise ArtifactNotFoundError("artifact path is absent") from exc
    if path.is_symlink() or not stat.S_ISDIR(path_stat.st_mode):
        raise ArtifactIntegrityError("artifact directory is unsafe")


def _load_index(attempt: Path) -> dict[str, object]:
    index_path = attempt / "index.json"
    content = _read_regular_file(index_path, limit=_MAX_ARTIFACT_BYTES)
    try:
        index = json.loads(content)
    except (TypeError, ValueError) as exc:
        raise ArtifactIntegrityError("artifact index is invalid") from exc
    if not isinstance(index, dict) or not isinstance(index.get("files"), dict):
        raise ArtifactIntegrityError("artifact index is invalid")
    return index


def _require_matching_index(
    index: dict[str, object], project_id: str, generation: int, dag_id: str, run_id: str, commit: str
) -> None:
    expected = {
        "project_id": project_id,
        "generation": str(generation),
        "dag_id": dag_id,
        "dag_run_id": run_id,
        "bundle_commit_sha": commit,
    }
    if any(index.get(key) != value for key, value in expected.items()):
        raise ArtifactIntegrityError("artifact index provenance does not match the run")


def _require_execution_invariant(index: dict[str, object], attempt: Path) -> None:
    """Accept evidence only from one complete, bounded dbt execution attempt."""

    try:
        attempt_number = int(attempt.name)
    except ValueError as exc:
        raise ArtifactIntegrityError("artifact attempt is invalid") from exc
    stage = index.get("stage")
    exit_code = index.get("exit_code")
    if (
        attempt_number < 1
        or index.get("try_number") != attempt.name
        or stage not in {"deps", "run", "test"}
        or isinstance(exit_code, bool)
        or not isinstance(exit_code, int)
        or exit_code < 0
        or (exit_code == 0 and stage != "test")
    ):
        raise ArtifactIntegrityError("artifact execution result is invalid")


def _read_verified_file(path: Path, metadata: object) -> bytes:
    if not isinstance(metadata, dict):
        raise ArtifactIntegrityError("artifact index is invalid")
    content = _read_regular_file(path, limit=_MAX_ARTIFACT_BYTES)
    expected_size = metadata.get("size")
    expected_sha = metadata.get("sha256")
    if (
        not isinstance(expected_size, int)
        or expected_size != len(content)
        or not isinstance(expected_sha, str)
        or hashlib.sha256(content).hexdigest() != expected_sha
    ):
        raise ArtifactIntegrityError("artifact integrity verification failed")
    return content


def _read_regular_file(path: Path, *, limit: int) -> bytes:
    try:
        path_stat = path.lstat()
    except FileNotFoundError as exc:
        raise ArtifactNotFoundError("artifact file is absent") from exc
    if path.is_symlink() or not stat.S_ISREG(path_stat.st_mode):
        raise ArtifactIntegrityError("artifact file is unsafe")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        descriptor_stat = os.fstat(descriptor)
        if not stat.S_ISREG(descriptor_stat.st_mode) or descriptor_stat.st_size > limit:
            raise ArtifactIntegrityError("artifact file exceeds its bound")
        chunks: list[bytes] = []
        remaining = limit + 1
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        content = b"".join(chunks)
        if len(content) > limit:
            raise ArtifactIntegrityError("artifact file exceeds its bound")
        return content
    finally:
        os.close(descriptor)
