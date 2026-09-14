"""Execute the approved dbt stages and publish bounded, immutable run evidence."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import NoReturn

_ARTIFACT_NAMES = ("manifest.json", "run_results.json")
_MAX_ARTIFACT_BYTES = 5 * 1024 * 1024
_SHA = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value or "\x00" in value:
        raise RuntimeError(f"missing trusted runtime context: {name}")
    return value


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _repository_root(project_dir: Path) -> Path:
    for candidate in (project_dir, *project_dir.parents):
        marker = candidate / ".git"
        if marker.is_symlink():
            raise RuntimeError("immutable bundle repository marker is unsafe")
        if marker.is_dir() or marker.is_file():
            return candidate.resolve()
    raise RuntimeError("immutable bundle repository marker is missing")


def _bundle_commit(project_dir: Path) -> str:
    root = _repository_root(project_dir)
    completed = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--verify", "HEAD^{commit}"],
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    commit = completed.stdout.strip().lower()
    if completed.returncode or not _SHA.fullmatch(commit):
        raise RuntimeError("immutable bundle commit cannot be verified")
    return commit


def _copy_artifact(source: Path, destination: Path) -> dict[str, object]:
    if source.is_symlink() or not source.is_file():
        return {"status": "missing"}
    size = source.stat().st_size
    if size > _MAX_ARTIFACT_BYTES:
        return {"status": "oversize", "size": size}
    digest = hashlib.sha256()
    written = 0
    with source.open("rb") as input_file, destination.open("xb") as output_file:
        while chunk := input_file.read(1024 * 1024):
            written += len(chunk)
            if written > _MAX_ARTIFACT_BYTES:
                raise RuntimeError("artifact exceeded bounded size during copy")
            digest.update(chunk)
            output_file.write(chunk)
        output_file.flush()
        os.fsync(output_file.fileno())
    return {"status": "stored", "size": written, "sha256": digest.hexdigest()}


def _publish(
    *, artifact_root: Path, project_id: str, generation: str, dag_id: str, run_id: str, commit: str,
    try_number: str, project_workdir: Path, stage: str, exit_code: int,
) -> None:
    final_directory = artifact_root / _digest(dag_id) / _digest(run_id) / commit / try_number
    try:
        final_directory.relative_to(artifact_root)
    except ValueError as exc:
        raise RuntimeError("artifact destination escaped project volume") from exc
    if final_directory.exists():
        raise RuntimeError("artifact attempt already exists")
    final_directory.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=final_directory.parent))
    try:
        files = {
            name: _copy_artifact(project_workdir / "target" / name, staging / name)
            for name in _ARTIFACT_NAMES
        }
        index = {
            "project_id": project_id,
            "generation": generation,
            "dag_id": dag_id,
            "dag_run_id": run_id,
            "bundle_commit_sha": commit,
            "try_number": try_number,
            "stage": stage,
            "exit_code": exit_code,
            "files": files,
        }
        index_path = staging / "index.json"
        with index_path.open("x", encoding="utf-8") as output_file:
            json.dump(index, output_file, separators=(",", ":"), sort_keys=True)
            output_file.write("\n")
            output_file.flush()
            os.fsync(output_file.fileno())
        os.replace(staging, final_directory)
        descriptor = os.open(final_directory.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def main() -> None:
    project_dir = Path(_required_env("CONDUCTOR_DBT_PROJECT_DIR")).resolve()
    profiles_dir = Path(_required_env("CONDUCTOR_DBT_PROFILES_DIR")).resolve()
    artifact_root = Path(_required_env("CONDUCTOR_DBT_ARTIFACT_ROOT")).resolve()
    project_id = _required_env("CONDUCTOR_PROJECT_ID")
    generation = _required_env("CONDUCTOR_RUNTIME_SUBPATH").rsplit("/", 1)[-1]
    dag_id = _required_env("AIRFLOW_CTX_DAG_ID")
    run_id = _required_env("AIRFLOW_CTX_DAG_RUN_ID")
    try_number = _required_env("AIRFLOW_CTX_TRY_NUMBER")
    commit = _bundle_commit(project_dir)
    stage = "deps"
    exit_code = 0
    with tempfile.TemporaryDirectory(prefix="conductor-dbt-") as temporary:
        workdir = Path(temporary) / "project"
        shutil.copytree(project_dir, workdir, symlinks=False)
        for stage, arguments in (("deps", ["dbt", "deps"]), ("run", ["dbt", "run"]), ("test", ["dbt", "test"])):
            completed = subprocess.run(
                [*arguments, "--profiles-dir", str(profiles_dir)], cwd=workdir, check=False
            )
            exit_code = completed.returncode
            if exit_code:
                break
        try:
            _publish(
                artifact_root=artifact_root,
                project_id=project_id,
                generation=generation,
                dag_id=dag_id,
                run_id=run_id,
                commit=commit,
                try_number=try_number,
                project_workdir=workdir,
                stage=stage,
                exit_code=exit_code,
            )
        except Exception as exc:
            if exit_code:
                raise RuntimeError("dbt failed and artifacts could not be published") from exc
            raise RuntimeError("dbt artifacts could not be published") from exc
    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
