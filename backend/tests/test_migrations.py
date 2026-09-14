from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_alembic_has_exactly_one_head() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    config = Config(str(backend_dir / "alembic.ini"))
    config.set_main_option("path_separator", "os")
    config.set_main_option("script_location", str(backend_dir / "alembic"))
    script = ScriptDirectory.from_config(config)

    heads = script.get_heads()

    assert len(heads) == 1, f"Expected exactly one Alembic head, found {len(heads)}: {heads}"
    assert heads[0] == "c2d3e4f5a6b7"


def test_lifecycle_revision_follows_the_merge_revision() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    config = Config(str(backend_dir / "alembic.ini"))
    config.set_main_option("path_separator", "os")
    config.set_main_option("script_location", str(backend_dir / "alembic"))
    script = ScriptDirectory.from_config(config)

    lifecycle_revision = script.get_revision("b1c2d3e4f5a6")

    assert lifecycle_revision is not None
    assert lifecycle_revision.down_revision == "b0c1d2e3f4a5"


def test_warehouse_revision_follows_the_lifecycle_revision() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    config = Config(str(backend_dir / "alembic.ini"))
    config.set_main_option("path_separator", "os")
    config.set_main_option("script_location", str(backend_dir / "alembic"))
    script = ScriptDirectory.from_config(config)

    warehouse_revision = script.get_revision("c2d3e4f5a6b7")

    assert warehouse_revision is not None
    assert warehouse_revision.down_revision == "b1c2d3e4f5a6"
