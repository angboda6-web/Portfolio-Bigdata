from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    base_dir: Path
    raw_dir: Path
    processed_dir: Path
    warehouse_dir: Path
    artifacts_dir: Path
    db_path: Path


def default_config(base_dir: Path | None = None) -> PipelineConfig:
    root = base_dir or Path(__file__).resolve().parents[1]
    return PipelineConfig(
        base_dir=root,
        raw_dir=root / "data" / "raw",
        processed_dir=root / "data" / "processed",
        warehouse_dir=root / "warehouse",
        artifacts_dir=root / "artifacts",
        db_path=root / "warehouse" / "sales.db",
    )

