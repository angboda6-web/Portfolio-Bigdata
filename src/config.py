from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .db import default_database_url


@dataclass(frozen=True)
class PipelineConfig:
    base_dir: Path
    raw_dir: Path
    processed_dir: Path
    warehouse_dir: Path
    artifacts_dir: Path
    db_path: Path
    database_url: str


def default_config(base_dir: Path | None = None, database_url: str | None = None) -> PipelineConfig:
    root = base_dir or Path(__file__).resolve().parents[1]
    resolved_database_url = default_database_url(root, database_url or os.getenv("DATABASE_URL"))
    return PipelineConfig(
        base_dir=root,
        raw_dir=root / "data" / "raw",
        processed_dir=root / "data" / "processed",
        warehouse_dir=root / "warehouse",
        artifacts_dir=root / "artifacts",
        db_path=root / "warehouse" / "sales.db",
        database_url=resolved_database_url,
    )

