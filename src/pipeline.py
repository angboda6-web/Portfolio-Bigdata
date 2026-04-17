from __future__ import annotations

import argparse
import json
from pathlib import Path

from .config import default_config
from .data_generator import generate_raw_files
from .report import build_report
from .warehouse import (
    build_warehouse,
    collect_metrics,
    load_and_clean,
    persist_metrics,
    run_data_quality_checks,
)


def run_pipeline(base_dir: Path | None = None) -> dict[str, object]:
    config = default_config(base_dir)
    config.raw_dir.mkdir(parents=True, exist_ok=True)
    config.processed_dir.mkdir(parents=True, exist_ok=True)
    config.warehouse_dir.mkdir(parents=True, exist_ok=True)
    config.artifacts_dir.mkdir(parents=True, exist_ok=True)

    generate_raw_files(config.raw_dir)
    cleaned = load_and_clean(config.raw_dir, config.processed_dir)
    counts = build_warehouse(config, cleaned)
    metrics = collect_metrics(config.db_path)
    issues = run_data_quality_checks(config.db_path)
    report_path, metrics_path = build_report(config.db_path, config.artifacts_dir)
    persist_metrics(
        {
            "counts": counts,
            "metrics": metrics,
            "quality_issues": issues,
            "report_path": str(report_path),
            "metrics_path": str(metrics_path),
        },
        config.artifacts_dir / "pipeline_summary.json",
    )

    return {
        "counts": counts,
        "metrics": metrics,
        "quality_issues": issues,
        "report_path": str(report_path),
        "metrics_path": str(metrics_path),
    }


def _print_summary(summary: dict[str, object]) -> None:
    print(json.dumps(summary, indent=2, ensure_ascii=False))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Retail ETL portfolio project")
    parser.add_argument(
        "command",
        nargs="?",
        default="run",
        choices=["run", "generate", "report"],
        help="Pipeline action to execute",
    )
    parser.add_argument("--base-dir", dest="base_dir", default=None, help="Override the project base directory")
    args = parser.parse_args(argv)

    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    config = default_config(base_dir)

    if args.command == "generate":
        files = generate_raw_files(config.raw_dir)
        _print_summary({k: str(v) for k, v in files.__dict__.items()})
        return 0

    if args.command == "report":
        report_path, metrics_path = build_report(config.db_path, config.artifacts_dir)
        _print_summary({"report_path": str(report_path), "metrics_path": str(metrics_path)})
        return 0

    summary = run_pipeline(base_dir)
    _print_summary(summary)
    return 0

