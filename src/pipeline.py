from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from .config import default_config
from .public_dataset import DEFAULT_SUPERSTORE_URL, download_public_dataset
from .report import build_report
from .warehouse import (
    build_warehouse,
    collect_metrics,
    load_and_clean,
    persist_metrics,
    run_data_quality_checks,
)

logger = logging.getLogger(__name__)


def run_pipeline(
    base_dir: Path | None = None,
    *,
    database_url: str | None = None,
    dataset_url: str | None = None,
) -> dict[str, object]:
    config = default_config(base_dir, database_url=database_url)
    config.raw_dir.mkdir(parents=True, exist_ok=True)
    config.processed_dir.mkdir(parents=True, exist_ok=True)
    config.warehouse_dir.mkdir(parents=True, exist_ok=True)
    config.artifacts_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Preparing public dataset")
    cleaned = load_and_clean(config.raw_dir, config.processed_dir, source_url=dataset_url)
    logger.info("Building warehouse")
    counts = build_warehouse(config, cleaned)
    logger.info("Collecting metrics")
    metrics = collect_metrics(config.database_url)
    logger.info("Running data quality checks")
    issues = run_data_quality_checks(config.database_url)
    logger.info("Building report")
    report_path, metrics_path = build_report(config, config.artifacts_dir)
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
        choices=["run", "download", "generate", "report"],
        help="Pipeline action to execute",
    )
    parser.add_argument("--base-dir", dest="base_dir", default=None, help="Override the project base directory")
    parser.add_argument("--database-url", dest="database_url", default=None, help="Override the database URL")
    parser.add_argument("--dataset-url", dest="dataset_url", default=None, help="Override the public dataset URL")
    parser.add_argument("--verbose", action="store_true", help="Enable step-by-step logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s",
    )

    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    config = default_config(base_dir, database_url=args.database_url)

    if args.command in {"download", "generate"}:
        raw_path = download_public_dataset(config.raw_dir, args.dataset_url or DEFAULT_SUPERSTORE_URL)
        _print_summary({"raw_path": str(raw_path)})
        return 0

    if args.command == "report":
        report_path, metrics_path = build_report(config, config.artifacts_dir)
        _print_summary({"report_path": str(report_path), "metrics_path": str(metrics_path)})
        return 0

    summary = run_pipeline(base_dir, database_url=args.database_url, dataset_url=args.dataset_url)
    _print_summary(summary)
    return 0
