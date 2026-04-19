from __future__ import annotations

import argparse
import logging
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

logger = logging.getLogger(__name__)

def run_pipeline(
    base_dir: Path | None = None,
    *,
    seed: int = 42,
    customers: int = 80,
    products: int = 18,
    days: int = 90,
    orders_per_day: int = 8,
) -> dict[str, object]:
    config = default_config(base_dir)
    config.raw_dir.mkdir(parents=True, exist_ok=True)
    config.processed_dir.mkdir(parents=True, exist_ok=True)
    config.warehouse_dir.mkdir(parents=True, exist_ok=True)
    config.artifacts_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Generating raw datasets")
    generate_raw_files(
        config.raw_dir,
        seed=seed,
        customer_count=customers,
        product_count=products,
        days=days,
        orders_per_day=orders_per_day,
    )
    logger.info("Cleaning raw data")
    cleaned = load_and_clean(config.raw_dir, config.processed_dir)
    logger.info("Building warehouse")
    counts = build_warehouse(config, cleaned)
    logger.info("Collecting metrics")
    metrics = collect_metrics(config.db_path)
    logger.info("Running data quality checks")
    issues = run_data_quality_checks(config.db_path)
    logger.info("Building report")
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
    parser.add_argument("--seed", type=int, default=42, help="Random seed for synthetic data generation")
    parser.add_argument("--customers", type=int, default=80, help="Number of customers to generate")
    parser.add_argument("--products", type=int, default=18, help="Number of products to generate")
    parser.add_argument("--days", type=int, default=90, help="Number of days to generate")
    parser.add_argument(
        "--orders-per-day",
        type=int,
        default=8,
        help="Average number of orders per day",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable step-by-step logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s",
    )

    base_dir = Path(args.base_dir).resolve() if args.base_dir else None
    config = default_config(base_dir)

    if args.command == "generate":
        files = generate_raw_files(
            config.raw_dir,
            seed=args.seed,
            customer_count=args.customers,
            product_count=args.products,
            days=args.days,
            orders_per_day=args.orders_per_day,
        )
        _print_summary({k: str(v) for k, v in files.__dict__.items()})
        return 0

    if args.command == "report":
        report_path, metrics_path = build_report(config.db_path, config.artifacts_dir)
        _print_summary({"report_path": str(report_path), "metrics_path": str(metrics_path)})
        return 0

    summary = run_pipeline(
        base_dir,
        seed=args.seed,
        customers=args.customers,
        products=args.products,
        days=args.days,
        orders_per_day=args.orders_per_day,
    )
    _print_summary(summary)
    return 0
