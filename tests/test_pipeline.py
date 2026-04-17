from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.config import default_config
from src.data_generator import generate_raw_files
from src.report import build_report
from src.warehouse import build_warehouse, collect_metrics, load_and_clean, run_data_quality_checks


class PipelineTests(unittest.TestCase):
    def test_end_to_end_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            config = default_config(base_dir)
            config.raw_dir.mkdir(parents=True, exist_ok=True)
            config.processed_dir.mkdir(parents=True, exist_ok=True)
            config.warehouse_dir.mkdir(parents=True, exist_ok=True)
            config.artifacts_dir.mkdir(parents=True, exist_ok=True)

            generate_raw_files(config.raw_dir, seed=7, customer_count=20, product_count=8, days=10, orders_per_day=3)
            cleaned = load_and_clean(config.raw_dir, config.processed_dir)
            counts = build_warehouse(config, cleaned)
            metrics = collect_metrics(config.db_path)
            issues = run_data_quality_checks(config.db_path)
            report_path, metrics_path = build_report(config.db_path, config.artifacts_dir)

            self.assertGreater(counts["customers"], 0)
            self.assertGreater(counts["orders"], 0)
            self.assertGreater(metrics["completed_orders"], 0)
            self.assertEqual(issues, [])
            self.assertTrue(report_path.exists())
            self.assertTrue(metrics_path.exists())


if __name__ == "__main__":
    unittest.main()

