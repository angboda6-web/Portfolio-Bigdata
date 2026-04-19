from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.config import default_config
from src.data_generator import generate_raw_files
from src.report import build_report
from src.warehouse import (
    build_warehouse,
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_payments,
    clean_products,
    collect_metrics,
    load_and_clean,
    run_data_quality_checks,
)


class PipelineTests(unittest.TestCase):
    def test_cleaning_functions(self) -> None:
        customers = clean_customers(
            [
                {
                    "customer_id": "C0001",
                    "customer_name": " ana lopez ",
                    "email": "ANA.LOPEZ1@EXAMPLE.COM",
                    "city": "madrid",
                    "signup_date": "2024-01-15",
                },
                {
                    "customer_id": "C0002",
                    "customer_name": "bad row",
                    "email": "invalid",
                    "city": "madrid",
                    "signup_date": "not-a-date",
                },
            ]
        )
        products = clean_products(
            [
                {
                    "product_id": "P0001",
                    "product_name": "keyboard",
                    "category": "electronics",
                    "unit_price": "25.5",
                },
                {
                    "product_id": "P0002",
                    "product_name": "broken",
                    "category": "electronics",
                    "unit_price": "-1",
                },
            ]
        )
        orders = clean_orders(
            [
                {
                    "order_id": "O00001",
                    "customer_id": "C0001",
                    "order_date": "2024-01-20",
                    "status": "completed",
                    "payment_method": "card",
                },
                {
                    "order_id": "O00002",
                    "customer_id": "C9999",
                    "order_date": "2024-01-20",
                    "status": "completed",
                    "payment_method": "card",
                },
            ],
            {"C0001"},
        )
        order_items = clean_order_items(
            [
                {
                    "item_id": "I000001",
                    "order_id": "O00001",
                    "product_id": "P0001",
                    "quantity": "2",
                    "unit_price": "25.5",
                    "line_total": "51",
                },
                {
                    "item_id": "I000002",
                    "order_id": "O99999",
                    "product_id": "P0001",
                    "quantity": "2",
                    "unit_price": "25.5",
                    "line_total": "51",
                },
            ],
            {"O00001"},
            {"P0001"},
        )
        payments = clean_payments(
            [
                {
                    "payment_id": "PAY-O00001",
                    "order_id": "O00001",
                    "payment_date": "2024-01-20",
                    "payment_method": "card",
                    "payment_status": "paid",
                    "amount": "51",
                },
                {
                    "payment_id": "PAY-O99999",
                    "order_id": "O99999",
                    "payment_date": "2024-01-20",
                    "payment_method": "card",
                    "payment_status": "paid",
                    "amount": "51",
                },
            ],
            {"O00001"},
        )

        self.assertEqual(len(customers), 1)
        self.assertEqual(customers[0]["customer_name"], "Ana Lopez")
        self.assertEqual(len(products), 1)
        self.assertEqual(len(orders), 1)
        self.assertEqual(len(order_items), 1)
        self.assertEqual(len(payments), 1)

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
            metrics = collect_metrics(config.database_url)
            issues = run_data_quality_checks(config.database_url)
            report_path, metrics_path = build_report(config, config.artifacts_dir)

            self.assertGreater(counts["customers"], 0)
            self.assertGreater(counts["orders"], 0)
            self.assertGreater(metrics["completed_orders"], 0)
            self.assertEqual(issues, [])
            self.assertTrue(report_path.exists())
            self.assertTrue(metrics_path.exists())

    def test_data_generation_is_reproducible(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            config = default_config(base_dir)
            config.raw_dir.mkdir(parents=True, exist_ok=True)

            first = generate_raw_files(config.raw_dir, seed=99, customer_count=5, product_count=4, days=3, orders_per_day=2)
            first_snapshot = (first.customers.read_text(encoding="utf-8"), first.orders.read_text(encoding="utf-8"))

            second_dir = base_dir / "second"
            second_dir.mkdir(parents=True, exist_ok=True)
            second = generate_raw_files(second_dir, seed=99, customer_count=5, product_count=4, days=3, orders_per_day=2)
            second_snapshot = (second.customers.read_text(encoding="utf-8"), second.orders.read_text(encoding="utf-8"))

            self.assertEqual(first_snapshot, second_snapshot)


if __name__ == "__main__":
    unittest.main()
