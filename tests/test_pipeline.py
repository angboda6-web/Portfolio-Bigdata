from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.config import default_config
from src.public_dataset import download_public_dataset
from src.report import build_report
from src.warehouse import build_warehouse, collect_metrics, load_and_clean, run_data_quality_checks


SAMPLE_SUPERSTORE = """RowID\tOrderID\tOrderDate\tShipDate\tShipMode\tCustomerID\tCustomerName\tSegment\tCountry\tCity\tState\tPostal Code\tRegion\tProductID\tCategory\tSubCategory\tProduct Name\tSales\tQuantity\tDiscount\tProfit
1\tCA-2016-152156\t11/8/2016\t11/11/2016\tSecond Class\tCG-12520\tClaire Gute\tConsumer\tUnited States\tHenderson\tKentucky\t42420\tSouth\tFUR-BO-10001798\tFurniture\tBookcases\tBush Somerset Collection Bookcase\t261.96\t2\t0\t41.9136
2\tCA-2016-152156\t11/8/2016\t11/11/2016\tSecond Class\tCG-12520\tClaire Gute\tConsumer\tUnited States\tHenderson\tKentucky\t42420\tSouth\tFUR-CH-10000454\tFurniture\tChairs\tHon Deluxe Fabric Upholstered Stacking Chairs, Rounded Back\t731.94\t3\t0\t219.582
3\tCA-2016-138688\t6/12/2016\t6/16/2016\tSecond Class\tDV-13045\tDarrin Van Huff\tCorporate\tUnited States\tLos Angeles\tCalifornia\t90036\tWest\tOFF-LA-10000240\tOffice Supplies\tLabels\tSelf-Adhesive Address Labels for Typewriters by Universal\t14.62\t2\t0\t6.8714
4\tUS-2015-108966\t10/11/2015\t10/18/2015\tStandard Class\tSO-20335\tSean O'Donnell\tConsumer\tUnited States\tFort Lauderdale\tFlorida\t33311\tSouth\tFUR-TA-10000577\tFurniture\tTables\tBretford CR4500 Series Slim Rectangular Table\t957.5775\t5\t0.45\t-383.031
5\tUS-2015-108966\t10/11/2015\t10/18/2015\tStandard Class\tSO-20335\tSean O'Donnell\tConsumer\tUnited States\tFort Lauderdale\tFlorida\t33311\tSouth\tOFF-ST-10000760\tOffice Supplies\tStorage\tEldon Fold 'N Roll Cart System\t22.368\t2\t0.2\t2.5164
"""


def _make_temp_source(base_dir: Path) -> Path:
    source = base_dir / "sample_superstore.tsv"
    source.write_text(SAMPLE_SUPERSTORE, encoding="utf-8")
    return source


class PipelineTests(unittest.TestCase):
    def test_public_dataset_download(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            raw_dir = base_dir / "data" / "raw"
            source = _make_temp_source(base_dir)

            raw_path = download_public_dataset(raw_dir, source_url=source.as_uri())

            self.assertTrue(raw_path.exists())
            self.assertEqual(raw_path.read_text(encoding="utf-8"), SAMPLE_SUPERSTORE)

    def test_cleaning_and_modeling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            sqlite_url = f"sqlite:///{(base_dir / 'warehouse' / 'sales.db').resolve().as_posix()}"
            config = default_config(base_dir, database_url=sqlite_url)
            source = _make_temp_source(base_dir)

            cleaned = load_and_clean(config.raw_dir, config.processed_dir, source_url=source.as_uri())

            self.assertEqual(cleaned.customers.shape[0], 3)
            self.assertEqual(cleaned.products.shape[0], 5)
            self.assertEqual(cleaned.orders.shape[0], 3)
            self.assertEqual(cleaned.order_items.shape[0], 5)

            counts = build_warehouse(config, cleaned)
            metrics = collect_metrics(config.database_url)
            issues = run_data_quality_checks(config.database_url)
            report_path, metrics_path = build_report(config, config.artifacts_dir)

            self.assertEqual(counts["customers"], 3)
            self.assertEqual(metrics["orders"], 3)
            self.assertGreater(metrics["revenue"], 0)
            self.assertEqual(issues, [])
            self.assertTrue(report_path.exists())
            self.assertTrue(metrics_path.exists())

    def test_filtered_download_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_dir = Path(tmp)
            sqlite_url = f"sqlite:///{(base_dir / 'warehouse' / 'sales.db').resolve().as_posix()}"
            config = default_config(base_dir, database_url=sqlite_url)
            source = _make_temp_source(base_dir)

            cleaned = load_and_clean(config.raw_dir, config.processed_dir, source_url=source.as_uri())
            build_warehouse(config, cleaned)
            metrics = collect_metrics(config.database_url)

            self.assertEqual(metrics["customers"], 3)
            self.assertIsNotNone(metrics["top_category"])


if __name__ == "__main__":
    unittest.main()

