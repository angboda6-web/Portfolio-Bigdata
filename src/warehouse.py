from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import make_url

from .config import PipelineConfig
from .db import create_database_engine
from .public_dataset import DEFAULT_SUPERSTORE_URL, download_public_dataset


@dataclass(frozen=True)
class CleanedData:
    customers: pd.DataFrame
    products: pd.DataFrame
    orders: pd.DataFrame
    order_items: pd.DataFrame


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [
        column.strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")
        for column in normalized.columns
    ]
    rename_map = {
        "rowid": "row_id",
        "orderid": "order_id",
        "orderdate": "order_date",
        "shipdate": "ship_date",
        "shipmode": "ship_mode",
        "customerid": "customer_id",
        "customername": "customer_name",
        "postalcode": "postal_code",
        "productid": "product_id",
        "subcategory": "sub_category",
        "productname": "product_name",
    }
    normalized = normalized.rename(columns={column: rename_map.get(column, column) for column in normalized.columns})
    return normalized


def _clean_superstore(raw_path: Path) -> CleanedData:
    df = pd.read_csv(raw_path, sep="\t")
    df = _normalize_columns(df)

    required_columns = [
        "row_id",
        "order_id",
        "order_date",
        "ship_date",
        "ship_mode",
        "customer_id",
        "customer_name",
        "segment",
        "country",
        "city",
        "state",
        "postal_code",
        "region",
        "product_id",
        "category",
        "sub_category",
        "product_name",
        "sales",
        "quantity",
        "discount",
        "profit",
    ]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise KeyError(missing_columns)

    df = df.dropna(subset=required_columns).copy()
    df["row_id"] = df["row_id"].astype(int)
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")
    df = df.dropna(subset=["order_date", "ship_date"])

    for column in ["customer_id", "customer_name", "segment", "country", "city", "state", "region", "product_id", "category", "sub_category", "product_name", "ship_mode"]:
        df[column] = df[column].astype("string").str.strip()

    df["postal_code"] = df["postal_code"].astype("string").str.strip()
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce").round(4)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce").fillna(0.0).round(4)
    df["profit"] = pd.to_numeric(df["profit"], errors="coerce").round(4)

    df = df.dropna(subset=["sales", "quantity", "profit"])
    df = df[df["sales"] > 0].copy()
    df["quantity"] = df["quantity"].astype(int)
    df["order_date"] = df["order_date"].dt.date.astype("string")
    df["ship_date"] = df["ship_date"].dt.date.astype("string")

    customers = (
        df[
            [
                "customer_id",
                "customer_name",
                "segment",
                "country",
                "city",
                "state",
                "postal_code",
                "region",
            ]
        ]
        .drop_duplicates(subset=["customer_id"])
        .sort_values(["customer_name", "customer_id"])
        .reset_index(drop=True)
    )

    products = (
        df[["product_id", "product_name", "category", "sub_category"]]
        .drop_duplicates(subset=["product_id"])
        .sort_values(["category", "product_name"])
        .reset_index(drop=True)
    )

    orders = (
        df[
            [
                "order_id",
                "order_date",
                "ship_date",
                "ship_mode",
                "customer_id",
                "segment",
                "country",
                "city",
                "state",
                "region",
            ]
        ]
        .groupby(
            [
                "order_id",
                "order_date",
                "ship_date",
                "ship_mode",
                "customer_id",
                "segment",
                "country",
                "city",
                "state",
                "region",
            ],
            as_index=False,
        )
        .size()
        .rename(columns={"size": "item_count"})
    )

    orders_totals = (
        df.groupby("order_id", as_index=False)
        .agg(
            sales_total=("sales", "sum"),
            profit_total=("profit", "sum"),
        )
        .reset_index(drop=True)
    )
    orders_totals["sales_total"] = orders_totals["sales_total"].round(4)
    orders_totals["profit_total"] = orders_totals["profit_total"].round(4)
    orders = orders.merge(orders_totals, on="order_id", how="left")

    order_items = df[
        [
            "row_id",
            "order_id",
            "product_id",
            "sales",
            "quantity",
            "discount",
            "profit",
        ]
    ].copy()
    order_items = order_items.rename(
        columns={
            "row_id": "item_id",
            "sales": "sales_amount",
        }
    )
    order_items["sales_amount"] = order_items["sales_amount"].round(4)
    order_items["discount"] = order_items["discount"].round(4)
    order_items["profit"] = order_items["profit"].round(4)

    return CleanedData(customers=customers, products=products, orders=orders, order_items=order_items)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def load_and_clean(raw_dir: Path, processed_dir: Path, source_url: str | None = None) -> CleanedData:
    raw_path = download_public_dataset(raw_dir, source_url or DEFAULT_SUPERSTORE_URL)
    cleaned = _clean_superstore(raw_path)

    _write_csv(processed_dir / "customers_clean.csv", cleaned.customers)
    _write_csv(processed_dir / "products_clean.csv", cleaned.products)
    _write_csv(processed_dir / "orders_clean.csv", cleaned.orders)
    _write_csv(processed_dir / "order_items_clean.csv", cleaned.order_items)

    return cleaned


def _create_tables(conn) -> None:
    for table in [
        "customer_summary",
        "top_products",
        "daily_sales",
        "fact_payments",
        "fact_order_items",
        "fact_orders",
        "dim_products",
        "dim_customers",
    ]:
        conn.execute(text(f"DROP TABLE IF EXISTS {table}"))

    statements = [
        """
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL,
            segment TEXT NOT NULL,
            country TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            postal_code TEXT NOT NULL,
            region TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            sub_category TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id TEXT PRIMARY KEY,
            order_date TEXT NOT NULL,
            ship_date TEXT NOT NULL,
            ship_mode TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            segment TEXT NOT NULL,
            country TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            region TEXT NOT NULL,
            item_count INTEGER NOT NULL,
            sales_total NUMERIC(14, 4) NOT NULL,
            profit_total NUMERIC(14, 4) NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_order_items (
            item_id INTEGER PRIMARY KEY,
            order_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            sales_amount NUMERIC(14, 4) NOT NULL,
            quantity INTEGER NOT NULL,
            discount NUMERIC(6, 4) NOT NULL,
            profit NUMERIC(14, 4) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
            FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_sales (
            order_date TEXT PRIMARY KEY,
            orders_count INTEGER NOT NULL,
            revenue NUMERIC(14, 4) NOT NULL,
            profit NUMERIC(14, 4) NOT NULL,
            avg_order_value NUMERIC(14, 4) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS top_products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            sub_category TEXT NOT NULL,
            units_sold INTEGER NOT NULL,
            revenue NUMERIC(14, 4) NOT NULL,
            profit NUMERIC(14, 4) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS customer_summary (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            orders_count INTEGER NOT NULL,
            lifetime_value NUMERIC(14, 4) NOT NULL,
            profit NUMERIC(14, 4) NOT NULL
        )
        """,
    ]
    for statement in statements:
        conn.execute(text(statement))


def _replace_table(conn, table: str, df: pd.DataFrame) -> None:
    conn.execute(text(f"DELETE FROM {table}"))
    if df.empty:
        return
    # Use chunked inserts so PostgreSQL does not hit the 65k parameter limit.
    df.to_sql(table, conn, if_exists="append", index=False, chunksize=1000)


def build_warehouse(config: PipelineConfig, cleaned: CleanedData) -> dict[str, int]:
    url = make_url(config.database_url)
    if url.drivername.startswith("sqlite") and url.database and url.database != ":memory:":
        db_path = Path(url.database).expanduser()
        if db_path.exists():
            db_path.unlink()

    engine = create_database_engine(config.database_url)
    try:
        with engine.begin() as conn:
            _create_tables(conn)

            for table in [
                "customer_summary",
                "top_products",
                "daily_sales",
                "fact_order_items",
                "fact_orders",
                "dim_products",
                "dim_customers",
            ]:
                conn.execute(text(f"DELETE FROM {table}"))

            _replace_table(conn, "dim_customers", cleaned.customers)
            _replace_table(conn, "dim_products", cleaned.products)
            _replace_table(conn, "fact_orders", cleaned.orders)
            _replace_table(conn, "fact_order_items", cleaned.order_items)

            conn.execute(
                text(
                    """
                    INSERT INTO daily_sales (order_date, orders_count, revenue, profit, avg_order_value)
                    SELECT
                        order_date,
                        COUNT(*) AS orders_count,
                        SUM(sales_total) AS revenue,
                        SUM(profit_total) AS profit,
                        AVG(sales_total) AS avg_order_value
                    FROM fact_orders
                    GROUP BY order_date
                    ORDER BY order_date
                    """
                )
            )

            conn.execute(
                text(
                    """
                    INSERT INTO top_products (product_id, product_name, category, sub_category, units_sold, revenue, profit)
                    SELECT
                        p.product_id,
                        p.product_name,
                        p.category,
                        p.sub_category,
                        SUM(i.quantity) AS units_sold,
                        SUM(i.sales_amount) AS revenue,
                        SUM(i.profit) AS profit
                    FROM fact_order_items i
                    JOIN dim_products p ON p.product_id = i.product_id
                    GROUP BY p.product_id, p.product_name, p.category, p.sub_category
                    ORDER BY revenue DESC
                    LIMIT 10
                    """
                )
            )

            conn.execute(
                text(
                    """
                    INSERT INTO customer_summary (customer_id, customer_name, city, state, orders_count, lifetime_value, profit)
                    SELECT
                        c.customer_id,
                        c.customer_name,
                        c.city,
                        c.state,
                        COUNT(o.order_id) AS orders_count,
                        COALESCE(SUM(o.sales_total), 0) AS lifetime_value,
                        COALESCE(SUM(o.profit_total), 0) AS profit
                    FROM dim_customers c
                    LEFT JOIN fact_orders o ON o.customer_id = c.customer_id
                    GROUP BY c.customer_id, c.customer_name, c.city, c.state
                    """
                )
            )

            return {
                "customers": int(cleaned.customers.shape[0]),
                "products": int(cleaned.products.shape[0]),
                "orders": int(cleaned.orders.shape[0]),
                "order_items": int(cleaned.order_items.shape[0]),
            }
    finally:
        engine.dispose()


def collect_metrics(database_url: str) -> dict[str, object]:
    engine = create_database_engine(database_url)
    try:
        with engine.connect() as conn:
            orders_count = conn.execute(text("SELECT COUNT(*) FROM fact_orders")).scalar_one()
            total_revenue = conn.execute(text("SELECT COALESCE(SUM(sales_total), 0) FROM fact_orders")).scalar_one()
            total_profit = conn.execute(text("SELECT COALESCE(SUM(profit_total), 0) FROM fact_orders")).scalar_one()
            customers_count = conn.execute(text("SELECT COUNT(*) FROM dim_customers")).scalar_one()
            top_category = conn.execute(
                text(
                    """
                    SELECT p.category, SUM(i.sales_amount) AS revenue
                    FROM fact_order_items i
                    JOIN dim_products p ON p.product_id = i.product_id
                    GROUP BY p.category
                    ORDER BY revenue DESC
                    LIMIT 1
                    """
                )
            ).mappings().first()
            best_day = conn.execute(
                text("SELECT order_date, revenue FROM daily_sales ORDER BY revenue DESC, order_date ASC LIMIT 1")
            ).mappings().first()
            top_state = conn.execute(
                text(
                    """
                    SELECT state, SUM(sales_total) AS revenue
                    FROM fact_orders
                    GROUP BY state
                    ORDER BY revenue DESC
                    LIMIT 1
                    """
                )
            ).mappings().first()

            return {
                "orders": int(orders_count),
                "revenue": float(total_revenue or 0.0),
                "profit": float(total_profit or 0.0),
                "customers": int(customers_count),
                "top_category": {"category": top_category["category"], "revenue": float(top_category["revenue"])} if top_category else None,
                "best_day": {"order_date": best_day["order_date"], "revenue": float(best_day["revenue"])} if best_day else None,
                "top_state": {"state": top_state["state"], "revenue": float(top_state["revenue"])} if top_state else None,
            }
    finally:
        engine.dispose()


def run_data_quality_checks(database_url: str) -> list[str]:
    issues: list[str] = []
    engine = create_database_engine(database_url)
    try:
        with engine.connect() as conn:
            null_customers = conn.execute(text("SELECT COUNT(*) FROM dim_customers WHERE customer_id IS NULL OR city IS NULL")).scalar_one()
            null_products = conn.execute(text("SELECT COUNT(*) FROM dim_products WHERE product_id IS NULL OR category IS NULL")).scalar_one()
            negative_sales = conn.execute(text("SELECT COUNT(*) FROM fact_orders WHERE sales_total <= 0")).scalar_one()
            orphan_items = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM fact_order_items i
                    LEFT JOIN fact_orders o ON o.order_id = i.order_id
                    WHERE o.order_id IS NULL
                    """
                )
            ).scalar_one()
            invalid_discount = conn.execute(
                text("SELECT COUNT(*) FROM fact_order_items WHERE discount < 0 OR discount > 1")
            ).scalar_one()
            order_mismatches = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM (
                        SELECT
                            o.order_id,
                            ABS(COALESCE(o.sales_total, 0) - COALESCE(SUM(i.sales_amount), 0)) AS sales_diff,
                            ABS(COALESCE(o.profit_total, 0) - COALESCE(SUM(i.profit), 0)) AS profit_diff
                        FROM fact_orders o
                        LEFT JOIN fact_order_items i ON i.order_id = o.order_id
                        GROUP BY o.order_id, o.sales_total, o.profit_total
                    ) diffs
                    WHERE sales_diff > 0.01 OR profit_diff > 0.01
                    """
                )
            ).scalar_one()

            if null_customers:
                issues.append("Hay clientes con campos vacios.")
            if null_products:
                issues.append("Hay productos con campos vacios.")
            if negative_sales:
                issues.append("Hay pedidos con ventas no validas.")
            if orphan_items:
                issues.append("Hay lineas de pedido sin pedido padre.")
            if invalid_discount:
                issues.append("Hay descuentos fuera de rango.")
            if order_mismatches:
                issues.append("Hay diferencias entre los totales de pedidos y sus lineas.")
            return issues
    finally:
        engine.dispose()


def persist_metrics(metrics: dict[str, object], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")
