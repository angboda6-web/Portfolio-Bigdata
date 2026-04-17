from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from contextlib import closing
from pathlib import Path

from .config import PipelineConfig


@dataclass(frozen=True)
class CleanedData:
    customers: list[dict[str, object]]
    products: list[dict[str, object]]
    orders: list[dict[str, object]]
    order_items: list[dict[str, object]]
    payments: list[dict[str, object]]


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _parse_date(value: str) -> str | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def clean_customers(raw_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    cleaned: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in raw_rows:
        customer_id = row.get("customer_id", "").strip()
        name = row.get("customer_name", "").strip().title()
        email = row.get("email", "").strip().lower()
        city = row.get("city", "").strip().title()
        signup_date = _parse_date(row.get("signup_date", "").strip())
        if not customer_id or customer_id in seen:
            continue
        if "@" not in email or not signup_date:
            continue
        seen.add(customer_id)
        cleaned.append(
            {
                "customer_id": customer_id,
                "customer_name": name,
                "email": email,
                "city": city,
                "signup_date": signup_date,
            }
        )
    return cleaned


def clean_products(raw_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    cleaned: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in raw_rows:
        product_id = row.get("product_id", "").strip()
        if not product_id or product_id in seen:
            continue
        try:
            unit_price = round(float(row.get("unit_price", "0")), 2)
        except ValueError:
            continue
        if unit_price <= 0:
            continue
        seen.add(product_id)
        cleaned.append(
            {
                "product_id": product_id,
                "product_name": row.get("product_name", "").strip().title(),
                "category": row.get("category", "").strip().title(),
                "unit_price": unit_price,
            }
        )
    return cleaned


def clean_orders(
    raw_rows: list[dict[str, str]],
    valid_customers: set[str],
) -> list[dict[str, object]]:
    cleaned: list[dict[str, object]] = []
    seen: set[str] = set()
    allowed_status = {"completed", "cancelled", "returned"}
    for row in raw_rows:
        order_id = row.get("order_id", "").strip()
        customer_id = row.get("customer_id", "").strip()
        order_date = _parse_date(row.get("order_date", "").strip())
        status = row.get("status", "").strip().lower()
        payment_method = row.get("payment_method", "").strip().lower()
        if not order_id or order_id in seen:
            continue
        if customer_id not in valid_customers or not order_date:
            continue
        if status not in allowed_status:
            continue
        seen.add(order_id)
        cleaned.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date,
                "status": status,
                "payment_method": payment_method,
            }
        )
    return cleaned


def clean_order_items(
    raw_rows: list[dict[str, str]],
    valid_orders: set[str],
    valid_products: set[str],
) -> list[dict[str, object]]:
    cleaned: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in raw_rows:
        item_id = row.get("item_id", "").strip()
        order_id = row.get("order_id", "").strip()
        product_id = row.get("product_id", "").strip()
        if not item_id or item_id in seen:
            continue
        if order_id not in valid_orders or product_id not in valid_products:
            continue
        try:
            quantity = int(row.get("quantity", "0"))
            unit_price = round(float(row.get("unit_price", "0")), 2)
            line_total = round(float(row.get("line_total", "0")), 2)
        except ValueError:
            continue
        if quantity <= 0 or unit_price <= 0 or line_total <= 0:
            continue
        seen.add(item_id)
        cleaned.append(
            {
                "item_id": item_id,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": line_total,
            }
        )
    return cleaned


def clean_payments(
    raw_rows: list[dict[str, str]],
    valid_completed_orders: set[str],
) -> list[dict[str, object]]:
    cleaned: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in raw_rows:
        payment_id = row.get("payment_id", "").strip()
        order_id = row.get("order_id", "").strip()
        payment_date = _parse_date(row.get("payment_date", "").strip())
        payment_method = row.get("payment_method", "").strip().lower()
        payment_status = row.get("payment_status", "").strip().lower()
        if not payment_id or payment_id in seen:
            continue
        if order_id not in valid_completed_orders or not payment_date:
            continue
        try:
            amount = round(float(row.get("amount", "0")), 2)
        except ValueError:
            continue
        if amount <= 0 or payment_status != "paid":
            continue
        seen.add(payment_id)
        cleaned.append(
            {
                "payment_id": payment_id,
                "order_id": order_id,
                "payment_date": payment_date,
                "payment_method": payment_method,
                "payment_status": payment_status,
                "amount": amount,
            }
        )
    return cleaned


def load_and_clean(raw_dir: Path, processed_dir: Path) -> CleanedData:
    raw_customers = _read_csv(raw_dir / "customers.csv")
    raw_products = _read_csv(raw_dir / "products.csv")
    raw_orders = _read_csv(raw_dir / "orders.csv")
    raw_items = _read_csv(raw_dir / "order_items.csv")
    raw_payments = _read_csv(raw_dir / "payments.csv")

    customers = clean_customers(raw_customers)
    products = clean_products(raw_products)
    valid_customers = {row["customer_id"] for row in customers}
    valid_products = {row["product_id"] for row in products}
    orders = clean_orders(raw_orders, valid_customers)
    valid_orders = {row["order_id"] for row in orders}
    order_items = clean_order_items(raw_items, valid_orders, valid_products)
    completed_orders = {row["order_id"] for row in orders if row["status"] == "completed"}
    payments = clean_payments(raw_payments, completed_orders)

    _write_csv(processed_dir / "customers_clean.csv", ["customer_id", "customer_name", "email", "city", "signup_date"], customers)
    _write_csv(processed_dir / "products_clean.csv", ["product_id", "product_name", "category", "unit_price"], products)
    _write_csv(processed_dir / "orders_clean.csv", ["order_id", "customer_id", "order_date", "status", "payment_method"], orders)
    _write_csv(
        processed_dir / "order_items_clean.csv",
        ["item_id", "order_id", "product_id", "quantity", "unit_price", "line_total"],
        order_items,
    )
    _write_csv(
        processed_dir / "payments_clean.csv",
        ["payment_id", "order_id", "payment_date", "payment_method", "payment_status", "amount"],
        payments,
    )

    return CleanedData(
        customers=customers,
        products=products,
        orders=orders,
        order_items=order_items,
        payments=payments,
    )


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _create_tables(conn: sqlite3.Connection) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email TEXT NOT NULL,
            city TEXT NOT NULL,
            signup_date TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            unit_price REAL NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            total_amount REAL NOT NULL,
            item_count INTEGER NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_order_items (
            item_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            line_total REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
            FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_payments (
            payment_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            payment_date TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            payment_status TEXT NOT NULL,
            amount REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_sales (
            order_date TEXT PRIMARY KEY,
            orders_count INTEGER NOT NULL,
            revenue REAL NOT NULL,
            avg_order_value REAL NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS top_products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            units_sold INTEGER NOT NULL,
            revenue REAL NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS customer_summary (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL,
            city TEXT NOT NULL,
            orders_count INTEGER NOT NULL,
            lifetime_value REAL NOT NULL
        )
        """,
    ]
    for statement in statements:
        conn.execute(statement)


def _replace_table(conn: sqlite3.Connection, table: str, rows: list[dict[str, object]]) -> None:
    conn.execute(f"DELETE FROM {table}")
    if not rows:
        return
    columns = list(rows[0].keys())
    placeholders = ",".join(["?"] * len(columns))
    column_sql = ",".join(columns)
    conn.executemany(
        f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})",
        [[row[column] for column in columns] for row in rows],
    )


def build_warehouse(config: PipelineConfig, cleaned: CleanedData) -> dict[str, int]:
    with closing(_connect(config.db_path)) as conn:
        _create_tables(conn)

        _replace_table(conn, "dim_customers", cleaned.customers)
        _replace_table(conn, "dim_products", cleaned.products)

        order_item_counts = Counter(item["order_id"] for item in cleaned.order_items)
        order_totals = Counter()
        for item in cleaned.order_items:
            order_totals[item["order_id"]] += float(item["line_total"])

        fact_orders = []
        for order in cleaned.orders:
            fact_orders.append(
                {
                    "order_id": order["order_id"],
                    "customer_id": order["customer_id"],
                    "order_date": order["order_date"],
                    "status": order["status"],
                    "payment_method": order["payment_method"],
                    "total_amount": round(order_totals.get(order["order_id"], 0.0), 2),
                    "item_count": int(order_item_counts.get(order["order_id"], 0)),
                }
            )
        _replace_table(conn, "fact_orders", fact_orders)
        _replace_table(conn, "fact_order_items", cleaned.order_items)
        _replace_table(conn, "fact_payments", cleaned.payments)

        conn.execute("DELETE FROM daily_sales")
        conn.execute(
            """
            INSERT INTO daily_sales (order_date, orders_count, revenue, avg_order_value)
            SELECT
                order_date,
                COUNT(*) AS orders_count,
                ROUND(SUM(total_amount), 2) AS revenue,
                ROUND(AVG(total_amount), 2) AS avg_order_value
            FROM fact_orders
            WHERE status = 'completed'
            GROUP BY order_date
            ORDER BY order_date
            """
        )

        conn.execute("DELETE FROM top_products")
        conn.execute(
            """
            INSERT INTO top_products (product_id, product_name, category, units_sold, revenue)
            SELECT
                p.product_id,
                p.product_name,
                p.category,
                SUM(i.quantity) AS units_sold,
                ROUND(SUM(i.line_total), 2) AS revenue
            FROM fact_order_items i
            JOIN dim_products p ON p.product_id = i.product_id
            GROUP BY p.product_id, p.product_name, p.category
            ORDER BY revenue DESC
            LIMIT 10
            """
        )

        conn.execute("DELETE FROM customer_summary")
        conn.execute(
            """
            INSERT INTO customer_summary (customer_id, customer_name, city, orders_count, lifetime_value)
            SELECT
                c.customer_id,
                c.customer_name,
                c.city,
                COUNT(o.order_id) AS orders_count,
                ROUND(COALESCE(SUM(o.total_amount), 0), 2) AS lifetime_value
            FROM dim_customers c
            LEFT JOIN fact_orders o ON o.customer_id = c.customer_id AND o.status = 'completed'
            GROUP BY c.customer_id, c.customer_name, c.city
            """
        )

        conn.commit()

        return {
            "customers": len(cleaned.customers),
            "products": len(cleaned.products),
            "orders": len(cleaned.orders),
            "order_items": len(cleaned.order_items),
            "payments": len(cleaned.payments),
        }


def collect_metrics(db_path: Path) -> dict[str, object]:
    with closing(_connect(db_path)) as conn:
        orders_count = conn.execute("SELECT COUNT(*) FROM fact_orders WHERE status = 'completed'").fetchone()[0]
        total_revenue = conn.execute(
            "SELECT COALESCE(ROUND(SUM(total_amount), 2), 0) FROM fact_orders WHERE status = 'completed'"
        ).fetchone()[0]
        customers_count = conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
        top_category = conn.execute(
            """
            SELECT p.category, ROUND(SUM(i.line_total), 2) AS revenue
            FROM fact_order_items i
            JOIN dim_products p ON p.product_id = i.product_id
            GROUP BY p.category
            ORDER BY revenue DESC
            LIMIT 1
            """
        ).fetchone()
        daily_peak = conn.execute(
            "SELECT order_date, revenue FROM daily_sales ORDER BY revenue DESC, order_date ASC LIMIT 1"
        ).fetchone()

        return {
            "completed_orders": int(orders_count),
            "revenue": float(total_revenue),
            "customers": int(customers_count),
            "top_category": dict(top_category) if top_category else None,
            "best_day": dict(daily_peak) if daily_peak else None,
        }


def run_data_quality_checks(db_path: Path) -> list[str]:
    issues: list[str] = []
    with closing(_connect(db_path)) as conn:
        null_customer_emails = conn.execute(
            "SELECT COUNT(*) FROM dim_customers WHERE email IS NULL OR email = ''"
        ).fetchone()[0]
        negative_totals = conn.execute("SELECT COUNT(*) FROM fact_orders WHERE total_amount < 0").fetchone()[0]
        empty_order_items = conn.execute(
            "SELECT COUNT(*) FROM fact_order_items WHERE quantity <= 0 OR line_total <= 0"
        ).fetchone()[0]
        orphan_payments = conn.execute(
            """
            SELECT COUNT(*)
            FROM fact_payments p
            LEFT JOIN fact_orders o ON o.order_id = p.order_id
            WHERE o.order_id IS NULL
            """
        ).fetchone()[0]
        if null_customer_emails:
            issues.append("Hay clientes con email vacío.")
        if negative_totals:
            issues.append("Hay pedidos con importe negativo.")
        if empty_order_items:
            issues.append("Hay líneas de pedido inválidas.")
        if orphan_payments:
            issues.append("Hay pagos sin pedido asociado.")
    return issues


def persist_metrics(metrics: dict[str, object], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")
