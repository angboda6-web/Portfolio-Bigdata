from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st
from sqlalchemy import bindparam, text

from src.config import default_config
from src.db import create_database_engine


def _read_frame(engine, query: str, params: dict[str, object] | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn, params=params or {})


def _load_bounds(engine) -> tuple[date | None, date | None]:
    bounds = _read_frame(
        engine,
        """
        SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date
        FROM fact_orders
        WHERE status = 'completed'
        """,
    )
    if bounds.empty or bounds.iloc[0]["min_date"] is None:
        return None, None
    return pd.to_datetime(bounds.iloc[0]["min_date"]).date(), pd.to_datetime(bounds.iloc[0]["max_date"]).date()


def _load_choices(engine) -> tuple[list[str], list[str]]:
    cities = _read_frame(
        engine,
        """
        SELECT DISTINCT city
        FROM dim_customers
        ORDER BY city
        """,
    )["city"].dropna().tolist()
    categories = _read_frame(
        engine,
        """
        SELECT DISTINCT category
        FROM dim_products
        ORDER BY category
        """,
    )["category"].dropna().tolist()
    return cities, categories


def _load_filtered_data(
    engine,
    start_date: date,
    end_date: date,
    cities: list[str],
    categories: list[str],
) -> pd.DataFrame:
    where_clauses = [
        "o.status = 'completed'",
        "o.order_date BETWEEN :start_date AND :end_date",
    ]
    params: dict[str, object] = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    if cities:
        where_clauses.append("c.city IN :cities")
        params["cities"] = tuple(cities)

    if categories:
        where_clauses.append("p.category IN :categories")
        params["categories"] = tuple(categories)

    query = f"""
        SELECT
            o.order_id,
            o.order_date,
            c.customer_id,
            c.customer_name,
            c.city,
            p.product_id,
            p.product_name,
            p.category,
            i.quantity,
            i.line_total
        FROM fact_orders o
        JOIN dim_customers c ON c.customer_id = o.customer_id
        JOIN fact_order_items i ON i.order_id = o.order_id
        JOIN dim_products p ON p.product_id = i.product_id
        WHERE {" AND ".join(where_clauses)}
        ORDER BY o.order_date, o.order_id
    """
    stmt = text(query)
    if cities:
        stmt = stmt.bindparams(bindparam("cities", expanding=True))
    if categories:
        stmt = stmt.bindparams(bindparam("categories", expanding=True))

    with engine.connect() as conn:
        return pd.read_sql_query(stmt, conn, params=params)


def _aggregate_sales(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["order_date", "orders_count", "revenue", "avg_order_value"])
    grouped = (
        df.groupby("order_date", as_index=False)
        .agg(orders_count=("order_id", "nunique"), revenue=("line_total", "sum"))
        .sort_values("order_date")
    )
    grouped["avg_order_value"] = grouped["revenue"] / grouped["orders_count"]
    return grouped


def _aggregate_top_products(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["product_name", "category", "units_sold", "revenue"])
    grouped = (
        df.groupby(["product_name", "category"], as_index=False)
        .agg(units_sold=("quantity", "sum"), revenue=("line_total", "sum"))
        .sort_values("revenue", ascending=False)
    )
    return grouped


def _aggregate_customers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["customer_name", "city", "orders_count", "lifetime_value"])
    grouped = (
        df.groupby(["customer_name", "city"], as_index=False)
        .agg(orders_count=("order_id", "nunique"), lifetime_value=("line_total", "sum"))
        .sort_values("lifetime_value", ascending=False)
    )
    return grouped


def _format_money(value: float) -> str:
    return f"{value:,.2f}"


st.set_page_config(page_title="Retail ETL Dashboard", layout="wide")

st.title("Retail ETL Dashboard")
st.caption("Interactive view of the warehouse with filters by date, category and city.")

config = default_config()
database_url = st.sidebar.text_input("Database URL", value=config.database_url)

if not database_url:
    st.stop()

try:
    engine = create_database_engine(database_url)
except Exception as exc:  # pragma: no cover - Streamlit runtime feedback
    st.error(f"Unable to open database: {exc}")
    st.stop()

start_bound, end_bound = _load_bounds(engine)
if start_bound is None or end_bound is None:
    st.warning("No completed orders found yet. Run the pipeline first.")
    st.stop()

cities, categories = _load_choices(engine)

with st.sidebar:
    st.markdown("### Filters")
    date_range = st.date_input("Date range", value=(start_bound, end_bound), min_value=start_bound, max_value=end_bound)
    selected_cities = st.multiselect("City", options=cities, default=cities)
    selected_categories = st.multiselect("Category", options=categories, default=categories)
    top_n = st.slider("Top products shown", min_value=3, max_value=10, value=5)
    st.markdown("### Quick summary")

if isinstance(date_range, tuple):
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

filtered = _load_filtered_data(
    engine,
    start_date=start_date,
    end_date=end_date,
    cities=selected_cities,
    categories=selected_categories,
)

if filtered.empty:
    st.warning("No data matches the selected filters.")
    st.stop()

daily_sales = _aggregate_sales(filtered)
top_products = _aggregate_top_products(filtered).head(top_n)
customer_summary = _aggregate_customers(filtered).head(10)

completed_orders = int(filtered["order_id"].nunique())
revenue = float(filtered["line_total"].sum())
customer_count = int(filtered["customer_id"].nunique())
top_category = (
    filtered.groupby("category", as_index=False)["line_total"].sum().sort_values("line_total", ascending=False).head(1)
)
best_day = (
    daily_sales.sort_values("revenue", ascending=False).head(1) if not daily_sales.empty else pd.DataFrame()
)

st.sidebar.metric("Completed orders", completed_orders)
st.sidebar.metric("Revenue", _format_money(revenue))
st.sidebar.metric("Customers", customer_count)

col1, col2, col3 = st.columns(3)
col1.metric("Completed orders", completed_orders)
col2.metric("Revenue", _format_money(revenue))
col3.metric("Customers", customer_count)

if not top_category.empty:
    st.info(
        f"Top category in the current selection: {top_category.iloc[0]['category']} "
        f"with revenue {_format_money(float(top_category.iloc[0]['line_total']))}"
    )

if not daily_sales.empty:
    st.subheader("Daily revenue")
    st.line_chart(daily_sales.set_index("order_date")["revenue"])

    st.subheader("Orders per day")
    st.bar_chart(daily_sales.set_index("order_date")["orders_count"])

if not top_products.empty:
    st.subheader("Top products")
    st.dataframe(top_products, use_container_width=True)
    st.bar_chart(top_products.set_index("product_name")["revenue"])

if not customer_summary.empty:
    st.subheader("Top customers")
    st.dataframe(customer_summary, use_container_width=True)

if not best_day.empty:
    st.success(
        f"Best day in the current selection: {best_day.iloc[0]['order_date']} "
        f"with revenue {_format_money(float(best_day.iloc[0]['revenue']))}"
    )

st.subheader("What this project demonstrates")
st.markdown(
    """
- Batch ETL pipeline
- Data cleaning and validation
- SQL warehouse modeling
- Reproducible reporting
- Optional PostgreSQL support
- Interactive filtering in Streamlit
"""
)
