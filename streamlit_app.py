from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text

from src.config import default_config
from src.db import create_database_engine
from src.warehouse import collect_metrics


def _read_frame(engine, query: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)


st.set_page_config(page_title="Retail ETL Dashboard", layout="wide")

st.title("Retail ETL Dashboard")
st.caption("Interactive view of the current warehouse. Works with SQLite or PostgreSQL through DATABASE_URL.")

config = default_config()
database_url = st.sidebar.text_input("Database URL", value=config.database_url)
top_n = st.sidebar.slider("Top products shown", min_value=3, max_value=10, value=5)

if not database_url:
    st.stop()

try:
    engine = create_database_engine(database_url)
    metrics = collect_metrics(database_url)
except Exception as exc:  # pragma: no cover - Streamlit runtime feedback
    st.error(f"Unable to open database: {exc}")
    st.stop()

st.sidebar.markdown("### Quick summary")
st.sidebar.metric("Completed orders", metrics["completed_orders"])
st.sidebar.metric("Revenue", f"{metrics['revenue']:.2f}")
st.sidebar.metric("Customers", metrics["customers"])

col1, col2, col3 = st.columns(3)
col1.metric("Completed orders", metrics["completed_orders"])
col2.metric("Revenue", f"{metrics['revenue']:.2f}")
col3.metric("Customers", metrics["customers"])

if metrics.get("top_category"):
    st.info(
        f"Top category: {metrics['top_category']['category']} with revenue {metrics['top_category']['revenue']:.2f}"
    )

daily_sales = _read_frame(
    engine,
    """
    SELECT order_date, orders_count, revenue, avg_order_value
    FROM daily_sales
    ORDER BY order_date ASC
    """,
)

top_products = _read_frame(
    engine,
    f"""
    SELECT product_name, category, units_sold, revenue
    FROM top_products
    ORDER BY revenue DESC
    LIMIT {top_n}
    """,
)

customer_summary = _read_frame(
    engine,
    """
    SELECT customer_name, city, orders_count, lifetime_value
    FROM customer_summary
    ORDER BY lifetime_value DESC
    LIMIT 10
    """,
)

if daily_sales.empty:
    st.warning("No daily sales data found yet. Run the pipeline first.")
else:
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

st.subheader("What this project demonstrates")
st.markdown(
    """
- Batch ETL pipeline
- Data cleaning and validation
- SQL warehouse modeling
- Reproducible reporting
- Optional PostgreSQL support
"""
)

