from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import bindparam, text

from src.config import default_config
from src.db import create_database_engine


@dataclass(frozen=True)
class Selection:
    start_date: date
    end_date: date
    cities: tuple[str, ...]
    categories: tuple[str, ...]
    segments: tuple[str, ...]
    regions: tuple[str, ...]
    ship_modes: tuple[str, ...]
    top_n: int
    brand_mode: str
    compare_previous: bool


def _format_money(value: float) -> str:
    return f"${value:,.2f}"


def _format_percent(value: float) -> str:
    return f"{value:.1f}%"


def _read_frame(database_url: str, query: str, params: dict[str, object] | None = None) -> pd.DataFrame:
    engine = create_database_engine(database_url)
    try:
        with engine.connect() as conn:
            return pd.read_sql_query(text(query), conn, params=params or {})
    finally:
        engine.dispose()


@st.cache_data(ttl=300, show_spinner=False)
def load_metadata(database_url: str) -> dict[str, object]:
    bounds = _read_frame(
        database_url,
        """
        SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date
        FROM fact_orders
        """,
    )
    if bounds.empty or bounds.iloc[0]["min_date"] is None:
        return {"bounds": None, "choices": {}}

    choices = {
        "cities": _read_frame(database_url, "SELECT DISTINCT city FROM fact_orders ORDER BY city")["city"].dropna().tolist(),
        "categories": _read_frame(database_url, "SELECT DISTINCT category FROM dim_products ORDER BY category")["category"].dropna().tolist(),
        "segments": _read_frame(database_url, "SELECT DISTINCT segment FROM fact_orders ORDER BY segment")["segment"].dropna().tolist(),
        "regions": _read_frame(database_url, "SELECT DISTINCT region FROM fact_orders ORDER BY region")["region"].dropna().tolist(),
        "ship_modes": _read_frame(database_url, "SELECT DISTINCT ship_mode FROM fact_orders ORDER BY ship_mode")["ship_mode"].dropna().tolist(),
    }
    return {
        "bounds": (
            pd.to_datetime(bounds.iloc[0]["min_date"]).date(),
            pd.to_datetime(bounds.iloc[0]["max_date"]).date(),
        ),
        "choices": choices,
    }


def _build_filters(database_url: str, selection: Selection) -> pd.DataFrame:
    where_clauses = ["o.order_date BETWEEN :start_date AND :end_date"]
    params: dict[str, object] = {
        "start_date": selection.start_date.isoformat(),
        "end_date": selection.end_date.isoformat(),
    }

    optional_filters: list[tuple[str, str, tuple[str, ...]]] = [
        ("o.city", "cities", selection.cities),
        ("p.category", "categories", selection.categories),
        ("o.segment", "segments", selection.segments),
        ("o.region", "regions", selection.regions),
        ("o.ship_mode", "ship_modes", selection.ship_modes),
    ]

    for column, key, values in optional_filters:
        if values:
            where_clauses.append(f"{column} IN :{key}")
            params[key] = values

    query = f"""
        SELECT
            o.order_id,
            o.order_date,
            o.ship_mode,
            o.segment,
            o.region,
            o.city,
            o.state,
            o.customer_id,
            c.customer_name,
            p.category,
            p.sub_category,
            p.product_id,
            p.product_name,
            i.sales_amount,
            i.quantity,
            i.discount,
            i.profit
        FROM fact_orders o
        JOIN dim_customers c ON c.customer_id = o.customer_id
        JOIN fact_order_items i ON i.order_id = o.order_id
        JOIN dim_products p ON p.product_id = i.product_id
        WHERE {" AND ".join(where_clauses)}
        ORDER BY o.order_date, o.order_id, i.item_id
    """

    stmt = text(query)
    for key in ["cities", "categories", "segments", "regions", "ship_modes"]:
        if key in params:
            stmt = stmt.bindparams(bindparam(key, expanding=True))

    engine = create_database_engine(database_url)
    try:
        with engine.connect() as conn:
            return pd.read_sql_query(stmt, conn, params=params)
    finally:
        engine.dispose()


@st.cache_data(ttl=300, show_spinner=False)
def load_filtered_data(database_url: str, selection: Selection) -> pd.DataFrame:
    return _build_filters(database_url, selection)


def _build_previous_selection(selection: Selection) -> Selection | None:
    period_days = (selection.end_date - selection.start_date).days + 1
    if period_days <= 0:
        return None
    previous_end = selection.start_date - timedelta(days=1)
    previous_start = previous_end - timedelta(days=period_days - 1)
    return Selection(
        start_date=previous_start,
        end_date=previous_end,
        cities=selection.cities,
        categories=selection.categories,
        segments=selection.segments,
        regions=selection.regions,
        ship_modes=selection.ship_modes,
        top_n=selection.top_n,
        brand_mode=selection.brand_mode,
        compare_previous=selection.compare_previous,
    )


def _aggregate(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        empty = pd.DataFrame()
        return {
            "orders": 0,
            "customers": 0,
            "items": 0,
            "revenue": 0.0,
            "profit": 0.0,
            "margin": 0.0,
            "aov": 0.0,
            "avg_discount": 0.0,
            "daily": empty,
            "products": empty,
            "customers_df": empty,
            "segments": empty,
            "regions": empty,
            "ship_modes": empty,
            "categories": empty,
            "states": empty,
            "top_category": None,
            "best_day": None,
            "top_state": None,
        }

    daily = (
        df.groupby("order_date", as_index=False)
        .agg(
            orders=("order_id", "nunique"),
            revenue=("sales_amount", "sum"),
            profit=("profit", "sum"),
        )
        .sort_values("order_date")
    )
    daily["aov"] = daily["revenue"] / daily["orders"]

    products = (
        df.groupby(["product_name", "category", "sub_category"], as_index=False)
        .agg(
            units_sold=("quantity", "sum"),
            revenue=("sales_amount", "sum"),
            profit=("profit", "sum"),
            avg_discount=("discount", "mean"),
        )
        .sort_values("revenue", ascending=False)
    )

    customers = (
        df.groupby(["customer_id", "customer_name", "city", "state"], as_index=False)
        .agg(
            orders=("order_id", "nunique"),
            revenue=("sales_amount", "sum"),
            profit=("profit", "sum"),
        )
        .sort_values("revenue", ascending=False)
    )

    segments = df.groupby("segment", as_index=False).agg(revenue=("sales_amount", "sum"), profit=("profit", "sum")).sort_values("revenue", ascending=False)
    regions = df.groupby("region", as_index=False).agg(revenue=("sales_amount", "sum"), profit=("profit", "sum")).sort_values("revenue", ascending=False)
    ship_modes = df.groupby("ship_mode", as_index=False).agg(revenue=("sales_amount", "sum"), orders=("order_id", "nunique")).sort_values("revenue", ascending=False)
    categories = df.groupby("category", as_index=False).agg(revenue=("sales_amount", "sum"), profit=("profit", "sum"), orders=("order_id", "nunique")).sort_values("revenue", ascending=False)
    states = df.groupby("state", as_index=False).agg(revenue=("sales_amount", "sum"), profit=("profit", "sum"), orders=("order_id", "nunique")).sort_values("revenue", ascending=False)

    top_category = categories.head(1).iloc[0].to_dict() if not categories.empty else None
    best_day = daily.sort_values(["revenue", "order_date"], ascending=[False, True]).head(1).iloc[0].to_dict() if not daily.empty else None
    top_state = states.head(1).iloc[0].to_dict() if not states.empty else None

    revenue = float(df["sales_amount"].sum())
    profit = float(df["profit"].sum())
    orders = int(df["order_id"].nunique())
    customers_count = int(df["customer_id"].nunique())
    items = int(df["quantity"].sum())
    avg_discount = float(df["discount"].mean())
    margin = (profit / revenue * 100.0) if revenue else 0.0
    aov = (revenue / orders) if orders else 0.0

    return {
        "orders": orders,
        "customers": customers_count,
        "items": items,
        "revenue": revenue,
        "profit": profit,
        "margin": margin,
        "aov": aov,
        "avg_discount": avg_discount,
        "daily": daily,
        "products": products,
        "customers_df": customers,
        "segments": segments,
        "regions": regions,
        "ship_modes": ship_modes,
        "categories": categories,
        "states": states,
        "top_category": top_category,
        "best_day": best_day,
        "top_state": top_state,
    }


def _percent_change(current: float, previous: float | int | None) -> str | None:
    if previous in (None, 0):
        return None
    return f"{((current - float(previous)) / float(previous)) * 100:.1f}%"


def _brand_css(mode: str) -> str:
    if mode == "Amazon-style":
        accent = "#ff9900"
        bg = "#0f172a"
        panel = "#111827"
        text = "#e5e7eb"
    elif mode == "Executive":
        accent = "#4f8cff"
        bg = "#0b1220"
        panel = "#111827"
        text = "#e5e7eb"
    else:
        accent = "#0ea5e9"
        bg = "#f8fafc"
        panel = "#ffffff"
        text = "#0f172a"

    return f"""
    <style>
        .stApp {{
            background: linear-gradient(180deg, {bg} 0%, #0f172a 100%);
            color: {text};
        }}
        .hero {{
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 20px;
            padding: 24px 28px;
            margin-bottom: 18px;
            background: linear-gradient(135deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
            box-shadow: 0 18px 40px rgba(0,0,0,0.18);
        }}
        .hero h1 {{
            margin: 0 0 6px 0;
            color: white;
            font-size: 2.1rem;
        }}
        .hero p {{
            margin: 0;
            color: rgba(255,255,255,0.78);
            font-size: 0.98rem;
        }}
        .section-title {{
            margin-top: 1rem;
            font-weight: 700;
            color: white;
        }}
        .insight {{
            border-left: 4px solid {accent};
            padding: 0.65rem 0.9rem;
            background: rgba(255,255,255,0.05);
            border-radius: 10px;
            color: white;
        }}
        .stMetric {{
            background: {panel};
            border-radius: 16px;
            padding: 14px 14px 8px 14px;
            border: 1px solid rgba(255,255,255,0.08);
        }}
    </style>
    """


def _render_kpi_cards(current: dict[str, object], previous: dict[str, object] | None) -> None:
    rows = [
        ("Revenue", _format_money(float(current["revenue"])), _percent_change(float(current["revenue"]), None if previous is None else float(previous["revenue"]))),
        ("Profit", _format_money(float(current["profit"])), _percent_change(float(current["profit"]), None if previous is None else float(previous["profit"]))),
        ("Orders", f"{int(current['orders']):,}", _percent_change(float(current["orders"]), None if previous is None else float(previous["orders"]))),
        ("Customers", f"{int(current['customers']):,}", _percent_change(float(current["customers"]), None if previous is None else float(previous["customers"]))),
        ("Margin", _format_percent(float(current["margin"])), _percent_change(float(current["margin"]), None if previous is None else float(previous["margin"]))),
        ("AOV", _format_money(float(current["aov"])), _percent_change(float(current["aov"]), None if previous is None else float(previous["aov"]))),
    ]
    cols = st.columns(6)
    for idx, (label, value, delta) in enumerate(rows):
        cols[idx].metric(label, value, delta)


def _render_executive(current: dict[str, object], previous: dict[str, object] | None) -> None:
    st.markdown("### Executive overview")
    left, right = st.columns([2, 1])
    with left:
        st.markdown("#### Trend")
        if not current["daily"].empty:
            daily = current["daily"].set_index("order_date")[["revenue", "profit"]]
            st.line_chart(daily)
        else:
            st.info("No data for the selected period.")
    with right:
        st.markdown("#### Operating snapshot")
        insight_lines = []
        if current["top_category"]:
            insight_lines.append(f"Top category: {current['top_category']['category']} ({_format_money(float(current['top_category']['revenue']))})")
        if current["best_day"]:
            insight_lines.append(f"Best day: {current['best_day']['order_date']} ({_format_money(float(current['best_day']['revenue']))})")
        if current["top_state"]:
            insight_lines.append(f"Top state: {current['top_state']['state']} ({_format_money(float(current['top_state']['revenue']))})")
        if not current["segments"].empty:
            top_segment = current["segments"].head(1).iloc[0]
            insight_lines.append(f"Top segment: {top_segment['segment']} ({_format_money(float(top_segment['revenue']))})")
        st.markdown(
            "<div class='insight'>" + "<br>".join(insight_lines or ["No insights available"]) + "</div>",
            unsafe_allow_html=True,
        )

    left, right = st.columns([1, 1])
    with left:
        st.markdown("#### Revenue by region")
        if not current["regions"].empty:
            st.bar_chart(current["regions"].set_index("region")[["revenue"]])
    with right:
        st.markdown("#### Ship mode mix")
        if not current["ship_modes"].empty:
            st.bar_chart(current["ship_modes"].set_index("ship_mode")[["revenue"]])


def _render_commercial(current: dict[str, object], top_n: int) -> None:
    st.markdown("### Catalog and demand")
    left, right = st.columns([1, 1])
    with left:
        st.markdown("#### Top products")
        top_products = current["products"].head(top_n)
        st.dataframe(
            top_products[["product_name", "category", "sub_category", "units_sold", "revenue", "profit", "avg_discount"]],
            use_container_width=True,
            hide_index=True,
        )
    with right:
        st.markdown("#### Revenue by category")
        if not current["categories"].empty:
            st.bar_chart(current["categories"].set_index("category")[["revenue"]])

    left, right = st.columns([1, 1])
    with left:
        st.markdown("#### Products by revenue")
        if not top_products.empty:
            st.bar_chart(top_products.set_index("product_name")[["revenue"]])
    with right:
        st.markdown("#### Discount profile")
        if not current["products"].empty:
            discount_view = current["products"].head(top_n).set_index("product_name")[["avg_discount"]]
            st.bar_chart(discount_view)


def _render_customers(current: dict[str, object]) -> None:
    st.markdown("### Customers")
    left, right = st.columns([1, 1])
    with left:
        st.markdown("#### Top customers")
        top_customers = current["customers_df"].head(10)
        st.dataframe(
            top_customers[["customer_name", "city", "state", "orders", "revenue", "profit"]],
            use_container_width=True,
            hide_index=True,
        )
    with right:
        st.markdown("#### Revenue by segment")
        if not current["segments"].empty:
            st.bar_chart(current["segments"].set_index("segment")[["revenue"]])

    left, right = st.columns([1, 1])
    with left:
        st.markdown("#### Customer concentration")
        if not current["customers_df"].empty:
            concentration = current["customers_df"].head(10)["revenue"].sum() / current["revenue"] * 100 if current["revenue"] else 0
            st.metric("Top 10 customer share", _format_percent(concentration))
    with right:
        st.markdown("#### Profit by segment")
        if not current["segments"].empty:
            st.bar_chart(current["segments"].set_index("segment")[["profit"]])


def _render_geography(current: dict[str, object]) -> None:
    st.markdown("### Geography")
    left, right = st.columns([1, 1])
    with left:
        st.markdown("#### Revenue by state")
        if not current["states"].empty:
            st.bar_chart(current["states"].head(10).set_index("state")[["revenue"]])
    with right:
        st.markdown("#### Orders by state")
        if not current["states"].empty:
            st.bar_chart(current["states"].head(10).set_index("state")[["orders"]])

    st.markdown("#### State leaderboard")
    if not current["states"].empty:
        st.dataframe(current["states"].head(15)[["state", "orders", "revenue", "profit"]], use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Retail Command Center", layout="wide")

    config = default_config()
    metadata = load_metadata(config.database_url)
    bounds = metadata["bounds"]
    choices = metadata["choices"]

    st.markdown(_brand_css("Amazon-style"), unsafe_allow_html=True)

    if not bounds:
        st.title("Retail Command Center")
        st.warning("No data found yet. Run the pipeline first.")
        return

    min_date, max_date = bounds

    st.markdown(
        """
        <div class="hero">
            <h1>Retail Command Center</h1>
            <p>Executive dashboard over the public Sample Superstore warehouse, designed for leadership, operations and category management.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.markdown("### Control panel")
        brand_mode = st.selectbox("View mode", ["Amazon-style", "Executive", "Neutral"], index=0)
        st.caption("The selected mode changes the visual language and the default emphasis of the dashboard.")
        date_range = st.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        selected_cities = st.multiselect("City", options=choices["cities"], default=choices["cities"])
        selected_categories = st.multiselect("Category", options=choices["categories"], default=choices["categories"])
        selected_segments = st.multiselect("Segment", options=choices["segments"], default=choices["segments"])
        selected_regions = st.multiselect("Region", options=choices["regions"], default=choices["regions"])
        selected_ship_modes = st.multiselect("Ship mode", options=choices["ship_modes"], default=choices["ship_modes"])
        top_n = st.slider("Top products shown", min_value=3, max_value=20, value=10)
        compare_previous = st.toggle("Compare with previous period", value=True)
        st.markdown("### Data source")
        st.code(config.database_url, language="text")

    st.markdown(_brand_css(brand_mode), unsafe_allow_html=True)

    if isinstance(date_range, tuple):
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range

    selection = Selection(
        start_date=start_date,
        end_date=end_date,
        cities=tuple(selected_cities),
        categories=tuple(selected_categories),
        segments=tuple(selected_segments),
        regions=tuple(selected_regions),
        ship_modes=tuple(selected_ship_modes),
        top_n=top_n,
        brand_mode=brand_mode,
        compare_previous=compare_previous,
    )

    filtered = load_filtered_data(config.database_url, selection)
    if filtered.empty:
        st.warning("No data matches the selected filters.")
        return

    previous = None
    if compare_previous:
        previous_selection = _build_previous_selection(selection)
        if previous_selection and previous_selection.start_date >= min_date:
            previous_frame = load_filtered_data(config.database_url, previous_selection)
            if not previous_frame.empty:
                previous = _aggregate(previous_frame)

    current = _aggregate(filtered)
    _render_kpi_cards(current, previous)

    tab_executive, tab_commercial, tab_customers, tab_geography = st.tabs(
        ["Executive", "Commercial", "Customers", "Geography"]
    )

    with tab_executive:
        _render_executive(current, previous)

    with tab_commercial:
        _render_commercial(current, top_n=selection.top_n)

    with tab_customers:
        _render_customers(current)

    with tab_geography:
        _render_geography(current)

    st.markdown("### Selection summary")
    st.caption(
        f"{selection.start_date} to {selection.end_date} | "
        f"{len(selection.cities)} cities | {len(selection.categories)} categories | "
        f"{len(selection.segments)} segments | {len(selection.regions)} regions | "
        f"{len(selection.ship_modes)} ship modes"
    )


if __name__ == "__main__":
    main()
