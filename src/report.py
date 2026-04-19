from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import text

from .config import PipelineConfig
from .db import create_database_engine


def build_report(config: PipelineConfig, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "report.md"
    metrics_path = output_dir / "metrics.json"

    engine = create_database_engine(config.database_url)
    try:
        with engine.connect() as conn:
            totals = conn.execute(
                text(
                    """
                    SELECT
                        COUNT(*) AS total_orders,
                        SUM(sales_total) AS revenue,
                        SUM(profit_total) AS profit
                    FROM fact_orders
                    """
                )
            ).mappings().one()
            customer = conn.execute(
                text(
                    """
                    SELECT customer_name, city, state, lifetime_value, profit
                    FROM customer_summary
                    ORDER BY lifetime_value DESC
                    LIMIT 1
                    """
                )
            ).mappings().first()
            top_products = conn.execute(
                text(
                    """
                    SELECT product_name, category, sub_category, units_sold, revenue, profit
                    FROM top_products
                    ORDER BY revenue DESC
                    LIMIT 5
                    """
                )
            ).mappings().all()
            daily = conn.execute(
                text(
                    """
                    SELECT order_date, orders_count, revenue, profit, avg_order_value
                    FROM daily_sales
                    ORDER BY order_date ASC
                    LIMIT 5
                    """
                )
            ).mappings().all()
    finally:
        engine.dispose()

    metrics = {
        "orders": int(totals["total_orders"]),
        "revenue": float(totals["revenue"] or 0.0),
        "profit": float(totals["profit"] or 0.0),
        "best_customer": {
            "customer_name": customer["customer_name"],
            "city": customer["city"],
            "state": customer["state"],
            "lifetime_value": float(customer["lifetime_value"]),
            "profit": float(customer["profit"]),
        }
        if customer
        else None,
        "top_products": [
            {
                "product_name": row["product_name"],
                "category": row["category"],
                "sub_category": row["sub_category"],
                "units_sold": int(row["units_sold"]),
                "revenue": float(row["revenue"]),
                "profit": float(row["profit"]),
            }
            for row in top_products
        ],
        "sample_daily_sales": [
            {
                "order_date": row["order_date"],
                "orders_count": int(row["orders_count"]),
                "revenue": float(row["revenue"]),
                "profit": float(row["profit"]),
                "avg_order_value": float(row["avg_order_value"]),
            }
            for row in daily
        ],
    }

    metrics_path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# Retail ETL Portfolio Project",
        "",
        "## Resumen",
        "",
        f"- Pedidos totales: {metrics['orders']}",
        f"- Ingresos totales: {metrics['revenue']:.2f}",
        f"- Beneficio total: {metrics['profit']:.2f}",
        "",
        "## Mejor cliente",
        "",
    ]

    if customer:
        lines.extend(
            [
                f"- Nombre: {customer['customer_name']}",
                f"- Ciudad: {customer['city']}, {customer['state']}",
                f"- Lifetime value: {float(customer['lifetime_value']):.2f}",
                f"- Profit: {float(customer['profit']):.2f}",
            ]
        )
    else:
        lines.append("- No disponible")

    lines.extend(["", "## Top productos", ""])
    if top_products:
        for row in top_products:
            lines.append(
                f"- {row['product_name']} ({row['category']} / {row['sub_category']}): {int(row['units_sold'])} unidades, {float(row['revenue']):.2f} euros, profit {float(row['profit']):.2f}"
            )
    else:
        lines.append("- No disponible")

    lines.extend(["", "## Muestra diaria", ""])
    if daily:
        for row in daily:
            lines.append(
                f"- {row['order_date']}: {int(row['orders_count'])} pedidos, {float(row['revenue']):.2f} euros, profit {float(row['profit']):.2f}, ticket medio {float(row['avg_order_value']):.2f}"
            )
    else:
        lines.append("- No disponible")

    lines.extend(
        [
            "",
            "## Qué demuestra este proyecto",
            "",
            "- Limpieza de datos reales de negocio",
            "- Modelado dimensional básico",
            "- SQL analítico",
            "- Generación de reporting reproducible",
            "- Filtros interactivos en Streamlit",
        ]
    )

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path, metrics_path
