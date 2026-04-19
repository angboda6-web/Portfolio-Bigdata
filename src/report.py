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
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_orders,
                    ROUND(SUM(CASE WHEN status = 'completed' THEN total_amount ELSE 0 END), 2) AS revenue
                FROM fact_orders
                    """
                )
            ).mappings().one()
            customer = conn.execute(
                text("SELECT customer_name, city, lifetime_value FROM customer_summary ORDER BY lifetime_value DESC LIMIT 1")
            ).mappings().first()
            top_products = conn.execute(
                text("SELECT product_name, category, units_sold, revenue FROM top_products ORDER BY revenue DESC LIMIT 5")
            ).mappings().all()
            daily = conn.execute(
                text("SELECT order_date, orders_count, revenue, avg_order_value FROM daily_sales ORDER BY order_date ASC LIMIT 5")
            ).mappings().all()
    finally:
        engine.dispose()

    metrics = {
        "orders": int(totals["total_orders"]),
        "completed_orders": int(totals["completed_orders"]),
        "revenue": float(totals["revenue"] or 0.0),
        "best_customer": dict(customer) if customer else None,
        "top_products": [dict(row) for row in top_products],
        "sample_daily_sales": [dict(row) for row in daily],
    }

    metrics_path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# Retail ETL Portfolio Project",
        "",
        "## Resumen",
        "",
        f"- Pedidos totales: {metrics['orders']}",
        f"- Pedidos completados: {metrics['completed_orders']}",
        f"- Ingresos totales: {metrics['revenue']:.2f}",
        "",
        "## Mejor cliente",
        "",
    ]

    if customer:
        lines.extend(
            [
                f"- Nombre: {customer['customer_name']}",
                f"- Ciudad: {customer['city']}",
                f"- Lifetime value: {float(customer['lifetime_value']):.2f}",
            ]
        )
    else:
        lines.append("- No disponible")

    lines.extend(["", "## Top productos", ""])
    if top_products:
        for row in top_products:
            lines.append(
                f"- {row['product_name']} ({row['category']}): {int(row['units_sold'])} unidades, {float(row['revenue']):.2f} euros"
            )
    else:
        lines.append("- No disponible")

    lines.extend(["", "## Muestra diaria", ""])
    if daily:
        for row in daily:
            lines.append(
                f"- {row['order_date']}: {int(row['orders_count'])} pedidos, {float(row['revenue']):.2f} euros, ticket medio {float(row['avg_order_value']):.2f}"
            )
    else:
        lines.append("- No disponible")

    lines.extend(
        [
            "",
            "## Qué demuestra este proyecto",
            "",
            "- Limpieza de datos y control de calidad",
            "- Modelado dimensional básico",
            "- SQL analítico",
            "- Generación de reporting reproducible",
        ]
    )

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path, metrics_path
