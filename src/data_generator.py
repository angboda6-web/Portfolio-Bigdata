from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path


FIRST_NAMES = [
    "Ana",
    "Carlos",
    "Lucia",
    "Mario",
    "Elena",
    "Pablo",
    "Marta",
    "Javier",
    "Sara",
    "Diego",
    "Laura",
    "Nuria",
]

LAST_NAMES = [
    "Gomez",
    "Lopez",
    "Sanchez",
    "Martin",
    "Perez",
    "Garcia",
    "Rodriguez",
    "Torres",
    "Fernandez",
    "Romero",
]

CITIES = [
    "Madrid",
    "Barcelona",
    "Valencia",
    "Sevilla",
    "Bilbao",
    "Malaga",
    "Zaragoza",
]

CATEGORIES = {
    "Electronics": ["Headphones", "Keyboard", "Mouse", "Monitor", "Webcam", "USB Hub"],
    "Home": ["Lamp", "Desk Organizer", "Coffee Mug", "Notebook", "Backpack", "Water Bottle"],
    "Fitness": ["Yoga Mat", "Dumbbells", "Resistance Band", "Smart Scale", "Skipping Rope"],
    "Beauty": ["Face Serum", "Shampoo", "Body Lotion", "Sunscreen", "Lip Balm"],
}

PAYMENT_METHODS = ["card", "paypal", "transfer"]


@dataclass(frozen=True)
class GeneratedFiles:
    customers: Path
    products: Path
    orders: Path
    order_items: Path
    payments: Path


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    _ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _slugify(text: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "." for ch in text).strip(".")


def build_customers(rng: random.Random, count: int) -> list[dict[str, object]]:
    customers: list[dict[str, object]] = []
    for idx in range(1, count + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        name = f"{first} {last}"
        email = f"{_slugify(first)}.{_slugify(last)}{idx}@example.com"
        if idx % 11 == 0:
            email = email.upper()
        if idx % 17 == 0:
            name = f" {name} "
        customers.append(
            {
                "customer_id": f"C{idx:04d}",
                "customer_name": name,
                "email": email,
                "city": rng.choice(CITIES),
                "signup_date": (date(2023, 1, 1) + timedelta(days=rng.randint(0, 500))).isoformat(),
            }
        )

    customers.append(
        {
            "customer_id": "C0000",
            "customer_name": "Invalid Row",
            "email": "invalid-email",
            "city": "",
            "signup_date": "not-a-date",
        }
    )
    return customers


def build_products(rng: random.Random, count: int) -> list[dict[str, object]]:
    products: list[dict[str, object]] = []
    category_items = [(category, item) for category, items in CATEGORIES.items() for item in items]
    for idx in range(1, count + 1):
        category, product_name = rng.choice(category_items)
        price = round(rng.uniform(8, 180), 2)
        products.append(
            {
                "product_id": f"P{idx:04d}",
                "product_name": product_name,
                "category": category,
                "unit_price": price,
            }
        )

    products.append(
        {
            "product_id": "P0000",
            "product_name": "Broken Item",
            "category": "Unknown",
            "unit_price": -10,
        }
    )
    return products


def build_orders(
    rng: random.Random,
    customers: list[dict[str, object]],
    days: int,
    orders_per_day: int,
) -> list[dict[str, object]]:
    orders: list[dict[str, object]] = []
    start_day = date(2024, 1, 1)
    order_counter = 1
    valid_customers = [row for row in customers if row["customer_id"] != "C0000"]

    for offset in range(days):
        current_date = start_day + timedelta(days=offset)
        daily_orders = max(1, int(rng.gauss(orders_per_day, max(1, orders_per_day * 0.2))))
        for _ in range(daily_orders):
            customer = rng.choice(valid_customers)
            status = rng.choices(
                ["completed", "cancelled", "returned"],
                weights=[0.82, 0.1, 0.08],
                k=1,
            )[0]
            orders.append(
                {
                    "order_id": f"O{order_counter:05d}",
                    "customer_id": customer["customer_id"],
                    "order_date": current_date.isoformat(),
                    "status": status,
                    "payment_method": rng.choice(PAYMENT_METHODS),
                }
            )
            order_counter += 1

    orders.append(
        {
            "order_id": "O00000",
            "customer_id": "C9999",
            "order_date": "2024-99-99",
            "status": "completed",
            "payment_method": "card",
        }
    )
    return orders


def build_order_items(
    rng: random.Random,
    orders: list[dict[str, object]],
    products: list[dict[str, object]],
) -> list[dict[str, object]]:
    product_pool = [row for row in products if row["product_id"] != "P0000"]
    product_prices = {row["product_id"]: float(row["unit_price"]) for row in product_pool}
    items: list[dict[str, object]] = []
    item_counter = 1

    for order in orders:
        if order["status"] != "completed":
            continue
        lines = rng.randint(1, 4)
        selected_products = rng.sample(product_pool, k=min(lines, len(product_pool)))
        for product in selected_products:
            quantity = rng.randint(1, 4)
            unit_price = product_prices[product["product_id"]]
            line_total = round(quantity * unit_price, 2)
            items.append(
                {
                    "item_id": f"I{item_counter:06d}",
                    "order_id": order["order_id"],
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
            )
            item_counter += 1

    items.append(
        {
            "item_id": "I000000",
            "order_id": "O99999",
            "product_id": "P9999",
            "quantity": 0,
            "unit_price": 5,
            "line_total": 0,
        }
    )
    return items


def build_payments(
    rng: random.Random,
    orders: list[dict[str, object]],
    order_items: list[dict[str, object]],
) -> list[dict[str, object]]:
    totals: dict[str, float] = {}
    for item in order_items:
        totals[item["order_id"]] = totals.get(item["order_id"], 0.0) + float(item["line_total"])

    payments: list[dict[str, object]] = []
    for order in orders:
        if order["status"] != "completed":
            continue
        total = round(totals.get(order["order_id"], 0.0), 2)
        payments.append(
            {
                "payment_id": f"PAY-{order['order_id']}",
                "order_id": order["order_id"],
                "payment_date": order["order_date"],
                "payment_method": order["payment_method"],
                "payment_status": "paid",
                "amount": total,
            }
        )

    return payments


def generate_raw_files(
    output_dir: Path,
    seed: int = 42,
    customer_count: int = 80,
    product_count: int = 18,
    days: int = 90,
    orders_per_day: int = 8,
) -> GeneratedFiles:
    rng = random.Random(seed)
    _ensure_dir(output_dir)

    customers = build_customers(rng, customer_count)
    products = build_products(rng, product_count)
    orders = build_orders(rng, customers, days, orders_per_day)
    items = build_order_items(rng, orders, products)
    payments = build_payments(rng, orders, items)

    customers_path = output_dir / "customers.csv"
    products_path = output_dir / "products.csv"
    orders_path = output_dir / "orders.csv"
    items_path = output_dir / "order_items.csv"
    payments_path = output_dir / "payments.csv"

    _write_csv(customers_path, ["customer_id", "customer_name", "email", "city", "signup_date"], customers)
    _write_csv(products_path, ["product_id", "product_name", "category", "unit_price"], products)
    _write_csv(orders_path, ["order_id", "customer_id", "order_date", "status", "payment_method"], orders)
    _write_csv(items_path, ["item_id", "order_id", "product_id", "quantity", "unit_price", "line_total"], items)
    _write_csv(
        payments_path,
        ["payment_id", "order_id", "payment_date", "payment_method", "payment_status", "amount"],
        payments,
    )

    return GeneratedFiles(
        customers=customers_path,
        products=products_path,
        orders=orders_path,
        order_items=items_path,
        payments=payments_path,
    )

