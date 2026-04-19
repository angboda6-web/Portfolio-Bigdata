# Retail ETL Portfolio Project

![Project cover](assets/cover.svg)

Proyecto de portfolio para Big Data centrado en un flujo batch ETL reproducible.

## Overview

- Synthetic e-commerce style data generation
- Data cleaning and validation
- Loading into a local SQLite or PostgreSQL warehouse
- Analytical SQL metrics
- Automatic report generation
- Streamlit dashboard for interactive exploration

## Preview

![Pipeline snapshot](assets/project-snapshot.svg)

## Architecture

```mermaid
flowchart LR
    A["Raw CSV files"] --> B["Cleaning and validation"]
    B --> C["SQLite or PostgreSQL warehouse"]
    C --> D["Analytical tables"]
    D --> E["Markdown report + metrics"]
    D --> F["Streamlit dashboard"]
```

## Project structure

- `main.py`: command-line entry point
- `src/data_generator.py`: creates raw datasets
- `src/warehouse.py`: cleaning, loading and SQL modeling
- `src/report.py`: builds the final report
- `streamlit_app.py`: interactive dashboard
- `tests/test_pipeline.py`: end-to-end and unit tests
- `assets/`: visual cover and snapshot for the README

## Requirements

- Python 3.10 or newer
- Packages from `requirements.txt`

## How to run

Run the full pipeline:

```powershell
python main.py run
```

Run with logs and custom generation settings:

```powershell
python main.py run --verbose --seed 42 --customers 80 --products 18 --days 90 --orders-per-day 8
```

Generate only raw data:

```powershell
python main.py generate
```

Build only the report from an existing warehouse:

```powershell
python main.py report
```

Use a PostgreSQL warehouse instead of SQLite:

```powershell
$env:DATABASE_URL="postgresql+psycopg://postgres:change_me@localhost:5432/portfolio_db"
python main.py run
```

Use the local PostgreSQL instance already installed on this machine:

```powershell
Copy-Item .env.example .env
```

Then edit `.env` so `DATABASE_URL` points to:

```powershell
postgresql+psycopg://portfolio_user:Portfolio2026!@localhost:5432/portfolio_db
```

After that, `python main.py run` and `streamlit run streamlit_app.py` will use PostgreSQL automatically.

## Run with Docker

Build the image:

```powershell
docker build -t retail-etl-portfolio .
```

Run it in a disposable container:

```powershell
docker run --rm -v "${PWD}:/app" retail-etl-portfolio
```

Or use Docker Compose:

```powershell
docker compose --profile pipeline up --build pipeline
```

Run only the dashboard after the pipeline has populated the warehouse volume:

```powershell
docker compose --profile dashboard up --build dashboard
```

Stop the stack and remove the PostgreSQL volume if you want a clean reset:

```powershell
docker compose down -v
```

If Docker Desktop is not installed yet, install it first and rerun the commands above.

## Run the dashboard

Start Streamlit:

```powershell
streamlit run streamlit_app.py
```

The dashboard reads the same warehouse used by the pipeline inside Docker. If you want PostgreSQL outside Docker, set `DATABASE_URL` before launching Streamlit.

## Outputs

After running the pipeline you will see:

- `data/raw/`: raw CSV files
- `data/processed/`: cleaned CSV files
- `warehouse/sales.db`: SQLite warehouse when using the default database
- `artifacts/report.md`: final report
- `artifacts/metrics.json`: summary metrics

## Current results

- Completed orders: 576
- Revenue: 250075.86
- Top category: Fitness
- Best day: 2024-02-11

## Future configuration

`.env.example` is included to make a future move to PostgreSQL or another database straightforward.

Copy it to `.env` when you need it:

```powershell
Copy-Item .env.example .env
```

## Why this project is useful for a portfolio

- Shows batch data engineering fundamentals
- Demonstrates SQL and data modeling
- Includes testing and reproducibility
- Generates business-oriented outputs, not just raw code
- Compatible with both SQLite and PostgreSQL

## Next improvements

1. Replace synthetic data with a real API source.
2. Add orchestration with Airflow or Prefect.
3. Add more dashboard filters and KPIs.
4. Add incremental loading.
5. Move fully to PostgreSQL in production mode.
