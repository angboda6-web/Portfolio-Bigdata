# Retail ETL Portfolio Project

![Project cover](assets/cover.svg)

Portfolio project built around a real public ecommerce dataset and an ETL pipeline that can run locally or with PostgreSQL.

The cover and preview images are aligned with the current warehouse outputs so the repository reflects what the project actually produces today.

## What it includes

- Public dataset download from GitHub
- Cleaning and dimensional modeling
- SQLite or PostgreSQL warehouse
- SQL metrics and automated reporting
- Streamlit dashboard with business filters
- Docker for reproducibility

## Preview

![Pipeline snapshot](assets/project-snapshot.svg)

## Dataset

This project uses the public **Sample Superstore** dataset hosted in a GitHub gist.

- Source URL: `https://gist.githubusercontent.com/JPJeanlis/98192ecb788a3e5d023618e1ba3ce801/raw/superstore.csv`
- Content: orders, customers, products, sales, discounts, and profit
- Size: about 10k rows

## Architecture

```mermaid
flowchart LR
    A["Public dataset"] --> B["Local download"]
    B --> C["Cleaning and validation"]
    C --> D["SQLite or PostgreSQL warehouse"]
    D --> E["Analytical tables"]
    E --> F["Markdown report + JSON metrics"]
    E --> G["Streamlit command center"]
```

## Structure

- `main.py`: entry point
- `src/public_dataset.py`: public dataset download
- `src/warehouse.py`: cleaning, modeling, loading, checks
- `src/report.py`: final report generation
- `streamlit_app.py`: executive dashboard
- `tests/test_pipeline.py`: unit and end-to-end tests
- `assets/`: cover and preview images

## Reproducibility

- Python 3.10+
- Dependencies from `requirements.txt`
- Optional Docker Desktop
- Optional PostgreSQL

## How to run

Download the public dataset only:

```powershell
python main.py download
```

Run the full pipeline:

```powershell
python main.py run --verbose
```

Generate only the report from an existing warehouse:

```powershell
python main.py report
```

Open the dashboard:

```powershell
streamlit run streamlit_app.py
```

## VS Code quick start

If you are opening the project in Visual Studio Code, the shortest path is:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py run --verbose
streamlit run streamlit_app.py
```

Then open:

- [http://localhost:8501](http://localhost:8501)

## Optional PostgreSQL

Set `DATABASE_URL` if you want PostgreSQL instead of SQLite:

```powershell
$env:DATABASE_URL="postgresql+psycopg://portfolio_user:change_me@localhost:5432/portfolio_db"
python main.py run
streamlit run streamlit_app.py
```

Copy the example if needed:

```powershell
Copy-Item .env.example .env
```

## Docker

```powershell
docker compose --profile pipeline up --build pipeline
docker compose --profile dashboard up --build dashboard
docker compose down -v
```

## Outputs

After the pipeline runs you get:

- `data/raw/`: downloaded dataset
- `data/processed/`: cleaned CSV files
- `warehouse/sales.db`: local SQLite warehouse
- `artifacts/report.md`: final report
- `artifacts/metrics.json`: summary metrics

## Dashboard filters

The dashboard is configurable with:

- date range
- city
- category
- segment
- region
- ship mode
- top N products
- view mode

## What the project demonstrates

- Public data ingestion
- Data cleaning and modeling
- Exact monetary metrics
- SQL analytics
- Reproducible testing
- Business-oriented dashboarding
- Dockerized execution

## Current validated metrics

- Orders: 5,009
- Revenue: 2,297,200.8603
- Profit: 286,397.0217
- Customers: 793
- Top category: Technology
- Top product: Canon imageCLASS 2200 Advanced Copier
- Best customer: Sean Miller
- Top state: California
- Best day: 2014-03-18

## Troubleshooting

- If the dashboard does not load, make sure `python main.py run --verbose` finished successfully first.
- If Streamlit is already running, close the terminal that launched it and start it again.
- If PowerShell blocks the virtual environment activation, run `Set-ExecutionPolicy -Scope Process RemoteSigned` and try again.

## Next improvements

1. Add a real API source for incremental updates.
2. Add orchestration with Airflow or Prefect.
3. Add scheduled refreshes and alerts.
4. Add more enterprise KPIs and a download/export action.
