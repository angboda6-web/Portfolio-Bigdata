# Portfolio Project 1: Retail ETL Pipeline

Proyecto de portfolio para Big Data centrado en un flujo **batch ETL** reproducible.

## Qué demuestra

- Ingesta de datos sintéticos con estructura realista de e-commerce
- Limpieza y estandarización de datos
- Carga en un almacén local con SQLite
- Cálculo de métricas analíticas con SQL
- Generación de un informe final para enseñar resultados

## Arquitectura

`raw CSV -> cleaning -> SQLite warehouse -> analytical tables -> report`

## Estructura

- `main.py`: punto de entrada
- `src/data_generator.py`: crea datos brutos
- `src/warehouse.py`: limpieza, carga y modelado SQL
- `src/report.py`: crea el informe final
- `tests/test_pipeline.py`: pruebas básicas del pipeline

## Requisitos

- Python 3.10+

No hace falta instalar librerías externas: este proyecto usa solo la librería estándar de Python.

## Ejecución

Generar datos y ejecutar el pipeline completo:

```bash
python main.py run
```

Generar solo datos brutos:

```bash
python main.py generate
```

Crear solo el informe a partir de la base de datos ya construida:

```bash
python main.py report
```

## Salidas

Después de ejecutar el pipeline tendrás:

- `data/raw/`: CSV originales
- `data/processed/`: CSV limpios
- `warehouse/sales.db`: base SQLite
- `artifacts/report.md`: informe final
- `artifacts/metrics.json`: métricas resumidas

## Ideas para mejorar el portfolio

1. Cambiar el dataset sintético por una API pública.
2. Añadir orquestación con Airflow o Prefect.
3. Migrar SQLite a PostgreSQL.
4. Añadir tests de calidad de datos más estrictos.
5. Crear un dashboard con Streamlit o Power BI.

