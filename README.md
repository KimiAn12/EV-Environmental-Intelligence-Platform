# EV Environmental Intelligence Platform

This project combines electric vehicle fleet operations with local weather data to show how environmental conditions relate to vehicle efficiency and estimated range. It produces regional risk summaries and charging capacity signals that can support fleet monitoring and planning.

## System design

1. **Sources:** A sample EV fleet JSON file and current weather data from Weatherstack (mock weather is enabled by default).
2. **Ingestion:** Python scripts load both sources into PostgreSQL raw tables. Weather inserts are deduplicated by payload hash; EV records are upserted by vehicle ID.
3. **Orchestration and quality:** An Airflow daily DAG ingests both sources, runs raw data checks, then builds and tests dbt models.
4. **Transformations:** dbt creates staging and intermediate views, then analytics tables for vehicle performance, regional environmental summaries, and charging planning signals.
5. **Reporting:** SQL views in the `powerbi` schema expose performance, planning, data quality, and refresh metrics for Power BI.

## Project structure

- `api-request/` — weather and EV ingestion plus raw data validation
- `airflow/` — scheduled pipeline DAG
- `dbt/my_project/` — transformations and analytics models
- `postgres/` — database initialization SQL
- `data/ev/` — sample fleet input
- `powerbi/` — reporting views, measures, and theme

## Run locally

1. Copy `.env.example` to `.env` and adjust settings if needed. Mock weather works without an API key; set `USE_MOCK_WEATHER=false` and provide `WEATHERSTACK_API_KEY` to use Weatherstack.
2. Start the services with `docker compose up --build`.
3. Open Airflow at `http://localhost:8080`, enable the `ev-environmental-intelligence` DAG, and trigger a run. The DAG runs daily once enabled.

PostgreSQL is exposed on port `5432`. Keep credentials in `.env`; do not commit that file.
