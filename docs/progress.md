# Project Progress

This file tracks small implementation increments and the design reasoning behind them. It is separate from git history so the project can show how the platform evolved.

## 2026-07-26

- Added initial project documentation so the existing pipeline, known gaps, and target architecture are visible before changing runtime behavior.
- Chose Power BI as the planned analytics/dashboard layer based on the project direction.
- Added Docker Compose, Airflow runtime setup, raw Postgres schemas, EV sample ingestion, raw validation, dbt staging/intermediate/analytics models, dbt tests, and Power BI connection guidance.
- Added a Power BI dashboard build package with Postgres views, DAX measures, a report theme, and a page-by-page dashboard specification.

## Running Anti-Pattern List

- The EV data source is currently a local sample file, which is reproducible but not a real external operational source.
- The Power BI layer is source-controlled as build assets rather than a committed `.pbix` file because Power BI Desktop does not expose reliable command-line report authoring.
