# Project Report: CHAI ASSESSMENT Medallion Data Pipeline

## 1. Design and Architecture Choices

- **Medallion Architecture (Bronze → Silver → Gold)**  
  - **Bronze**: Stores raw ingested data for three datasets only: `users`, `posts`, and `covid`. Raw API/CSV files are written to `data/bronze` and loaded into PostgreSQL `bronze` schema tables (`raw_users`, `raw_posts`, `raw_covid`, plus `ingestion_metadata`).  
  - **Silver**: Cleansed, enriched data in `silver` schema (`clean_users`, `clean_posts`, `clean_covid`) with data-quality scores and logs in `data_quality_logs`. Business‑friendly fields (email domain, geo, company, COVID rates, moving averages) are computed here.  
  - **Gold**: Analytics‑ready tables in `gold` schema (`user_analytics_summary`, `covid_global_summary`, `covid_country_trends`, `user_engagement_metrics`) plus materialized views for fast reporting.

- **Storage and Compute Separation**  
  - **MinIO** as an S3‑compatible data lake for file storage across layers (`bronze-layer`, `silver-layer`, `gold-layer`).  
  - **PostgreSQL** as the warehouse and serving layer for structured analytics queries.

- **Orchestration**  
  - A single orchestration entrypoint `orchestration/medallion_pipeline.py` coordinates Bronze → Silver → Gold via dedicated classes: `BronzeIngestor`, `BronzeLoader`, `SilverTransformer`, `SilverValidator`, `GoldModeler`, `GoldAggregator`.  
  - Docker Compose defines services for the pipeline container, Postgres, MinIO, and Airflow (for scheduling/monitoring if desired).

- **Configuration‑Driven**  
  - `config/config.yaml` centralizes database connection details, schema names, storage paths, MinIO settings, and data source URLs. This keeps the pipeline portable across environments.

- **Scope Simplification**  
  -  The pipeline is intentionally focused on the users/posts REST APIs and the COVID CSV dataset for a compact, easier‑to‑understand design.

## 2. Optimization Strategies Applied

- **Efficient Database Loading**  
  - Bronze loader uses PostgreSQL `COPY` via bulk inserts from in‑memory CSV buffers to load large datasets efficiently.  
  - Tables are truncated where appropriate before new loads to avoid duplicates and keep schemas compact.

- **Schema and Index Design**  
  - Separate schemas (`bronze`, `silver`, `gold`) isolate concerns and make lifecycle management easier.  
  - Targeted indexes on join/filter keys (e.g., user IDs, dates, country) support common analytics queries without over‑indexing.

- **Data Quality and Validation**  
  - Silver layer applies validation rules (null checks, type checks, COVID business rules, freshness checks) and logs results to a unified `data_quality_logs` table.  
  - Data quality scores (for users and COVID) are computed once in Silver and reused in Gold analytics.

- **Batch‑Friendly Orchestration**  
  - The pipeline is designed as an idempotent batch job: ingestion, load, transform, validate, and model steps can be rerun after truncation without manual cleanup.

- **Storage Hygiene**  
  - Generated artifacts (`__pycache__`, old bronze files, large logs) are excluded from the core logic and can be cleaned regularly to keep the repository light.

## 3. How to Scale or Extend the Pipeline

- **Scaling Volume and Concurrency**  
  - Run the pipeline container on more powerful or autoscaled infrastructure (larger CPU/RAM, Kubernetes) while keeping Postgres and MinIO as shared services.  
  - Partition large tables by date (e.g., `record_date` in COVID tables) to improve query and maintenance performance.  
  - Offload heavy aggregations to materialized views and refresh them incrementally instead of recomputing full tables.

- **Adding New Data Sources**  
  - Use the same pattern as `users`, `posts`, and `covid`:  
    - Add API/CSV endpoints in `config/config.yaml`.  
    - Extend Bronze ingestion to fetch and land new raw files.  
    - Add new Bronze tables and corresponding Silver transforms for cleaning and standardizing.  
    - Surface new KPIs in Gold tables or views.

- **Richer Data Quality and Governance**  
  - Add more rules to the data‑quality configuration (e.g., thresholds, uniqueness, referential rules) and centralize them in config instead of hard‑coding.  
  - Track pipeline runs and data quality over time to build operational dashboards.

- **Operationalization with Airflow**  
  - Wrap each layer (Bronze, Silver, Gold) as separate Airflow tasks/DAGs to enable retries, dependencies, and SLAs.  
  - Schedule periodic runs (hourly/daily) with notifications on failure.

- **Extending Analytics**  
  - Add new Gold‑layer models for user cohorts, engagement funnels, or COVID regional forecasts.  
  - Expose Gold tables and views directly to BI tools (e.g., Power BI, Tableau, Metabase) for self‑service analytics.
