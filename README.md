## CHAI Medallion Data Pipeline (Bronze → Silver → Gold) ASSESSMENT

This project implements an end‑to‑end medallion data architecture using a **Bronze / Silver / Gold** pattern.
It ingests data from APIs and CSV files into a data lake (MinIO), loads it into a PostgreSQL data warehouse,
applies transformations and data quality checks, and finally builds business‑ready analytics tables.

The pipeline can be orchestrated via a standalone Python orchestration script
(`orchestration/medallion_pipeline.py`) and is containerized with Docker Compose.

### Quick Summary: How to Run the Pipeline

1. Make sure Docker and Docker Compose are installed.
2. Open a terminal in the project root, for example:
	```powershell
	cd "PROJECT PATH"
	```
3. Run a one‑time pipeline execution (MinIO + Postgres + pipeline):
	```bash
	docker-compose up --build pipeline
	```
4. (Optional) Start the full stack including Airflow UI:
	```bash
	docker-compose up --build
	```
5. After it finishes, check:
	- data in PostgreSQL schemas `bronze`, `silver`, `gold`
	- MinIO console at `http://localhost:9001`
	- latest run metadata in `data/pipeline_metadata.json`.

---

## 1. Pipeline Design

High‑level design (see diagram in `design/Data architecture.png`):

- **Bronze layer (Raw)**
	- Ingests raw data from:
		- REST APIs (e.g. `users`, `posts` from JSONPlaceholder)
		- CSV datasets (e.g. COVID‑19 time‑series)
	- Stores raw files in MinIO buckets (`bronze-layer`) and local folder `data/bronze`.
	- Loads raw files into PostgreSQL schema `bronze` (tables like `raw_users`, `raw_posts`, etc.).

- **Silver layer (Cleansed & Conformed)**
	- Cleans and standardizes raw data (type casting, null handling, normalization).
	- Applies **data quality rules** defined in `config/config.yaml` (e.g. not‑null, valid values).
	- Writes cleaned datasets to PostgreSQL schema `silver`.

- **Gold layer (Analytics / Business)**
	- Builds business‑oriented models from silver tables (e.g. user analytics, COVID trends).
	- Creates aggregates and views in PostgreSQL schema `gold` for BI and reporting.

- **Orchestration & Metadata**
	- `MedallionPipeline` in `orchestration/medallion_pipeline.py` orchestrates:
		- Bronze: ingest → load
		- Silver: transform → validate
		- Gold: model → aggregate
	- Execution summary is logged to `data/pipeline_metadata.json`.
	- Detailed logs are written via Python logging (to console and log file inside the container).

---

## 2. Project Structure

Key folders and files:

- `docker-compose.yml` – Defines MinIO, PostgreSQL, pipeline service, and Airflow services.
- `Dockerfile` – Builds the image that runs the medallion pipeline.
- `config/`
	- `config.yaml` – Data sources, MinIO, PostgreSQL connection, schemas, and data quality rules.
- `orchestration/`
	- `medallion_pipeline.py` – Main orchestrator for Bronze → Silver → Gold execution.
- `scripts/`
	- `bronze/`
		- `ingest_bronze.py` – Fetches API & CSV data into `data/bronze` and MinIO `bronze-layer`.
		- `load_bronze.py` – Loads bronze files into PostgreSQL `bronze` schema.
	- `silver/`
		- `transform_silver.py` – Transforms raw bronze tables into clean silver tables.
		- `validate_silver.py` – Runs data quality checks using rules from `config.yaml`.
	- `gold/`
		- `model_gold.py` – Builds business models in `gold` schema.
		- `aggregate_gold.py` – Creates aggregates and views for analytics.
	- `database/init_db.py` – (Optional) Database helper/init script.
	- `run_local_gold.py` – Example script to run gold layer locally (if needed).
	- `run_and_tail_pipeline.ps1` – Helper PowerShell script to run pipeline and tail logs.
- `data/`
	- `sql/init_db.sql` – SQL DDL to initialize PostgreSQL schemas and base tables.
	- `bronze/` – Raw ingested files (JSON/CSV/Parquet); repopulated each run.
	- `pipeline_metadata.json` – JSON metadata for the latest pipeline execution.
- `airflow/`
	- `dags/` – Airflow DAG definitions (for scheduling the pipeline).
	- `plugins/` – Custom Airflow plugins (if any).
	- `logs/` – Created at runtime inside the Airflow container.
- `design/`
	- `Data architecture.png` – Architecture / pipeline design diagram.
- `requirements.txt` – Python dependencies for the pipeline image.

---

## 3. Technology Stack

- **Compute & Orchestration**: Python 3 (orchestration script), Docker, Docker Compose
- **Data Lake**: MinIO (S3‑compatible object storage)
- **Data Warehouse**: PostgreSQL (schemas: `bronze`, `silver`, `gold`)
- **Scheduling / UI (optional)**: Apache Airflow

---

## 4. Prerequisites

Before running the project, ensure you have:

- Docker
- Docker Compose
- (Optional) Python 3.10+ locally if you want to run scripts outside Docker.

On Windows, run all commands from a terminal in the project root:

```powershell
cd "PROJECT PATH"
```

---

## 5. How to Run the Pipeline (Docker Compose)

This is the recommended way to run the full medallion pipeline end‑to‑end.

### 5.1 Start Only the Pipeline Service

This will start MinIO and PostgreSQL, then run the pipeline service once:

```bash
docker-compose up --build pipeline
```

What happens:

1. **MinIO** starts and exposes:
	 - Console at `http://localhost:9001`
	 - S3 API at `http://localhost:9000`
2. **PostgreSQL (medallion_dw)** starts with:
	 - User: `admin`
	 - Password: `admin123`
	 - DB: `medallion_dw`
	 - Initialized with `data/sql/init_db.sql`.
3. **Pipeline container** runs:
	 - `python orchestration/medallion_pipeline.py`
	 - Writes execution metadata to `data/pipeline_metadata.json`.
	 - Writes raw files to `data/bronze` and MinIO `bronze-layer`.
	 - Populates PostgreSQL schemas `bronze`, `silver`, `gold`.

After the run completes, the container keeps running with `tail -f /dev/null` so you can
inspect logs and data.

### 5.2 Start Full Stack with Airflow

To bring up the full environment including Airflow:

```bash
docker-compose up --build
```

This will start:

- `minio` – Data lake
- `postgres` – Warehouse for medallion schemas
- `pipeline` – One‑off medallion pipeline run
- `postgres-airflow` – Metadata DB for Airflow
- `airflow` – Web UI and scheduler

Once services are healthy:

- Open Airflow UI at: `http://localhost:8080`
	- Username: `admin`
	- Password: `admin`
- DAGs from `airflow/dags` are automatically loaded.

You can then schedule or trigger the pipeline DAG from the Airflow UI.

---



## 6. Inspecting Outputs

After a successful run:

- **MinIO (raw and processed files)**
	- Access console at `http://localhost:9001` (user `admin`, password `password123`).
	- Buckets:
		- `bronze-layer` – raw ingested files
		- `silver-layer` – cleaned/curated files (if configured)
		- `gold-layer` – business‑ready exports (if configured)

- **PostgreSQL (warehouse)**
	- Connect to `localhost:5432` with:
		- User: `admin`
		- Password: `admin123`
		- DB: `medallion_dw`
	- Schemas:
		- `bronze` – raw tables
		- `silver` – cleaned & conformed tables
		- `gold` – analytics / aggregates

- **Execution metadata**
	- Check the latest run summary in `data/pipeline_metadata.json`.

---

## 7. Customization

- **Data sources**
	- Edit `config/config.yaml` under `data_sources` to point to different APIs or CSV files.

- **Data quality rules**
	- Update `data_quality.rules` in `config/config.yaml` to:
		- Add/remove required columns
		- Change allowed values (e.g. valid categories)

- **Database and storage settings**
	- Adjust `storage` and `database` sections in `config/config.yaml`.
	- If you change credentials or ports, keep `docker-compose.yml` in sync.

---

## 9. Troubleshooting

- **Containers fail to start**
	- Run `docker-compose ps` to check which service is unhealthy.
	- Ensure ports `5432`, `9000`, `9001`, `8080` are not already in use.

- **Pipeline fails mid‑run**
	- Inspect logs:
		- Docker logs: `docker-compose logs pipeline`
		- Inside container: check log output from `medallion_pipeline.py`.

- **Schema or table missing**
	- Verify `data/sql/init_db.sql` has been applied (it runs automatically when
		the `postgres` container is first created).

---

