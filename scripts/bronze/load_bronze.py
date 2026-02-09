import pandas as pd
import json
import os
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
import yaml
import io
import logging
from typing import Dict, List


logger = logging.getLogger(__name__)


class BronzeLoader:
    """Loads data from the Bronze layer into a relational database for the Silver layer."""

    def truncate_all_medallion_tables(self):
        """Truncate all tables in bronze, silver, and gold schemas before loading new data."""
        schemas = self.config['database']['schemas']
        tables_by_schema = {
            'bronze': [
                'raw_users',
                'raw_posts',
                'raw_covid',
                'ingestion_metadata'
            ],
            'silver': [
                'cleaned_users',
                'cleaned_posts',
                'cleaned_covid',
                
            ],
            'gold': [
            ]
        }
        with self.engine.connect() as conn:
            for layer, schema in schemas.items():
                tables = tables_by_schema.get(layer, [])
                for table in tables:
                    try:
                        conn.execute(text(f'TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE;'))
                        logger.info(f"Truncated table: {schema}.{table}")
                    except Exception as e:
                        logger.warning(f"Could not truncate {schema}.{table}: {e}")
            conn.commit()

    def truncate_bronze_tables(self):
        """Truncate all bronze tables before loading to prevent duplicates."""
        tables = [
            'raw_users',
            'raw_posts',
            'raw_covid',
            'ingestion_metadata'
        ]
        with self.engine.connect() as conn:
            for table in tables:
                try:
                    conn.execute(text(f'TRUNCATE TABLE {self.bronze_schema}.{table} RESTART IDENTITY CASCADE;'))
                    logger.info(f"Truncated table: {self.bronze_schema}.{table}")
                except Exception as e:
                    logger.warning(f"Could not truncate {self.bronze_schema}.{table}: {e}")
            conn.commit()

    def __init__(self, config_path: str = 'config/config.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        print("DEBUG BronzeLoader config:", self.config)

        db_config = self.config['database']
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        self.bronze_schema = db_config['schemas']['bronze']
        
        self.silver_schema = db_config['schemas'].get('silver', 'silver')
        self.gold_schema = db_config['schemas'].get('gold', 'gold')

        self.bronze_local_path = self.config['storage']['local_path']

    def create_bronze_schema(self):
        """Creates the Bronze schema in the database if it doesn't exist."""
        with self.engine.connect() as conn:
            conn.execute(
                text(f"CREATE SCHEMA IF NOT EXISTS {self.bronze_schema}"))
            logger.info(f"Ensured Bronze schema exists: {self.bronze_schema}")

            conn.execute(text(f"""
                -- Users table (from API)
                CREATE TABLE IF NOT EXISTS {self.bronze_schema}.raw_users (
                    ingestion_id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    name VARCHAR(255),
                    username VARCHAR(100),
                    email VARCHAR(255),
                    phone VARCHAR(50),
                    website VARCHAR(255),
                    address JSONB,
                    company JSONB,
                    raw_data JSONB,
                    source_filename VARCHAR(255),
                    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_hash VARCHAR(64),
                    validation_status VARCHAR(20) DEFAULT 'pending'
                );
                
                -- Posts table (from API)
                CREATE TABLE IF NOT EXISTS {self.bronze_schema}.raw_posts (
                    ingestion_id SERIAL PRIMARY KEY,
                    post_id INTEGER,
                    user_id INTEGER,
                    title TEXT,
                    body TEXT,
                    raw_data JSONB,
                    source_filename VARCHAR(255),
                    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_hash VARCHAR(64),
                    validation_status VARCHAR(20) DEFAULT 'pending'
                );
                
                -- COVID data table (from CSV)
                CREATE TABLE IF NOT EXISTS {self.bronze_schema}.raw_covid (
                    ingestion_id SERIAL PRIMARY KEY,
                    date DATE,
                    country VARCHAR(100),
                    province VARCHAR(100),
                    confirmed INTEGER,
                    deaths INTEGER,
                    recovered INTEGER,
                    source_filename VARCHAR(255),
                    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_hash VARCHAR(64),
                    validation_status VARCHAR(20) DEFAULT 'pending'
                );

                -- Ingestion metadata table
                CREATE TABLE IF NOT EXISTS {self.bronze_schema}.ingestion_metadata (
                    metadata_id SERIAL PRIMARY KEY,
                    source_type VARCHAR(20),
                    source_name VARCHAR(100),
                    filename VARCHAR(255),
                    record_count INTEGER,
                    columns_count INTEGER,
                    data_hash VARCHAR(64),
                    ingestion_timestamp TIMESTAMP,
                    local_path VARCHAR(500),
                    minio_path VARCHAR(500),
                    status VARCHAR(20),
                    error_message TEXT,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))

            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS idx_bronze_users_id ON {self.bronze_schema}.raw_users(user_id);
                CREATE INDEX IF NOT EXISTS idx_bronze_posts_id ON {self.bronze_schema}.raw_posts(post_id);
                CREATE INDEX IF NOT EXISTS idx_bronze_covid_date ON {self.bronze_schema}.raw_covid(date);
                CREATE INDEX IF NOT EXISTS idx_bronze_covid_country ON {self.bronze_schema}.raw_covid(country);
                CREATE INDEX IF NOT EXISTS idx_bronze_metadata_source ON {self.bronze_schema}.ingestion_metadata(source_name);
            """))

            conn.commit()

        logger.info(
            f"Bronze schema '{self.bronze_schema}' created successfully")

    def _check_and_truncate_table(self, table_name: str):
        """Check whether a table has rows and truncate it before bulk insert."""
        try:
            with self.engine.begin() as conn:
                res = conn.execute(text(f"SELECT EXISTS (SELECT 1 FROM {self.bronze_schema}.{table_name} LIMIT 1);"))
                has_rows = bool(res.scalar())
                if has_rows:
                    conn.execute(text(f"TRUNCATE TABLE {self.bronze_schema}.{table_name} RESTART IDENTITY CASCADE;"))
                    logger.info(f"Truncated table {self.bronze_schema}.{table_name} before bulk insert.")
        except Exception as e:
            logger.warning(f"Unable to check/truncate {self.bronze_schema}.{table_name}: {e}")

    def _bulk_insert_df(self, df: pd.DataFrame, table_name: str) -> int:
        """Perform a PostgreSQL COPY FROM STDIN bulk insert from a DataFrame.

        Returns number of inserted rows (or 0 on failure).
        """
        if df is None or df.empty:
            logger.info(f"No records to insert for {self.bronze_schema}.{table_name}")
            return 0

        def _sanitize_col(col: str) -> str:
            import re
            s = re.sub(r'[^0-9a-zA-Z_]', '_', col)
            s = s.lower()
            if re.match(r'^[0-9]', s):
                s = '_' + s
            return s

        df_to_write = df.copy()
        sanitized_cols = [_sanitize_col(c) for c in df_to_write.columns.tolist()]
        df_to_write.columns = sanitized_cols

        csv_buffer = io.StringIO()
        df_to_write.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        raw_conn = self.engine.raw_connection()
        try:
            cur = raw_conn.cursor()
            cols_sql = ','.join([c for c in df_to_write.columns.tolist()])
            copy_sql = f"COPY {self.bronze_schema}.{table_name} ({cols_sql}) FROM STDIN WITH CSV"
            cur.copy_expert(copy_sql, csv_buffer)
            raw_conn.commit()
            return len(df)
        except Exception as e:
            raw_conn.rollback()
            logger.error(f"Bulk insert failed for {self.bronze_schema}.{table_name}: {e}")
            return 0
        finally:
            try:
                cur.close()
            except Exception:
                pass
            raw_conn.close()

    def load_json_data(self, file_path: str, table_name: str) -> int:
        """Loads JSON data from the Bronze layer into the specified database table."""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            if not isinstance(data, list):
                data = [data]

            filename = os.path.basename(file_path)

            records = []
            for item in data:
                record = {
                    'raw_data': json.dumps(item),
                    'source_filename': filename,
                    'ingestion_timestamp': datetime.now(),
                    'data_hash': '',  # Add empty data_hash for schema compatibility
                    'validation_status': 'pending'
                }

                if table_name == 'raw_users':
                    record.update({
                        'user_id': item.get('id'),
                        'name': item.get('name'),
                        'username': item.get('username'),
                        'email': item.get('email'),
                        'phone': item.get('phone'),
                        'website': item.get('website'),
                        'address': json.dumps(item.get('address', {})),
                        'company': json.dumps(item.get('company', {}))
                    })
                elif table_name == 'raw_posts':
                    record.update({
                        'post_id': item.get('id'),
                        'user_id': item.get('userId'),
                        'title': item.get('title'),
                        'body': item.get('body')
                    })

                records.append(record)

            df = pd.DataFrame(records)
            self._check_and_truncate_table(table_name)
            inserted = self._bulk_insert_df(df, table_name)
            logger.info(f"Loaded {inserted} records to {self.bronze_schema}.{table_name}")
            return inserted

        except Exception as e:
            logger.error(f"Failed to load JSON data from {file_path}: {e}")
            return 0
    
    def load_parquet_data(self, file_path: str, table_name: str) -> int:
        """Load Parquet data to bronze table"""
        try:
            df = pd.read_parquet(file_path)
            filename = os.path.basename(file_path)
            
            df['source_filename'] = filename
            df['ingestion_timestamp'] = datetime.now()
            df['data_hash'] = ''  # Add empty data_hash for schema compatibility
            df['validation_status'] = 'pending'
            
            self._check_and_truncate_table(table_name)
            inserted = self._bulk_insert_df(df, table_name)
            logger.info(f"Loaded {inserted} records to {self.bronze_schema}.{table_name}")
            return inserted
            
        except Exception as e:
            logger.error(f"Failed to load Parquet data from {file_path}: {e}")
            return 0
    
    def load_ingestion_metadata(self, metadata: List[Dict]):
        """Load ingestion metadata to database, mapping keys to match table schema."""
        try:
            mapped_metadata = []
            for item in metadata:
                mapped_item = {
                    'source_type': item.get('source', None),
                    'source_name': item.get('dataset', item.get('endpoint', None)),
                    'filename': item.get('file_name', item.get('local_filename', None)),
                    'record_count': item.get('records', item.get('record_count', None)),
                    'columns_count': len(item.get('columns', [])) if 'columns' in item else item.get('columns_count', None),
                    'data_hash': item.get('data_hash', None),
                    'ingestion_timestamp': item.get('ingestion_timestamp', None),
                    'local_path': item.get('local_path', None),
                    'minio_path': item.get('minio_path', None),
                    'status': item.get('status', None),
                    'error_message': item.get('error', None),
                }
                mapped_metadata.append(mapped_item)
            df = pd.DataFrame(mapped_metadata)
            df['loaded_at'] = datetime.now()
            for col in ['record_count', 'columns_count']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            self._check_and_truncate_table('ingestion_metadata')
            inserted = self._bulk_insert_df(df, 'ingestion_metadata')
            logger.info(f"Loaded {inserted} metadata records")
        except Exception as e:
            logger.error(f"Failed to load metadata: {e}")

    def load_csv_data(self, file_path: str, table_name: str) -> int:
        """Load CSV data to bronze table, with diagnostic logging and column renaming."""
        logger.info(f"Attempting to load CSV file: {file_path} into table: {self.bronze_schema}.{table_name}")
        try:
            try:
                df = pd.read_csv(file_path)
            except UnicodeDecodeError:
                logger.warning(f"UTF-8 decode failed for {file_path}, retrying with latin-1")
                df = pd.read_csv(file_path, encoding='latin-1')

            logger.info(f"CSV columns: {list(df.columns)}")
            filename = os.path.basename(file_path)

            if table_name == 'raw_covid':
                df = df.rename(columns={
                    'Date': 'date',
                    'Country/Region': 'country',
                    'Province/State': 'province',
                    'Confirmed': 'confirmed',
                    'Deaths': 'deaths',
                    'Recovered': 'recovered',
                })

            df = df.replace(r'^\s*$', pd.NA, regex=True)

            df['source_filename'] = filename
            df['ingestion_timestamp'] = datetime.now()
            if 'data_hash' not in df.columns:
                df['data_hash'] = ''
            if 'validation_status' not in df.columns:
                df['validation_status'] = 'pending'

            if table_name == 'raw_covid':
                for col in ['confirmed', 'deaths', 'recovered']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date

            if table_name == 'raw_covid':
                expected_cols = [
                    'date', 'country', 'province', 'confirmed', 'deaths', 'recovered',
                    'source_filename', 'ingestion_timestamp', 'data_hash', 'validation_status'
                ]
                df = df[[col for col in expected_cols if col in df.columns]]

            logger.info(f"DataFrame shape before insert: {df.shape}")
            logger.info(f"DataFrame head before insert:\n{df.head()}\nColumns: {list(df.columns)}")
            self._check_and_truncate_table(table_name)
            inserted = self._bulk_insert_df(df, table_name)
            logger.info(f"Loaded {inserted} records to {self.bronze_schema}.{table_name}")
            return inserted
        except Exception as e:
            logger.error(f"Failed to load CSV data from {file_path}: {e}")
            return 0

    def run(self, bronze_files: List[str], ingestion_metadata: List[Dict]):
        """Execute bronze layer loading, supporting both parquet and csv for covid."""
        logger.info("=" * 60)
        logger.info("BRONZE LAYER: Data Loading Started")
        logger.info("=" * 60)

        self.create_bronze_schema()

        self.truncate_bronze_tables()

        total_records = 0
        for file_path in bronze_files:
            filename = os.path.basename(file_path)
            ext = os.path.splitext(filename)[1].lower()

            if 'users' in filename:
                if ext == '.json':
                    records = self.load_json_data(file_path, 'raw_users')
                else:
                    logger.info(f"Skipping non-JSON users file during bronze load: {filename}")
                    continue
            elif 'posts' in filename:
                records = self.load_json_data(file_path, 'raw_posts')
            elif 'covid' in filename:
                if ext == '.parquet':
                    records = self.load_parquet_data(file_path, 'raw_covid')
                elif ext == '.csv':
                    records = self.load_csv_data(file_path, 'raw_covid')
                else:
                    logger.warning(f"Unsupported covid file type: {filename}")
                    continue
            else:
                logger.warning(f"Unknown file type: {filename}")
                continue
            total_records += records

        self.load_ingestion_metadata(ingestion_metadata)

        logger.info("=" * 60)
        logger.info(f"BRONZE LAYER: Data Loading Completed - {total_records} total records")
        logger.info("=" * 60)

        return total_records