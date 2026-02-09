import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import yaml
import logging
from datetime import datetime, timedelta
import json
from typing import Dict, Tuple
import os
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

class SilverTransformer:
    """Silver Layer: Transform raw data into clean, validated datasets"""
    
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        db_config = self.config['database']
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        self.bronze_schema = db_config['schemas']['bronze']
        self.silver_schema = db_config['schemas']['silver']
        
        self.data_quality_rules = self.config['data_quality']
        minio_conf = self.config['storage']['minio']
        self.minio_client = Minio(
            minio_conf['endpoint'],
            access_key=minio_conf['access_key'],
            secret_key=minio_conf['secret_key'],
            secure=False
        )
        try:
            for b in [self.config['storage']['minio']['bronze_bucket'],
                      self.config['storage']['minio']['silver_bucket'],
                      self.config['storage']['minio']['gold_bucket']]:
                if not self.minio_client.bucket_exists(b):
                    self.minio_client.make_bucket(b)
                    logger.info(f"Created MinIO bucket: {b}")
        except Exception:
            logger.debug("MinIO bucket creation/check failed; continuing")
    
    def create_silver_schema(self):
        """Create silver schema and tables"""
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.silver_schema};"))
            
            conn.execute(text(f"""
                -- Cleaned users table
                CREATE TABLE IF NOT EXISTS {self.silver_schema}.clean_users (
                    user_id INTEGER PRIMARY KEY,
                    full_name VARCHAR(255),
                    username VARCHAR(100),
                    email VARCHAR(255),
                    email_domain VARCHAR(100),
                    phone VARCHAR(50),
                    formatted_phone VARCHAR(50),
                    website VARCHAR(255),
                    street VARCHAR(255),
                    city VARCHAR(100),
                    zipcode VARCHAR(20),
                    geo_lat DECIMAL(10,6),
                    geo_lng DECIMAL(10,6),
                    company_name VARCHAR(255),
                    company_catchphrase TEXT,
                    company_bs TEXT,
                    name_length INTEGER,
                    email_valid BOOLEAN,
                    phone_valid BOOLEAN,
                    data_quality_score INTEGER,
                    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_ingestion_id INTEGER
                );
                
                -- Cleaned posts table
                CREATE TABLE IF NOT EXISTS {self.silver_schema}.clean_posts (
                    post_id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    title TEXT,
                    body TEXT,
                    title_length INTEGER,
                    body_length INTEGER,
                    word_count INTEGER,
                    avg_word_length DECIMAL(5,2),
                    has_links BOOLEAN,
                    sentiment_score DECIMAL(5,3),
                    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_ingestion_id INTEGER
                );
                
                -- Cleaned COVID data
                CREATE TABLE IF NOT EXISTS {self.silver_schema}.clean_covid (
                    covid_id SERIAL PRIMARY KEY,
                    record_date DATE,
                    country VARCHAR(100),
                    province VARCHAR(100),
                    confirmed INTEGER,
                    deaths INTEGER,
                    recovered INTEGER,
                    active_cases INTEGER,
                    mortality_rate DECIMAL(5,2),
                    recovery_rate DECIMAL(5,2),
                    daily_new_cases INTEGER,
                    daily_new_deaths INTEGER,
                    weekly_avg_cases DECIMAL(10,2),
                    data_quality_score INTEGER,
                    outlier_flag BOOLEAN DEFAULT FALSE,
                    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_ingestion_id INTEGER
                );
                
                
                -- Data quality logs
                CREATE TABLE IF NOT EXISTS {self.silver_schema}.data_quality_logs (
                    log_id SERIAL PRIMARY KEY,
                    table_name VARCHAR(100),
                    quality_check VARCHAR(100),
                    records_checked INTEGER,
                    records_failed INTEGER,
                    failure_rate DECIMAL(5,2),
                    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    details JSONB
                );
            """))
            
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS idx_silver_users_email ON {self.silver_schema}.clean_users(email);
                CREATE INDEX IF NOT EXISTS idx_silver_posts_user ON {self.silver_schema}.clean_posts(user_id);
                CREATE INDEX IF NOT EXISTS idx_silver_covid_date_country ON {self.silver_schema}.clean_covid(record_date, country);
                CREATE INDEX IF NOT EXISTS idx_silver_quality_logs ON {self.silver_schema}.data_quality_logs(table_name, check_timestamp);
            """))
            
            conn.commit()
        
        logger.info(f"Silver schema '{self.silver_schema}' created successfully")
    
    def transform_users(self) -> Tuple[int, Dict]:
        """Transform raw users data to silver layer"""
        try:
            query = f"""
                SELECT 
                    ingestion_id, user_id, name, username, email, phone, website,
                    address::jsonb, company::jsonb, raw_data::jsonb
                FROM {self.bronze_schema}.raw_users
                WHERE validation_status = 'pending'
            """
            
            df = pd.read_sql(query, self.engine)
            
            if df.empty:
                logger.info("No new users data to transform")
                return 0, {}
            
            df['address_dict'] = df['address']
            df['company_dict'] = df['company']
            
            transformed_data = []
            quality_issues = []
            
            for _, row in df.iterrows():
                full_name = str(row['name']).strip() if pd.notna(row['name']) else None
                email = str(row['email']).lower().strip() if pd.notna(row['email']) else None
                phone = str(row['phone']) if pd.notna(row['phone']) else None
                
                address = row['address_dict']
                street = address.get('street', '') if address else ''
                city = address.get('city', '') if address else ''
                zipcode = address.get('zipcode', '') if address else ''
                
                geo = address.get('geo', {}) if address else {}
                geo_lat = float(geo.get('lat', 0)) if geo and geo.get('lat') else 0
                geo_lng = float(geo.get('lng', 0)) if geo and geo.get('lng') else 0
                
                company = row['company_dict']
                company_name = company.get('name', '') if company else ''
                company_catchphrase = company.get('catchPhrase', '') if company else ''
                company_bs = company.get('bs', '') if company else ''
                
                email_valid = bool(email and '@' in email and '.' in email.split('@')[1])
                phone_valid = bool(phone and any(char.isdigit() for char in phone))
                
                quality_score = 100
                if not email_valid:
                    quality_score -= 30
                    quality_issues.append(f"Invalid email for user {row['user_id']}")
                if not phone_valid:
                    quality_score -= 20
                    quality_issues.append(f"Invalid phone for user {row['user_id']}")
                
                email_domain = email.split('@')[1] if email and '@' in email else None
                name_length = len(full_name) if full_name else 0
                
                formatted_phone = ''.join(filter(str.isdigit, str(phone))) if phone else None
                
                transformed_data.append({
                    'user_id': row['user_id'],
                    'full_name': full_name,
                    'username': row['username'],
                    'email': email,
                    'email_domain': email_domain,
                    'phone': phone,
                    'formatted_phone': formatted_phone,
                    'website': row['website'],
                    'street': street,
                    'city': city,
                    'zipcode': zipcode,
                    'geo_lat': geo_lat,
                    'geo_lng': geo_lng,
                    'company_name': company_name,
                    'company_catchphrase': company_catchphrase,
                    'company_bs': company_bs,
                    'name_length': name_length,
                    'email_valid': email_valid,
                    'phone_valid': phone_valid,
                    'data_quality_score': quality_score,
                    'source_ingestion_id': row['ingestion_id']
                })
            
            result_df = pd.DataFrame(transformed_data)
            expected_columns = [
                'user_id', 'full_name', 'username', 'email', 'email_domain', 'phone', 'formatted_phone',
                'website', 'street', 'city', 'zipcode', 'geo_lat', 'geo_lng', 'company_name',
                'company_catchphrase', 'company_bs', 'name_length', 'email_valid', 'phone_valid',
                'data_quality_score', 'processing_timestamp', 'source_ingestion_id'
            ]
            result_df = result_df.drop_duplicates(subset=['user_id'])
            with self.engine.connect() as conn:
                existing_user_ids = pd.read_sql(
                    f"SELECT user_id FROM {self.silver_schema}.clean_users",
                    conn
                )
            if not existing_user_ids.empty:
                result_df = result_df[~result_df['user_id'].isin(existing_user_ids['user_id'])]
            result_df = result_df[[col for col in expected_columns if col in result_df.columns]]
            if not result_df.empty:
                result_df.to_sql(
                    'clean_users',
                    self.engine,
                    schema=self.silver_schema,
                    if_exists='append',
                    index=False,
                    chunksize=10000
                )
            
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {self.bronze_schema}.raw_users
                    SET validation_status = 'processed'
                    WHERE ingestion_id IN ({','.join(map(str, df['ingestion_id'].tolist()))})
                """))
                conn.commit()
            
            self._log_data_quality(
                table_name='clean_users',
                quality_check='user_data_validation',
                records_checked=len(df),
                records_failed=len(quality_issues),
                details={'issues': quality_issues}
            )
            
            logger.info(f"Transformed {len(result_df)} users records to silver layer")
            return len(result_df), {'issues': quality_issues}
            
        except Exception as e:
            logger.error(f"Failed to transform users data: {e}")
            return 0, {'error': str(e)}
    
    def transform_covid_data(self) -> Tuple[int, Dict]:
        """Transform raw COVID data to silver layer"""
        try:
            query = f"""
                SELECT 
                    ingestion_id, date, country, province, confirmed, deaths, recovered
                FROM {self.bronze_schema}.raw_covid
                WHERE validation_status = 'pending'
                ORDER BY date, country
            """
            
            df = pd.read_sql(query, self.engine)
            
            if df.empty:
                logger.info("No new COVID data to transform")
                return 0, {}
            
            df_clean = df.copy()
            
            df_clean['confirmed'] = pd.to_numeric(df_clean['confirmed'], errors='coerce').fillna(0).astype(int)
            df_clean['deaths'] = pd.to_numeric(df_clean['deaths'], errors='coerce').fillna(0).astype(int)
            df_clean['recovered'] = pd.to_numeric(df_clean['recovered'], errors='coerce').fillna(0).astype(int)
            
            df_clean['active_cases'] = df_clean['confirmed'] - df_clean['deaths'] - df_clean['recovered']
            df_clean['active_cases'] = df_clean['active_cases'].clip(lower=0)
            
            df_clean['mortality_rate'] = np.where(
                df_clean['confirmed'] > 0,
                (df_clean['deaths'] / df_clean['confirmed'] * 100).round(2),
                0
            )
            
            df_clean['recovery_rate'] = np.where(
                df_clean['confirmed'] > 0,
                (df_clean['recovered'] / df_clean['confirmed'] * 100).round(2),
                0
            )
            
            df_clean = df_clean.sort_values(['country', 'date'])
            df_clean['daily_new_cases'] = df_clean.groupby('country')['confirmed'].diff().fillna(0).astype(int)
            df_clean['daily_new_deaths'] = df_clean.groupby('country')['deaths'].diff().fillna(0).astype(int)
            
            df_clean['weekly_avg_cases'] = df_clean.groupby('country')['daily_new_cases'] \
                .transform(lambda x: x.rolling(7, min_periods=1).mean().round(2))
            
            def calculate_quality_score(row):
                score = 100
                
                if row['confirmed'] < 0 or row['deaths'] < 0 or row['recovered'] < 0:
                    score -= 30
                
                if row['deaths'] > row['confirmed']:
                    score -= 20
                
                if row['recovered'] > row['confirmed']:
                    score -= 20
                
                if row['confirmed'] > 10000000 or row['deaths'] > 500000:
                    score -= 10
                
                return max(score, 0)
            
            df_clean['data_quality_score'] = df_clean.apply(calculate_quality_score, axis=1)
            df_clean['outlier_flag'] = (
                (df_clean['confirmed'] > 10000000) | 
                (df_clean['deaths'] > 500000) |
                (df_clean['daily_new_cases'] > 500000)
            )
            
            silver_columns = [
                'record_date', 'country', 'province', 'confirmed', 'deaths', 'recovered',
                'active_cases', 'mortality_rate', 'recovery_rate', 'daily_new_cases',
                'daily_new_deaths', 'weekly_avg_cases', 'data_quality_score',
                'outlier_flag', 'source_ingestion_id'
            ]
            
            df_clean['record_date'] = df_clean['date']
            df_clean['source_ingestion_id'] = df_clean['ingestion_id']
            
            result_df = df_clean[silver_columns]
            result_df = result_df.drop_duplicates(subset=['record_date', 'country', 'province'])
            result_df.to_sql(
                'clean_covid',
                self.engine,
                schema=self.silver_schema,
                if_exists='append',
                index=False,
                chunksize=10000
            )
            
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {self.bronze_schema}.raw_covid
                    SET validation_status = 'processed'
                    WHERE ingestion_id IN ({','.join(map(str, df['ingestion_id'].tolist()))})
                """))
                conn.commit()
            
            quality_issues = df_clean[df_clean['data_quality_score'] < 80].shape[0]
            self._log_data_quality(
                table_name='clean_covid',
                quality_check='covid_data_validation',
                records_checked=len(df),
                records_failed=quality_issues,
                details={'low_quality_records': quality_issues}
            )
            
            logger.info(f"Transformed {len(result_df)} COVID records to silver layer")
            return len(result_df), {'quality_issues': quality_issues}
            
        except Exception as e:
            logger.error(f"Failed to transform COVID data: {e}")
            return 0, {'error': str(e)}
    
    def _log_data_quality(self, table_name: str, quality_check: str, 
                         records_checked: int, records_failed: int,
                         details: Dict):
        """Log data quality results"""
        try:
            failure_rate = (records_failed / records_checked * 100) if records_checked > 0 else 0
            
            log_entry = {
                'table_name': table_name,
                'quality_check': quality_check,
                'records_checked': records_checked,
                'records_failed': records_failed,
                'failure_rate': round(failure_rate, 2),
                'details': json.dumps(details)
            }
            
            log_df = pd.DataFrame([log_entry])
            log_df.to_sql(
                'data_quality_logs',
                self.engine,
                schema=self.silver_schema,
                if_exists='append',
                index=False
            )
            
        except Exception as e:
            logger.error(f"Failed to log data quality: {e}")

    def _upload_table_to_minio(self, table_name: str):
        """Export a silver table to CSV and upload to MinIO silver bucket."""
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(f"SELECT * FROM {self.silver_schema}.{table_name}", conn)

            if df.empty:
                logger.info(f"No data found in {self.silver_schema}.{table_name} to upload")
                return

            local_base = self.config['storage']['local_path']
            local_dir = os.path.join(local_base, 'silver')
            os.makedirs(local_dir, exist_ok=True)

            filename = f"{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
            local_path = os.path.join(local_dir, filename)
            df.to_csv(local_path, index=False)

            minio_bucket = self.config['storage']['minio']['silver_bucket']
            minio_path = f"silver/{table_name}/{filename}"
            self.minio_client.fput_object(minio_bucket, minio_path, local_path)
            logger.info(f"Uploaded {table_name} to MinIO: {minio_bucket}/{minio_path}")

        except Exception as e:
            logger.error(f"Failed to upload {table_name} to MinIO: {e}")
            raise
    
    def run(self):
        """Execute silver layer transformations"""
        logger.info("=" * 60)
        logger.info("SILVER LAYER: Data Transformation Started")
        logger.info("=" * 60)
        
        self.create_silver_schema()

        with self.engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {self.silver_schema}.clean_users RESTART IDENTITY CASCADE;"))
            conn.commit()
        logger.info(f"Truncated table {self.silver_schema}.clean_users before transformation.")

        results = {}

        user_count, user_issues = self.transform_users()
        results['users'] = {
            'records_transformed': user_count,
            'issues': user_issues
        }

        covid_count, covid_issues = self.transform_covid_data()
        results['covid'] = {
            'records_transformed': covid_count,
            'issues': covid_issues
        }

        try:
            self._upload_table_to_minio('clean_users')
            self._upload_table_to_minio('clean_covid')
        except Exception as e:
            logger.error(f"Failed uploading silver tables to MinIO: {e}")

        logger.info("=" * 60)
        logger.info("SILVER LAYER: Data Transformation Completed")
        logger.info(f"Results: {results}")
        logger.info("=" * 60)

        return results