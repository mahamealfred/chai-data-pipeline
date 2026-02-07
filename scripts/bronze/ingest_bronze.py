import requests
import pandas as pd
import json
import os
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import yaml
from typing import Dict, Any
import hashlib
import logging

logger = logging.getLogger(__name__)


class BronzeIngestor:
    """Ingests data from the API and stores it in the Bronze layer of the Medallion architecture."""

    def __init__(self, config_path: 'config/congig.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Steup local directories
        self.bronze_dir = self.config['storage']['local_path']
        os.makedirs(self.bronze_dir, exist_ok=True)

        # Steup MinIO client
        minio_config = self.config['storage']['minio']
        self.minio_client = Minio(
            minio_config['endpoint'],
            access_key=minio_config['access_key'],
            secret_key=minio_config['secret_key'],
            secure=False
        )

        # Create buckets if they don't exist
        self._create_minio_bucket()
        self.ingestion_metadata = []

    def _create_minio_bucket(self):
        """Creates the MinIO bucket if it doesn't exist."""
        buckets = [
            self.config['storage']['minio']['bronze_bucket'],
            self.config['storage']['minio']['silver_bucket'],
            self.config['storage']['minio']['gold_bucket']
        ]
        for bucket in buckets:
            if not self.minio_client.bucket_exists(bucket):
                self.minio_client.make_bucket(bucket)
                logger.info(f"Created MinIO bucket: {bucket}")
            else:
                logger.info(f"MinIO bucket already exists: {bucket}")

    def _generate_data_hash(self, data: Dict[str, Any]) -> str:
        """Generates a hash for the given data to lineage tracking"""
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def fetch_api_data(self, endpoint_name: str, endpoint_url: str) -> Dict:
        """Fetches data from the API endpoint."""
        try:
            logger.info(f"Fetching API data from {endpoint_name}")
            response = requests.get(endpoint_url, timeout=30)
            response.raise_for_status()
            data = response.json()

            # generate metadata for lineage tracking
            timestamp = datetime.now()
            data_hash = self._generate_data_hash(json.dumps(data))

            # Save to local bronze stsorage
            local_filename = f"{endpoint_name}_{timestamp.strftime('%Y%m%d%H%M%S')}.json"
            local_path = os.path.join(self.bronze_dir, local_filename)

            with open(local_path, 'w') as f:
                json.dump(data, f, indent=2)

            # Upload to MinIO           minio_path= f"bronze/{local_filename}"
            minio_path = f"api/{endpoint_name}/{local_filename}"
            self.minio_client.fput_object(
                self.config['storage']['minio']['bronze_bucket'],
                minio_path,
                local_path
            )

            # record ingestion metadata for lineage tracking
            metadata = {
                'source': 'api',
                'endpoint': endpoint_name,
                'local_filename': local_filename,
                'records': len(data) if isinstance(data, list) else 1,
                'data_hash': data_hash,
                'ingestion_timestamp': timestamp,
                'local_path': local_path,
                'minio_path': minio_path,
                'status': 'success'
            }
            self.ingestion_metadata.append(metadata)
            logger.info(f"Successfully ingested data from {endpoint_name}: {metadata}: {len(data) if isinstance(data, list) else 1} records")

            return metadata

        except Exception as e:
            logger.error(f"Failed to fetch API data {endpoint_name}: {e}")
            return {
                'source': 'api',
                'endpoint': endpoint_name,
                'status': 'failed',
                'error': str(e)
            }
    def fetch_csv_data(self, dataset_name: str, csv_url: str) -> Dict:
        """Fetches data from a CSV file."""
        try:
            logger.info(f"Fetching CSV data from {dataset_name} at {csv_url}")
            df = pd.read_csv(csv_url)

            # generate metadata for lineage tracking
            timestamp = datetime.now()
            data_hash = self._generate_data_hash(df.to_csv(index=False))

            # Save to local bronze storage

            filename = os.path.basename(csv_url)
            local_filename = f"{os.path.splitext(filename)[0]}_{timestamp.strftime('%Y%m%d%H%M%S')}.csv"
            local_path = os.path.join(self.bronze_dir, local_filename)
            df.to_csv(local_path, index=False)

            # Upload to MinIO
            minio_path = f"csv/{dataset_name}/{local_filename}"
            self.minio_client.fput_object(
                self.config['storage']['minio']['bronze_bucket'],
                minio_path,
                local_path
            )

            # record ingestion metadata for lineage tracking
            metadata = {
                'source': 'csv',
                'dataset': dataset_name,
                'file_name': filename,
                'local_filename': local_filename,
                'records': len(df),
                'columns': list(df.columns),
                'data_hash': data_hash,
                'ingestion_timestamp': timestamp,
                'local_path': local_path,
                'minio_path': minio_path,
                'status': 'success'
            }
            self.ingestion_metadata.append(metadata)
            logger.info(f"Successfully ingested CSV data from {dataset_name}: {metadata} : {len(df)} records")

            return metadata

        except Exception as e:
            logger.error(f"Failed to fetch CSV data from {dataset_name}: {e}")
            return {
                'source': 'csv',
                'file_name': os.path.basename(dataset_name),
                'status': 'failed',
                'error': str(e)
            }
    def save_ingestion_metadata(self):
        """Saves ingestion metadata for lineage tracking."""
        metatadata_path = os.path.join(self.bronze_dir, 'ingestion_metadata.json')
        with open(metatadata_path, 'w') as f:
            json.dump(self.ingestion_metadata, f, indent=2, default=str)

        # Upload metadata to MinIO
        self.minio_client.fput_object(
            self.config['storage']['minio']['bronze_bucket'],
            'ingestion_metadata/ingestion_metadata.json',
            metatadata_path
        )
        logger.info(f"Saved ingestion metadata to {metatadata_path} and uploaded to MinIO: {len(self.ingestion_metadata)} records" )
    
    def run(self):
       """Execuste bronze layer ingestion for all configured data sources."""
       logger.info("="*50)
       logger.info("Starting Bronze Layer Ingestion")
       logger.info("="*50)

       # Fetch API data
       api_sources = self.config['data_sources']['api']
       for endpoint_name, endpoint_url in api_sources.items():
           self.fetch_api_data(endpoint_name, endpoint_url)
    

       # Fetch CSV data
       csv_sources = self.config['data_sources']['csv']
       for dataset_name, csv_url in csv_sources.items():
            self.fetch_csv_data(dataset_name, csv_url)
    
       #Save ingestion metadata
       self.save_ingestion_metadata()
       logger.info("="*50)
       logger.info("Completed Bronze Layer Ingestion")
       logger.info("="*50)

       return self.ingestion_metadata

    
    
