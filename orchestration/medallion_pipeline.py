#!/usr/bin/env python3
"""
Medallion Architecture Pipeline Orchestration
Bronze → Silver → Gold
"""
import sys
import os
import logging
from datetime import datetime
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import medallion layer modules
from scripts.bronze.ingest_bronze import BronzeIngestor
from scripts.bronze.load_bronze import BronzeLoader
# from scripts.silver.transform_silver import SilverTransformer
# from scripts.silver.validate_silver import SilverValidator
# from scripts.gold.model_gold import GoldModeler
# from scripts.gold.aggregate_gold import GoldAggregator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler('data/pipeline_execution.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MedallionPipeline:
    """Orchestrates the complete medallion architecture pipeline"""
    
    def __init__(self):
        config_path = os.path.join('config', 'config.yaml')
        self.ingestor = BronzeIngestor(config_path)
        self.bronze_loader = BronzeLoader(config_path)
        # self.silver_transformer = SilverTransformer()
        # self.silver_validator = SilverValidator()
        # self.gold_modeler = GoldModeler()
        # self.gold_aggregator = GoldAggregator()
        
        # Pipeline execution metadata
        self.execution_metadata = {
            'pipeline_start': None,
            'pipeline_end': None,
            'layer_results': {},
            'overall_status': None
        }
    
    def _log_execution_metadata(self):
        """Log pipeline execution metadata"""
        metadata_file = 'data/pipeline_metadata.json'
        
        try:
            import json
            with open(metadata_file, 'w') as f:
                json.dump(self.execution_metadata, f, indent=2, default=str)
            
            logger.info(f"Execution metadata saved to {metadata_file}")
        except Exception as e:
            logger.error(f"Failed to save execution metadata: {e}")
    
    def execute_bronze_layer(self) -> Dict[str, Any]:
        """Execute Bronze Layer: Data ingestion and loading"""
        logger.info("Starting BRONZE Layer Execution")
        start_time = datetime.now()
        
        try:
            # Step 1: Data Ingestion
            logger.info("Step 1: Data Ingestion")
            ingestion_metadata = self.ingestor.run()
            
            # Get list of bronze files created
            import glob
            bronze_files = (
                glob.glob("data/bronze/**/*.json", recursive=True)
                + glob.glob("data/bronze/**/*.parquet", recursive=True)
                + glob.glob("data/bronze/**/*.csv", recursive=True)
            )
            
            # Step 2: Load to Database
            logger.info("Step 2: Load to Bronze Database")
            records_loaded = self.bronze_loader.run(bronze_files, ingestion_metadata)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                'status': 'SUCCESS',
                'duration_seconds': duration,
                'files_ingested': len(bronze_files),
                'records_loaded': records_loaded,
                'start_time': start_time,
                'end_time': end_time
            }
            
            logger.info(f"BRONZE Layer Completed: {records_loaded} records loaded in {duration:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"BRONZE Layer Failed: {e}")
            return {
                'status': 'FAILED',
                'error': str(e),
                'start_time': start_time,
                'end_time': datetime.now()
            }
    
    # def execute_silver_layer(self) -> Dict[str, Any]:
    #     """Execute Silver Layer: Data transformation and validation"""
    #     logger.info("Starting SILVER Layer Execution")
    #     start_time = datetime.now()
        
    #     try:
    #         # Step 1: Transform data
    #         logger.info("Step 1: Data Transformation")
    #         transformation_results = self.silver_transformer.run()
            
    #         # Step 2: Validate data quality
    #         logger.info("Step 2: Data Validation")
    #         validation_results = self.silver_validator.run()
            
    #         end_time = datetime.now()
    #         duration = (end_time - start_time).total_seconds()
            
    #         result = {
    #             'status': 'SUCCESS',
    #             'duration_seconds': duration,
    #             'transformation_results': transformation_results,
    #             'validation_results': validation_results,
    #             'start_time': start_time,
    #             'end_time': end_time
    #         }
            
    #         quality_score = validation_results.get('quality_score', 0)
    #         logger.info(f"SILVER Layer Completed: Quality Score = {quality_score:.1f}% in {duration:.2f}s")
    #         return result
            
    #     except Exception as e:
    #         logger.error(f"SILVER Layer Failed: {e}")
    #         return {
    #             'status': 'FAILED',
    #             'error': str(e),
    #             'start_time': start_time,
    #             'end_time': datetime.now()
    #         }
    
    # def execute_gold_layer(self) -> Dict[str, Any]:
    #     """Execute Gold Layer: Business modeling and aggregation"""
    #     logger.info("Starting GOLD Layer Execution")
    #     start_time = datetime.now()
        
    #     try:
    #         # Step 1: Create business models
    #         logger.info("Step 1: Business Modeling")
    #         modeling_results = self.gold_modeler.run()
            
    #         # Step 2: Create aggregates
    #         logger.info("Step 2: Aggregation")
    #         aggregation_results = self.gold_aggregator.run()
            
    #         end_time = datetime.now()
    #         duration = (end_time - start_time).total_seconds()
            
    #         result = {
    #             'status': 'SUCCESS',
    #             'duration_seconds': duration,
    #             'modeling_results': modeling_results,
    #             'aggregation_results': aggregation_results,
    #             'start_time': start_time,
    #             'end_time': end_time
    #         }
            
    #         total_records = sum(r.get('records_created', 0) for r in modeling_results.values())
    #         logger.info(f"GOLD Layer Completed: {total_records} models created in {duration:.2f}s")
    #         return result
            
    #     except Exception as e:
    #         logger.error(f"GOLD Layer Failed: {e}")
    #         return {
    #             'status': 'FAILED',
    #             'error': str(e),
    #             'start_time': start_time,
    #             'end_time': datetime.now()
    #         }
    
    def run_pipeline(self):
        """Execute complete medallion architecture pipeline"""
        logger.info("=" * 70)
        logger.info("MEDALLION ARCHITECTURE PIPELINE")
        logger.info(f"Start Time: {datetime.now()}")
        logger.info("=" * 70)
        
        self.execution_metadata['pipeline_start'] = datetime.now()
        
        try:
            # BRONZE Layer
            bronze_result = self.execute_bronze_layer()
            self.execution_metadata['layer_results']['bronze'] = bronze_result
            
            if bronze_result['status'] == 'FAILED':
                logger.error("Pipeline stopped due to BRONZE layer failure")
                self.execution_metadata['overall_status'] = 'FAILED'
                return False
            
            # # SILVER Layer
            # silver_result = self.execute_silver_layer()
            # self.execution_metadata['layer_results']['silver'] = silver_result
            
            # if silver_result['status'] == 'FAILED':
            #     logger.error("Pipeline stopped due to SILVER layer failure")
            #     self.execution_metadata['overall_status'] = 'FAILED'
            #     return False
            
            # # GOLD Layer
            # gold_result = self.execute_gold_layer()
            # self.execution_metadata['layer_results']['gold'] = gold_result
            
            # if gold_result['status'] == 'FAILED':
            #     logger.error("Pipeline stopped due to GOLD layer failure")
            #     self.execution_metadata['overall_status'] = 'FAILED'
            #     return False
            
            # Pipeline Success
            self.execution_metadata['pipeline_end'] = datetime.now()
            self.execution_metadata['overall_status'] = 'SUCCESS'
            
            # Calculate total duration
            total_duration = (
                self.execution_metadata['pipeline_end'] - 
                self.execution_metadata['pipeline_start']
            ).total_seconds()
            
            logger.info("=" * 70)
            logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
            logger.info(f"Total Duration: {total_duration:.2f} seconds")
            logger.info("=" * 70)
            
            # Log execution metadata
            self._log_execution_metadata()
            
            return True
            
        except Exception as e:
            logger.error(f"Unexpected pipeline error: {e}")
            self.execution_metadata['pipeline_end'] = datetime.now()
            self.execution_metadata['overall_status'] = 'FAILED'
            self.execution_metadata['error'] = str(e)
            self._log_execution_metadata()
            return False

def main():
    """Main entry point for the pipeline"""
    pipeline = MedallionPipeline()
    
    # Run the pipeline
    success = pipeline.run_pipeline()
    
    # Exit with appropriate code
    if success:
        logger.info("Pipeline completed successfully 🎉")
        sys.exit(0)
    else:
        logger.error("Pipeline failed")
        sys.exit(1)

if __name__ == "__main__":
    main()