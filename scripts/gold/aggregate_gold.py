import pandas as pd
from sqlalchemy import create_engine, text
import yaml
import logging
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

class GoldAggregator:
    """Gold Layer: Create aggregated tables for reporting"""
    
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        db_config = self.config['database']
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        self.gold_schema = db_config['schemas']['gold']
    
    def create_daily_aggregates(self):
        """Create daily aggregated tables"""
        try:
            with self.engine.connect() as conn:
                
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {self.gold_schema}.daily_aggregates (
                        aggregate_date DATE PRIMARY KEY,
                        data_sources_processed INTEGER,
                        total_records_processed BIGINT,
                        bronze_records INTEGER,
                        silver_records INTEGER,
                        gold_records INTEGER,
                        data_quality_score INTEGER,
                        processing_duration_seconds INTEGER,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """))
                
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {self.gold_schema}.weekly_aggregates (
                        week_start_date DATE PRIMARY KEY,
                        week_number INTEGER,
                        year INTEGER,
                        total_records_processed BIGINT,
                        avg_daily_records DECIMAL(10,2),
                        peak_day_records INTEGER,
                        avg_quality_score DECIMAL(5,2),
                        trend_direction VARCHAR(20),
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """))
                
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {self.gold_schema}.monthly_kpis (
                        month_start DATE PRIMARY KEY,
                        month_name VARCHAR(20),
                        year INTEGER,
                        total_covid_cases BIGINT,
                        total_covid_deaths BIGINT,
                        avg_mortality_rate DECIMAL(5,2),
                        total_users INTEGER,
                        new_users INTEGER,
                        user_growth_rate DECIMAL(5,2),
                        data_completeness_score INTEGER,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """))
                
                conn.commit()
            
            logger.info("Created aggregated tables in gold layer")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create aggregated tables: {e}")
            return False
    
    def populate_daily_aggregates(self):
        """Populate daily aggregates with data"""
        try:
            today = datetime.now().date()
            
            metrics_query = """
                WITH bronze_stats AS (
                    SELECT COUNT(*) as bronze_count
                    FROM bronze.raw_users
                    WHERE DATE(ingestion_timestamp) = CURRENT_DATE
                ),
                silver_stats AS (
                    SELECT COUNT(*) as silver_count
                    FROM silver.clean_users
                    WHERE DATE(processing_timestamp) = CURRENT_DATE
                ),
                gold_stats AS (
                    SELECT COUNT(*) as gold_count
                    FROM gold.user_analytics_summary
                    WHERE summary_date = CURRENT_DATE
                )
                SELECT 
                    COALESCE(b.bronze_count, 0) as bronze_records,
                    COALESCE(s.silver_count, 0) as silver_records,
                    COALESCE(g.gold_count, 0) as gold_records
                FROM bronze_stats b
                CROSS JOIN silver_stats s
                CROSS JOIN gold_stats g
            """
            
            metrics_df = pd.read_sql(metrics_query, self.engine)
            
            total_records = (
                metrics_df['bronze_records'].iloc[0] +
                metrics_df['silver_records'].iloc[0] +
                metrics_df['gold_records'].iloc[0]
            )
            
            quality_query = f"""
                SELECT AVG(data_quality_score) as avg_score
                FROM {self.gold_schema}.covid_global_summary
                WHERE summary_date = CURRENT_DATE
            """
            
            quality_df = pd.read_sql(quality_query, self.engine)
            if quality_df.empty or pd.isna(quality_df['avg_score'].iloc[0]):
                quality_score = 85
            else:
                quality_score = int(quality_df['avg_score'].iloc[0])
            
            daily_data = {
                'aggregate_date': today,
                'data_sources_processed': 4,  # API, CSV, etc.
                'total_records_processed': int(total_records),
                'bronze_records': int(metrics_df['bronze_records'].iloc[0]),
                'silver_records': int(metrics_df['silver_records'].iloc[0]),
                'gold_records': int(metrics_df['gold_records'].iloc[0]),
                'data_quality_score': quality_score,
                'processing_duration_seconds': 3600,  # Example
                'last_updated': datetime.now()
            }
            
            insert_sql = text(f"""
                INSERT INTO {self.gold_schema}.daily_aggregates (
                    aggregate_date, data_sources_processed, total_records_processed,
                    bronze_records, silver_records, gold_records, data_quality_score,
                    processing_duration_seconds, last_updated
                ) VALUES (
                    :aggregate_date, :data_sources_processed, :total_records_processed,
                    :bronze_records, :silver_records, :gold_records, :data_quality_score,
                    :processing_duration_seconds, :last_updated
                )
                ON CONFLICT (aggregate_date) DO UPDATE SET
                    data_sources_processed = EXCLUDED.data_sources_processed,
                    total_records_processed = EXCLUDED.total_records_processed,
                    bronze_records = EXCLUDED.bronze_records,
                    silver_records = EXCLUDED.silver_records,
                    gold_records = EXCLUDED.gold_records,
                    data_quality_score = EXCLUDED.data_quality_score,
                    processing_duration_seconds = EXCLUDED.processing_duration_seconds,
                    last_updated = EXCLUDED.last_updated;
            """)

            with self.engine.connect() as conn:
                conn.execute(insert_sql, daily_data)
                conn.commit()
            
            logger.info(f"Populated daily aggregates for {today}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to populate daily aggregates: {e}")
            return False
    
    def create_performance_views(self):
        """Create performance monitoring views"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    CREATE OR REPLACE VIEW {self.gold_schema}.v_pipeline_performance AS
                    SELECT 
                        aggregate_date,
                        total_records_processed,
                        data_quality_score,
                        processing_duration_seconds,
                        CASE 
                            WHEN data_quality_score >= 90 THEN 'EXCELLENT'
                            WHEN data_quality_score >= 80 THEN 'GOOD'
                            WHEN data_quality_score >= 70 THEN 'FAIR'
                            ELSE 'POOR'
                        END as quality_rating,
                        ROUND(total_records_processed::DECIMAL / NULLIF(processing_duration_seconds, 0), 2) as records_per_second
                    FROM {self.gold_schema}.daily_aggregates
                    ORDER BY aggregate_date DESC;
                """))
                
                conn.execute(text(f"""
                    CREATE OR REPLACE VIEW {self.gold_schema}.v_data_completeness AS
                    SELECT 
                        summary_date,
                        total_countries,
                        total_confirmed,
                        total_deaths,
                        CASE 
                            WHEN total_confirmed > 0 THEN 
                                ROUND((total_confirmed - total_deaths)::DECIMAL / total_confirmed * 100, 2)
                            ELSE 0
                        END as survival_rate,
                        CASE 
                            WHEN total_confirmed > 0 THEN 
                                ROUND(total_recovered::DECIMAL / total_confirmed * 100, 2)
                            ELSE 0
                        END as recovery_percentage
                    FROM {self.gold_schema}.covid_global_summary
                    ORDER BY summary_date DESC;
                """))
                
                conn.execute(text(f"""
                    CREATE OR REPLACE VIEW {self.gold_schema}.v_trend_analysis AS
                    WITH daily_trends AS (
                        SELECT 
                            trend_date,
                            country,
                            confirmed_cases,
                            LAG(confirmed_cases, 1) OVER (PARTITION BY country ORDER BY trend_date) as prev_day_cases,
                            LAG(confirmed_cases, 7) OVER (PARTITION BY country ORDER BY trend_date) as prev_week_cases
                        FROM {self.gold_schema}.covid_country_trends
                    )
                    SELECT 
                        trend_date,
                        country,
                        confirmed_cases,
                        prev_day_cases,
                        prev_week_cases,
                        confirmed_cases - COALESCE(prev_day_cases, 0) as daily_increase,
                        CASE 
                            WHEN prev_week_cases > 0 THEN 
                                ROUND((confirmed_cases - prev_week_cases)::DECIMAL / prev_week_cases * 100, 2)
                            ELSE NULL
                        END as weekly_growth_percent
                    FROM daily_trends
                    ORDER BY trend_date DESC, confirmed_cases DESC;
                """))
                
                conn.commit()
            
            logger.info("Created performance monitoring views")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create performance views: {e}")
            return False
    
    def run(self):
        """Execute gold layer aggregation"""
        logger.info("=" * 60)
        logger.info("GOLD LAYER: Aggregation Started")
        logger.info("=" * 60)
        
        results = {}
        
        tables_created = self.create_daily_aggregates()
        results['tables_created'] = tables_created
        
        aggregates_populated = self.populate_daily_aggregates()
        results['aggregates_populated'] = aggregates_populated
        
        views_created = self.create_performance_views()
        results['views_created'] = views_created
        
        logger.info("=" * 60)
        logger.info("GOLD LAYER: Aggregation Completed")
        logger.info("=" * 60)
        
        return results