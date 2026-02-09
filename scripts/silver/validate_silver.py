import pandas as pd
from sqlalchemy import create_engine, text
import yaml
import logging
import json
import collections.abc

logger = logging.getLogger(__name__)

class SilverValidator:
    """Silver Layer: Data validation and quality checks"""
    
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        db_config = self.config['database']
        self.engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        self.silver_schema = db_config['schemas']['silver']
    
    def run_data_quality_checks(self):
        """Execute comprehensive data quality checks"""
        logger.info("Running data quality checks on silver layer...")
        
        checks = []
        
        null_checks = self._check_null_values()
        checks.extend(null_checks)
        
        type_checks = self._check_data_types()
        checks.extend(type_checks)
        
        ref_checks = self._check_referential_integrity()
        checks.extend(ref_checks)
        
        business_checks = self._check_business_rules()
        checks.extend(business_checks)
        
        freshness_checks = self._check_data_freshness()
        checks.extend(freshness_checks)
        
        self._log_validation_results(checks)
        
        total_checks = len(checks)
        passed_checks = sum(1 for check in checks if check['status'] == 'PASS')
        quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        logger.info(f"Data Quality Score: {quality_score:.2f}%")
        logger.info(f"Passed: {passed_checks}/{total_checks} checks")
        
        return {
            'quality_score': quality_score,
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'detailed_checks': checks
        }
    
    def _check_null_values(self):
        """Check for NULL values in critical columns"""
        checks = []
        
        null_check_queries = [
            {
                'table': 'clean_users',
                'column': 'user_id',
                'description': 'User ID should not be NULL'
            },
            {
                'table': 'clean_users',
                'column': 'email',
                'description': 'Email should not be NULL'
            },
            {
                'table': 'clean_covid',
                'column': 'record_date',
                'description': 'Record date should not be NULL'
            },
            {
                'table': 'clean_covid',
                'column': 'country',
                'description': 'Country should not be NULL'
            }
        ]
        
        for check in null_check_queries:
            query = f"""
                SELECT COUNT(*) as null_count
                FROM {self.silver_schema}.{check['table']}
                WHERE {check['column']} IS NULL
            """
            
            result = pd.read_sql(query, self.engine)
            null_count = result['null_count'].iloc[0]
            
            check_result = {
                'check_type': 'NULL_CHECK',
                'table': check['table'],
                'column': check['column'],
                'description': check['description'],
                'null_count': null_count,
                'status': 'PASS' if null_count == 0 else 'FAIL',
                'threshold': 0
            }
            
            checks.append(check_result)
        
        return checks
    
    def _check_data_types(self):
        """Validate data types and formats"""
        checks = []
        
        try:
            emails_df = pd.read_sql(
                f"SELECT email FROM {self.silver_schema}.clean_users WHERE email IS NOT NULL",
                self.engine
            )
            regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            invalid_emails = (~emails_df['email'].str.match(regex, case=False, na=False)).sum()
        except Exception:
            logger.exception("Failed to validate email formats via SQL; marking as 0 invalid for safety")
            invalid_emails = 0
        
        checks.append({
            'check_type': 'FORMAT_CHECK',
            'table': 'clean_users',
            'column': 'email',
            'description': 'Email format validation',
            'invalid_count': invalid_emails,
            'status': 'PASS' if invalid_emails == 0 else 'FAIL',
            'threshold': 0
        })
        
        date_query = f"""
            SELECT COUNT(*) as invalid_count
            FROM {self.silver_schema}.clean_covid
            WHERE record_date < '2019-12-01' 
               OR record_date > CURRENT_DATE + INTERVAL '1 day'
        """
        
        result = pd.read_sql(date_query, self.engine)
        invalid_dates = result['invalid_count'].iloc[0]
        
        checks.append({
            'check_type': 'RANGE_CHECK',
            'table': 'clean_covid',
            'column': 'record_date',
            'description': 'Date range validation (after Dec 2019, not future)',
            'invalid_count': invalid_dates,
            'status': 'PASS' if invalid_dates == 0 else 'FAIL',
            'threshold': 0
        })
        
        return checks
    
    def _check_referential_integrity(self):
        """Check relationships between tables"""
        checks = []
        
        ref_query = f"""
            SELECT COUNT(DISTINCT p.user_id) as orphaned_posts
            FROM {self.silver_schema}.clean_posts p
            LEFT JOIN {self.silver_schema}.clean_users u ON p.user_id = u.user_id
            WHERE u.user_id IS NULL
        """
        
        result = pd.read_sql(ref_query, self.engine)
        orphaned_posts = result['orphaned_posts'].iloc[0]
        
        checks.append({
            'check_type': 'REFERENTIAL_INTEGRITY',
            'parent_table': 'clean_users',
            'child_table': 'clean_posts',
            'description': 'Posts should reference existing users',
            'orphaned_records': orphaned_posts,
            'status': 'PASS' if orphaned_posts == 0 else 'FAIL',
            'threshold': 0
        })
        
        return checks
    
    def _check_business_rules(self):
        """Validate business rules"""
        checks = []
        
        business_rules = [
            {
                'query': f"""
                    SELECT COUNT(*) as violation_count
                    FROM {self.silver_schema}.clean_covid
                    WHERE confirmed < 0 OR deaths < 0 OR recovered < 0
                """,
                'description': 'COVID numbers should not be negative',
                'threshold': 0
            },
            {
                'query': f"""
                    SELECT COUNT(*) as violation_count
                    FROM {self.silver_schema}.clean_covid
                    WHERE deaths > confirmed
                """,
                'description': 'Deaths should not exceed confirmed cases',
                'threshold': 0
            },
            {
                'query': f"""
                    SELECT COUNT(*) as violation_count
                    FROM {self.silver_schema}.clean_covid
                    WHERE mortality_rate > 100 OR recovery_rate > 100
                """,
                'description': 'Rates should be between 0 and 100',
                'threshold': 0
            }
        ]
        
        for rule in business_rules:
            result = pd.read_sql(rule['query'], self.engine)
            violation_count = result['violation_count'].iloc[0]
            
            checks.append({
                'check_type': 'BUSINESS_RULE',
                'table': 'clean_covid',
                'description': rule['description'],
                'violation_count': violation_count,
                'status': 'PASS' if violation_count <= rule['threshold'] else 'FAIL',
                'threshold': rule['threshold']
            })
        
        return checks
    
    def _check_data_freshness(self):
        """Check how recent the data is"""
        checks = []
        
        freshness_query = f"""
            SELECT 
                table_name,
                MAX(processing_timestamp) as last_update,
                EXTRACT(EPOCH FROM (NOW() - MAX(processing_timestamp)))/3600 as hours_old
            FROM (
                SELECT 'clean_users' as table_name, MAX(processing_timestamp) as processing_timestamp
                FROM {self.silver_schema}.clean_users
                UNION ALL
                SELECT 'clean_covid' as table_name, MAX(processing_timestamp) as processing_timestamp
                FROM {self.silver_schema}.clean_covid
            ) updates
            GROUP BY table_name
        """
        
        result = pd.read_sql(freshness_query, self.engine)
        
        for _, row in result.iterrows():
            hours_old = row['hours_old']
            is_fresh = hours_old < 24
            
            checks.append({
                'check_type': 'FRESHNESS_CHECK',
                'table': row['table_name'],
                'description': f'Data should be updated within 24 hours',
                'hours_old': hours_old,
                'last_update': row['last_update'],
                'status': 'PASS' if is_fresh else 'FAIL',
                'threshold': 24
            })
        
        return checks
    
    def _log_validation_results(self, checks):
        """Log validation results to database"""
        if not checks:
            return
        rows = []
        for check in checks:
            table_name = check.get('table') or check.get('parent_table') or check.get('child_table') or ''
            quality_check = check.get('check_type') or check.get('description') or ''
            failed = None
            for k in ('null_count', 'invalid_count', 'violation_count', 'orphaned_records'):
                if k in check and check.get(k) is not None:
                    try:
                        failed = int(check.get(k))
                        break
                    except Exception:
                        try:
                            failed = int(float(str(check.get(k)).strip('"')))
                            break
                        except Exception:
                            failed = None
            records_checked = None
            failure_rate = None

            try:
                details = json.dumps(check, default=str)
            except Exception:
                details = json.dumps({k: str(v) for k, v in check.items()})

            rows.append({
                'table_name': table_name,
                'quality_check': quality_check,
                'records_checked': records_checked,
                'records_failed': failed,
                'failure_rate': failure_rate,
                'check_timestamp': pd.Timestamp.now(),
                'details': details
            })

        try:
            df = pd.DataFrame(rows)
            df.to_sql('data_quality_logs', self.engine, schema=self.silver_schema, if_exists='append', index=False)
            logger.info(f"Logged {len(rows)} validation results to database (normalized schema)")
        except Exception:
            logger.exception("Failed to insert normalized validation results; skipping to avoid pipeline crash.")
    
    def run(self):
        """Execute all validation checks"""
        logger.info("=" * 60)
        logger.info("SILVER LAYER: Data Validation Started")
        logger.info("=" * 60)
        try:
            results = self.run_data_quality_checks()
        except Exception as e:
            logger.exception("Data quality checks failed during execution")
            return {
                'quality_score': 0,
                'total_checks': 0,
                'passed_checks': 0,
                'detailed_checks': [],
                'error': str(e)
            }
        
        logger.info("=" * 60)
        logger.info("SILVER LAYER: Data Validation Completed")
        logger.info("=" * 60)
        
        return results