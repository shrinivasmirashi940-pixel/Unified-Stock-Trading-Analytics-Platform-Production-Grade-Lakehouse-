"""
Unified Stock Trading Analytics Pipeline - Single DAG
===================================================

This DAG orchestrates the complete data pipeline:
1. Bronze Layer: Ingest FX rates and symbols from GCS
2. Silver Layer: Transform BigQuery trades with enrichment
3. Gold Layer: Aggregate analytics (PnL, Liquidity, User Activity)

All tasks run in sequence with proper dependencies within a single DAG.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=45),
}

# Create the unified DAG
dag = DAG(
    'stock_trading_pipeline',
    default_args=default_args,
    description='Complete Bronze → Silver → Gold data pipeline',
    schedule_interval='*/60 * * * *',  # Every 60 minutes
    start_date=datetime(2026, 8, 30),
    catchup=False,
    tags=['unified', 'bronze', 'silver', 'gold', 'iceberg'],
)

# ============================================================================
# BRONZE LAYER TASKS
# ============================================================================
# Sensor: Check if FX rates file exists for the date
check_fx_rates_file = GCSObjectExistenceSensor(
    task_id='check_fx_rates_file',
    bucket='fintech-trading-lakehouse-gds1',
    object='reference/fx_rates/ds={{ ds }}/fx_rates.csv',
    timeout=300,
    poke_interval=60,
    dag=dag,
)

# Ingest FX rates to Bronze
ingest_fx_rates_bronze = DataprocSubmitJobOperator(
    task_id='ingest_fx_rates_bronze',
    project_id='project-2eef9167-38f3-4223-ac1',
    region='us-central1',
    job={
        'placement': {'cluster_name': 'my-data-cluster'},
        'pyspark_job': {
            'main_python_file_uri': 'gs://fintech-trading-lakehouse-gds1/spark_jobs/bronze/ingest_fx_bronze.py'
        }
    },
    dag=dag,
)

# Ingest symbols to Bronze
ingest_symbols_bronze = DataprocSubmitJobOperator(
    task_id='ingest_symbols_bronze',
    project_id='project-2eef9167-38f3-4223-ac1',
    region='us-central1',
    job={
        'placement': {'cluster_name': 'my-data-cluster'},
        'pyspark_job': {
            'main_python_file_uri': 'gs://fintech-trading-lakehouse-gds1/spark_jobs/bronze/ingest_symbols_bronze.py'
        }
    },
    dag=dag,
)

# ============================================================================
# SILVER LAYER TASKS
# ============================================================================

# Transform BigQuery trades to Silver with enrichment
transform_to_silver = DataprocSubmitJobOperator(
    task_id='transform_to_silver',
    project_id='project-2eef9167-38f3-4223-ac1',
    region='us-central1',
    job={
        'placement': {'cluster_name': 'my-data-cluster'},
        'pyspark_job': {
            'main_python_file_uri': 'gs://fintech-trading-lakehouse-gds1/spark_jobs/silver/transform_silver.py'
        }
    },
    dag=dag,
)

# ============================================================================
# GOLD LAYER TASKS (Run in parallel)
# ============================================================================

# Aggregate PnL metrics
aggregate_pnl = DataprocSubmitJobOperator(
    task_id='aggregate_pnl',
    project_id='project-2eef9167-38f3-4223-ac1',
    region='us-central1',
    job={
        'placement': {'cluster_name': 'my-data-cluster'},
        'pyspark_job': {
            'main_python_file_uri': 'gs://fintech-trading-lakehouse-gds1/spark_jobs/gold/aggregate_pnl.py'
        }
    },
    dag=dag,
)

# Aggregate liquidity metrics
aggregate_liquidity = DataprocSubmitJobOperator(
    task_id='aggregate_liquidity',
    project_id='project-2eef9167-38f3-4223-ac1',
    region='us-central1',
    job={
        'placement': {'cluster_name': 'my-data-cluster'},
        'pyspark_job': {
            'main_python_file_uri': 'gs://fintech-trading-lakehouse-gds1/spark_jobs/gold/aggregate_liquidity.py'
        }
    },
    dag=dag,
)

# Aggregate user activity metrics
aggregate_user_activity = DataprocSubmitJobOperator(
    task_id='aggregate_user_activity',
    project_id='project-2eef9167-38f3-4223-ac1',
    region='us-central1',
    job={
        'placement': {'cluster_name': 'my-data-cluster'},
        'pyspark_job': {
            'main_python_file_uri': 'gs://fintech-trading-lakehouse-gds1/spark_jobs/gold/aggregate_user_activity.py'
        }
    },
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Bronze layer: Check file → Ingest FX → Ingest Symbols (parallel)
check_fx_rates_file >> ingest_fx_rates_bronze
[ingest_fx_rates_bronze, ingest_symbols_bronze] >> transform_to_silver

# Silver layer: Transform trades (depends on both Bronze tasks)
transform_to_silver >> [aggregate_pnl, aggregate_liquidity, aggregate_user_activity]

# Gold layer: All aggregations run in parallel after Silver completes
