from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'customer360_orchestrated_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    ingest_db_task = DatabricksSubmitRunOperator(
        task_id='ingest_postgres_customers',
        databricks_conn_id='databricks_default',
        spark_python_task={
            'python_file': 'dbfs:/pipelines/customer360/db_ingest.py',
            'parameters': ['--table', 'customers', '--batch-date', '{{ ds }}']
        }
    )

    feature_store_task = DatabricksSubmitRunOperator(
        task_id='generate_customer_features',
        databricks_conn_id='databricks_default',
        spark_python_task={
            'python_file': 'dbfs:/pipelines/customer360/feature_store.py',
            'parameters': ['--gcs-target', 'gs://bh-demo-app-bucket/test-scenarios/python-customer360/delta/features']
        }
    )

    ingest_db_task >> feature_store_task
