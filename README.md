# Python + Orchestration Workload (`test-scenario-python-orchestrated`)

Multi-source Python ETL and Orchestration testing:
* Partitioned JDBC database reading
* Spark ↔ PyArrow / Polars feature engineering
* Databricks Asset Bundles (`databricks.yml`)
* Airflow DAGs (`DatabricksSubmitRunOperator`)
* Target GCS Delta writes to `gs://bh-demo-app-bucket/test-scenarios/python-customer360/delta/`
