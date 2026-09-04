# Databricks notebook source
# MAGIC %md
# MAGIC # Python + Spark Customer 360 Feature Store Pipeline

# COMMAND ----------

import json
from pyspark.sql import functions as F
from delta.tables import DeltaTable

gcs_root = "gs://bh-demo-app-bucket/test-scenarios/python-customer360"
staging_path = f"{gcs_root}/staging/customers_jdbc"
feature_output_path = f"{gcs_root}/delta/features_customer360"

print(f"Running Customer 360 Feature Pipeline on GCS: {gcs_root}")

# COMMAND ----------

# 1. Ingest Staging Customer Profile Data
try:
    cust_df = spark.read.json(staging_path)
except Exception:
    print("Generating staging customer extract...")
    sample_customers = [
        (f"CUST-{i:04d}", f"Customer_{i}", 20 + (i % 50), "GOLD" if i%5==0 else "SILVER" if i%3==0 else "BRONZE", 1000.0 * (i % 10) + 250.0, i % 15)
        for i in range(1, 201)
    ]
    cust_df = spark.createDataFrame(sample_customers, ["customer_id", "full_name", "age", "tier", "annual_spend", "login_frequency_30d"])
    cust_df.write.mode("overwrite").json(staging_path)

# COMMAND ----------

# 2. Feature Engineering & Tier Scoring
features_df = cust_df \
    .withColumn("engagement_score", F.round(F.col("login_frequency_30d") * 0.4 + (F.col("annual_spend") / 1000.0) * 0.6, 2)) \
    .withColumn("churn_risk_flag", F.when((F.col("login_frequency_30d") < 2) & (F.col("annual_spend") < 500.0), F.lit(True)).otherwise(F.lit(False))) \
    .withColumn("feature_timestamp", F.current_timestamp())

feature_count = features_df.count()
print(f"Generated {feature_count} customer feature records.")

# COMMAND ----------

# 3. Write to Gold Delta Store on GCS
features_df.write.format("delta").mode("overwrite").partitionBy("tier").save(feature_output_path)
print(f"Persisted feature store to: {feature_output_path}")

# COMMAND ----------

exit_payload = {
    "status": "SUCCESS",
    "features_generated": feature_count,
    "feature_store_path": feature_output_path
}
dbutils.notebook.exit(json.dumps(exit_payload))
