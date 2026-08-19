# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("pipeline_run_id", f"manual_run {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
environment = dbutils.widgets.get("environment")
run_id = dbutils.widgets.get("pipeline_run_id")

if environment == "dev":
    storage_account = "pptrainingsa"
    container = "myntra-clickstream"

    catalog = "myntra_clickstream_de"
elif environment == "prod":
    storage_account = "pptrainingsaprod"
    container = "myntra-clickstream-prod"

    catalog = "myntra_clickstream_de_prod"
else:
    raise ValueError(f"Unsupported Environment {environment}")

# COMMAND ----------

display(spark.read.table(f"{catalog}.gold.product_performance_metrics"))

# COMMAND ----------

display(spark.read.table(f"{catalog}.gold.product_performance_metrics").filter(col("product_id").isNull()))

# COMMAND ----------

display(
    spark.read.table(f"{catalog}.gold.product_performance_metrics").filter(
        col("purchase_count") > col("product_view_count")
    )
)