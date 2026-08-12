# Databricks notebook source
from pyspark.sql.functions import *

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

if environment == "dev":
    storage_account = "pptrainingsa"
    container = "myntra-clickstream"

    catalog = "myntra_de"
elif environment == "prod":
    storage_account = "pptrainingsaprod"
    container = "myntra-clickstream-prod"

    catalog = "myntra_de_prod"
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