# Databricks notebook source
from pyspark.sql import functions as F
import uuid
from datetime import datetime
import traceback
from pyspark.sql.types import *

# pipeline control
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("pipeline_run_id", f"manual_run {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
environment = dbutils.widgets.get("environment")
run_id = dbutils.widgets.get("pipeline_run_id")


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

# DBTITLE 1,Create bronze clickstream table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.clickstream (
    event_id STRING,
    user_id STRING,
    session_id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP,
    product_id STRING,
    product_name STRING,
    brand STRING,
    category STRING,
    price BIGINT,
    search_query STRING,
    device_type STRING,
    city STRING,
    visible_products ARRAY<STRING>,
    page_name STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING,
    ingestion_date DATE
)
USING DELTA
TBLPROPERTIES (
    delta.enableChangeDataFeed = true
)
""")
print(f"Table '{catalog}.bronze.clickstream' created (or already exists).")

# COMMAND ----------

audit_table = f"{catalog}.metadata.pipeline_audit"
bronze_table = f"{catalog}.bronze.clickstream"

base_path = f"abfss://{container}@" f"{storage_account}.dfs.core.windows.net"

landing_path = f"{base_path}/landing/clickstream"

schema_path = f"{base_path}/autoloader/schema/clickstream"

bronze_checkpoint = f"{base_path}/autoloader/checkpoint/bronze_clickstream"

bronze_start_version = spark.sql(f"Describe history {bronze_table} limit 1").collect()[0]["version"]

audit_id = str(uuid.uuid4())

pipeline_name = "bronze_autoloader_pipeline"

pipeline_layer = "BRONZE"

run_start_timestamp = datetime.utcnow()

print(f"Bronze start version: {bronze_start_version}")

# COMMAND ----------

def write_pipeline_audit(
    run_status,
    run_end_timestamp,
    records_read,
    records_written,
    files_processed,
    batch_id,
    checkpoint_path,
    target_start_version,
    target_end_version,
    error_message=None,
):

    audit_schema = StructType(
        [
            StructField("audit_id", StringType(), True),
            StructField("pipeline_name", StringType(), True),
            StructField("pipeline_layer", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("files_processed", LongType(), True),
            StructField("batch_id", LongType(), True),
            StructField("records_read", LongType(), True),
            StructField("records_written", LongType(), True),
            StructField("checkpoint_path", StringType(), True),
            StructField("run_start_timestamp", TimestampType(), True),
            StructField("run_end_timestamp", TimestampType(), True),
            StructField("run_status", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("target_start_version", LongType(), True),
            StructField("target_end_version", LongType(), True)
        ]
    )

    audit_data = [
        (
            audit_id,
            pipeline_name,
            pipeline_layer,
            run_id,
            landing_path,
            bronze_table,
            int(files_processed) if files_processed is not None else None,
            int(batch_id) if batch_id is not None else None,
            int(records_read) if records_read is not None else None,
            int(records_written) if records_written is not None else None,
            checkpoint_path,
            run_start_timestamp,
            run_end_timestamp,
            run_status,
            error_message,
            target_start_version,
            target_end_version
        )
    ]

    audit_df = spark.createDataFrame(audit_data, audit_schema).withColumn(
        "created_timestamp", F.current_timestamp()
    )

    (audit_df.write.format("delta").mode("append").saveAsTable(audit_table))

# COMMAND ----------

def process_bronze_batch(batch_df, batch_id):
    batch_start = datetime.utcnow()

    files_processed = batch_df.select("source_file").distinct().count()

    records_read = batch_df.count()

    (
        batch_df.write.format("delta")
        .mode("append")
        .saveAsTable("myntra_de.bronze.clickstream")
    )

    records_written = records_read

    bronze_end_version = spark.sql(
        f"Describe history {bronze_table} limit 1"
    ).collect()[0]["version"]

    batch_end = datetime.utcnow()

    write_pipeline_audit(
        run_status="SUCCESS",
        run_end_timestamp=batch_end,
        records_read=records_read,
        records_written=records_written,
        files_processed=files_processed,
        batch_id=batch_id,
        checkpoint_path=bronze_checkpoint,
        target_start_version=bronze_start_version,
        target_end_version=bronze_end_version,
    )

# COMMAND ----------

try:
    bronze_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(landing_path)
    )

    bronze_df = (
        bronze_stream.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_date", F.current_date())
        .withColumn(
            "visible_products", F.from_json(F.col("visible_products"), "array<string>")
        )
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("price", F.col("price").cast("bigint"))
        .select(
            "event_id",
            "user_id",
            "session_id",
            "event_type",
            "event_timestamp",
            "product_id",
            "product_name",
            "brand",
            "category",
            "price",
            "search_query",
            "device_type",
            "city",
            "visible_products",
            "page_name",
            "ingestion_timestamp",
            "source_file",
            "ingestion_date",
        )
    )
    bronze_query = (
        bronze_df.writeStream.foreachBatch(process_bronze_batch)
        .option("checkpointLocation", bronze_checkpoint)
        .trigger(availableNow=True)
        .start()
    )
except Exception as error:

    run_end_timestamp = datetime.utcnow()

    error_message = traceback.format_exc()

    bronze_end_version = spark.sql(
        f"Describe history {bronze_table} limit 1"
    ).collect()[0]["version"]

    write_pipeline_audit(
        run_status="FAILED",
        run_end_timestamp=(run_end_timestamp),
        error_message=(error_message),
        target_start_version=bronze_start_version,
        target_end_version=bronze_end_version,
    )

    print("Bronze pipeline failed.")

    raise error

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as total_rows from myntra_de.bronze.clickstream

# COMMAND ----------

display(spark.sql(f"describe history {bronze_table} limit 1").collect()[0]["version"]);
display(spark.read.table("myntra_de.metadata.pipeline_audit"))

# COMMAND ----------

display(spark.sql("describe history myntra_de.silver.clickstream_events"));

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from myntra_de.metadata.pipeline_control;