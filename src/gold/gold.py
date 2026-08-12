# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import traceback
from datetime import datetime
import uuid

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

spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.gold.product_performance_metrics (
    event_date DATE,
    product_id STRING,

    impression_count BIGINT,
    product_view_count BIGINT,
    add_to_cart_count BIGINT,
    purchase_count BIGINT,

    unique_users BIGINT,

    ctr DOUBLE,
    add_to_cart_rate DOUBLE,
    conversion_rate DOUBLE,

    gold_processed_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date)
TBLPROPERTIES (
    delta.enableChangeDataFeed = true
)""")

# COMMAND ----------

silver_table = f"{catalog}.silver.clickstream_events"

gold_table = f"{catalog}.gold.product_performance_metrics"

control_table = f"{catalog}.metadata.pipeline_control"

audit_table = f"{catalog}.metadata.pipeline_audit"

pipeline_name = "gold_cdf_pipeline"

pipeline_layer = "GOLD"

audit_id = str(uuid.uuid4())

run_start_timestamp = datetime.utcnow()

control_df = spark.read.table(control_table).filter(
    F.col("pipeline_name") == pipeline_name
)

last_processed_version = (
    0
    if control_df.count() == 0
    else control_df.select("last_processed_version").first()[0]
)

starting_version = 0 if last_processed_version == 0 else last_processed_version + 1

latest_silver_version = (
    spark.sql(f"describe history {silver_table} limit 1")
    .select("version")
    .first()["version"]
)

gold_start_version = spark.sql(f"describe history {gold_table} limit 1").collect()[0][
    "version"
]

print(f"Last processed silver version: " f"{last_processed_version}")

print(f"Latest silver version: {latest_silver_version}")

print(f"Starting CDF version: " f"{starting_version}")

print(f"Gold current version: {gold_start_version}")

# COMMAND ----------

def write_pipeline_audit(
    run_status,
    run_end_timestamp,
    records_read,
    gold_written,
    target_start_version,
    target_end_version,
    error_message=None,
):

    audit_data = [
        (
            audit_id,
            pipeline_name,
            pipeline_layer,
            run_id,
            silver_table,
            gold_table,
            starting_version,
            latest_silver_version,
            records_read,
            gold_written,
            run_start_timestamp,
            run_end_timestamp,
            run_status,
            error_message,
            target_start_version,
            target_end_version
        )
    ]

    audit_columns = StructType(
        [
            StructField("audit_id", StringType(), True),
            StructField("pipeline_name", StringType(), True),
            StructField("pipeline_layer", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("source_start_version", LongType(), True),
            StructField("source_end_version", LongType(), True),
            StructField("records_read", LongType(), True),
            StructField("records_written", LongType(), True),
            StructField("run_start_timestamp", TimestampType(), True),
            StructField("run_end_timestamp", TimestampType(), True),
            StructField("run_status", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("target_start_version", LongType(), True),
            StructField("target_end_version", LongType(), True)
        ]
    )

    audit_df = spark.createDataFrame(audit_data, audit_columns).withColumn(
        "created_timestamp", F.current_timestamp()
    )

    (audit_df.write.format("delta").mode("append").saveAsTable(audit_table))

# COMMAND ----------

try:
    if starting_version > latest_silver_version:
        print("if running")
        records_read = 0
        gold_written = 0
        run_end_timestamp = datetime.utcnow()
        gold_end_version = spark.sql(f"describe history {gold_table} limit 1").collect()[0]["version"]

        write_pipeline_audit(
            run_status="NO_DATA",
            run_end_timestamp=run_end_timestamp,
            records_read=records_read,
            gold_written=gold_written,
            error_message=None,
            target_start_version=gold_start_version,
            target_end_version=gold_end_version
        )

        print("No new Bronze changes to process.")
    else:
        print("else running")
        silver_cdf_df = (
            spark.read.format("delta")
            .option("readChangeFeed", True)
            .option("startingVersion", starting_version)
            .table(silver_table)
        )

        records_read = silver_cdf_df.count()

        new_silver_events = (
            silver_cdf_df
                .filter(
                    F.col("_change_type") == "insert"
                )
                .filter(
                    F.col("product_id").isNotNull()
                )
        )

        incremental_metrics = new_silver_events.groupBy("event_date", "product_id").agg(
            F.sum(F.when(F.col("event_type") == "LISTING_IMPRESSION", 1).otherwise(0))
            .cast("long")
            .alias("impression_count"),
            F.sum(F.when(F.col("event_type") == "PRODUCT_VIEW", 1).otherwise(0))
            .cast("long")
            .alias("product_view_count"),
            F.sum(F.when(F.col("event_type") == "ADD_TO_CART", 1).otherwise(0))
            .cast("long")
            .alias("add_to_cart_count"),
            F.sum(F.when(F.col("event_type") == "PURCHASE", 1).otherwise(0))
            .cast("long")
            .alias("purchase_count"),
            F.countDistinct("user_id").cast("long").alias("incremental_unique_users"),
        )

        gold_written = incremental_metrics.select("event_date", "product_id").distinct().count()

        gold_delta = DeltaTable.forName(spark, gold_table)

        (
            gold_delta.alias("target")
            .merge(
                incremental_metrics.alias("source"),
                """
                target.event_date = source.event_date
                AND target.product_id = source.product_id
                """,
            )
            .whenMatchedUpdate(
                set={
                    "impression_count": "target.impression_count + source.impression_count",
                    "product_view_count": "target.product_view_count + source.product_view_count",
                    "add_to_cart_count": "target.add_to_cart_count + source.add_to_cart_count",
                    "purchase_count": "target.purchase_count + source.purchase_count",
                    "gold_processed_timestamp": ("current_timestamp()"),
                }
            )
            .whenNotMatchedInsert(
                values={
                    "event_date": "source.event_date",
                    "product_id": "source.product_id",
                    "impression_count": "source.impression_count",
                    "product_view_count": "source.product_view_count",
                    "add_to_cart_count": "source.add_to_cart_count",
                    "purchase_count": "source.purchase_count",
                    "unique_users": "source.incremental_unique_users",
                    "ctr": "0.0",
                    "add_to_cart_rate": "0.0",
                    "conversion_rate": "0.0",
                    "gold_processed_timestamp": "current_timestamp()",
                }
            )
            .execute()
        )

        unique_users_df = (
            spark.table(silver_table)
            .filter(
                (F.col("product_id").isNotNull())
                # & (
                #     (F.col("event_date") == F.to_date(F.current_timestamp()))
                #     | (F.col("event_date") == F.to_date(F.current_timestamp()) - 1)
                # )
            )
            .groupBy("event_date", "product_id")
            .agg(F.countDistinct("user_id").alias("unique_users"))
        )

        gold_delta_update = DeltaTable.forName(spark, gold_table)

        (
            gold_delta_update.alias("t")
            .merge(
                unique_users_df.alias("s"),
                "t.product_id = s.product_id and t.event_date = s.event_date",
            )
            .whenMatchedUpdate(
                set={
                    "unique_users": "s.unique_users",
                    "ctr": "CASE WHEN t.impression_count > 0 THEN ROUND(t.product_view_count / t.impression_count, 2) ELSE 0 END",
                    "add_to_cart_rate": "CASE WHEN t.product_view_count > 0 THEN ROUND(t.add_to_cart_count / t.product_view_count, 2) ELSE 0 END",
                    "conversion_rate": "CASE WHEN t.add_to_cart_count > 0 THEN ROUND(t.purchase_count / t.add_to_cart_count, 2) ELSE 0 END",
                    "gold_processed_timestamp": "current_timestamp()",
                }
            )
            .execute()
        )

        gold_end_version = spark.sql(f"describe history {gold_table} limit 1").collect()[0]["version"]

        control_schema = StructType([
            StructField("pipeline_name", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("last_processed_version", LongType(), False),
            StructField("last_run_status", StringType(), True),
            StructField("last_run_id", StringType(), True),
        ])

        control_delta = DeltaTable.forName(spark, control_table)

        control_delta.alias("t").merge(
            spark.createDataFrame(
                [(pipeline_name, silver_table, latest_silver_version, "SUCCESS", str(run_id))],
                control_schema,
            ).alias("source"),
            "t.pipeline_name = source.pipeline_name",
        ).whenMatchedUpdate(
            set={
                "source_table": "source.source_table",
                "last_processed_version": "source.last_processed_version",
                "last_processed_timestamp": "current_timestamp()",
                "last_run_status": "source.last_run_status",
                "last_run_id": "source.last_run_id",
                "updated_timestamp": "current_timestamp()",
            }
        ).whenNotMatchedInsert(
            values={
                "pipeline_name": "source.pipeline_name",
                "source_table": "source.source_table",
                "last_processed_version": "source.last_processed_version",
                "last_processed_timestamp": "current_timestamp()",
                "last_run_status": "source.last_run_status",
                "last_run_id": "source.last_run_id",
                "updated_timestamp": "current_timestamp()",
            }
        ).execute()

        run_end_timestamp = datetime.utcnow()

        write_pipeline_audit(
            run_status="SUCCESS",
            run_end_timestamp=(run_end_timestamp),
            records_read=records_read,
            gold_written=gold_written,
            error_message="",
            target_start_version=gold_start_version,
            target_end_version=gold_end_version
        )

        print("Gold pipeline " "completed successfully.")
except Exception as error:
    run_end_timestamp = datetime.utcnow()

    error_message = traceback.format_exc()

    gold_end_version = spark.sql(f"describe history {gold_table} limit 1").collect()[0]["version"]

    write_pipeline_audit(
        run_status="FAILED",
        run_end_timestamp=(run_end_timestamp),
        records_read=0,
        gold_written=0,
        error_message=(error_message),
        target_start_version=gold_start_version,
        target_end_version=gold_end_version
    )

    print("Gold pipeline failed.")

    raise error

# COMMAND ----------

display(spark.sql(f"describe history {gold_table} limit 1"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from myntra_de.gold.product_performance_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from myntra_de.metadata.pipeline_audit;
# MAGIC
# MAGIC -- select * from myntra_de.metadata.pipeline_control;

# COMMAND ----------

# MAGIC %sql
# MAGIC update myntra_de.metadata.pipeline_control set last_processed_version = 10 where pipeline_name = "gold_cdf_pipeline"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from myntra_de.gold.product_performance_metrics

# COMMAND ----------

