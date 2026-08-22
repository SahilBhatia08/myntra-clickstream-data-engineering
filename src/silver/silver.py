# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import StorageLevel
from delta.tables import DeltaTable
from datetime import datetime
import traceback
import uuid

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

spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.silver.clickstream_events (
    silver_event_key STRING,
    source_event_id STRING,
    user_id STRING,
    session_id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP,
    event_date DATE,

    product_id STRING,
    product_name STRING,
    brand STRING,
    category STRING,
    price BIGINT,

    search_query STRING,
    device_type STRING,
    city STRING,
    page_name STRING,

    source_file STRING,
    bronze_commit_version BIGINT,
    bronze_commit_timestamp TIMESTAMP,
    silver_processed_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date)
TBLPROPERTIES (
    delta.enableChangeDataFeed = true
)""")

# COMMAND ----------

# DBTITLE 1,Cell 2

bronze_table = f"{catalog}.bronze.clickstream"

silver_table = f"{catalog}.silver.clickstream_events"

control_table = f"{catalog}.metadata.pipeline_control"

quarantine_table = f"{catalog}.metadata.clickstream_quarantine"

audit_table = f"{catalog}.metadata.pipeline_audit"

pipeline_name = "silver_cdf_pipeline"

pipeline_layer = "SILVER"

audit_id = str(uuid.uuid4())

run_start_timestamp = datetime.utcnow()

control_df = spark.table(control_table).filter(F.col("pipeline_name") == pipeline_name)

last_processed_version = (
    0
    if control_df.count() == 0
    else control_df.select("last_processed_version").first()[0]
)

starting_version = 0 if last_processed_version == 0 else last_processed_version + 1

latest_bronze_version = (
    spark.sql(f"describe history {bronze_table} limit 1")
    .select("version")
    .first()["version"]
)
silver_start_version = spark.sql(f"describe history {silver_table} limit 1").collect()[
    0
]["version"]
print(f"Bronze last processed version: " f"{last_processed_version}")

print(f"Latest bronze version: {latest_bronze_version}")

print(f"Starting CDF version: " f"{starting_version}")

print(f"Current silver version: {silver_start_version}")

print(f"Bronze table: {bronze_table}")
print(f"Silver table: {silver_table}")
print(f"Pipeline RunId: {run_id}")

# COMMAND ----------

def write_pipeline_audit(run_status, run_end_timestamp, records_read, valid_records, invalid_records, silver_written, target_start_version, target_end_version,  error_message=None):

    audit_data = [
        (
            audit_id,
            pipeline_name,
            pipeline_layer,
            run_id,
            bronze_table,
            silver_table,
            starting_version,
            latest_bronze_version,
            records_read,
            valid_records,
            invalid_records,
            silver_written,
            run_start_timestamp,
            run_end_timestamp,
            run_status,
            error_message,
            target_start_version,
            target_end_version
        )
    ]

    audit_columns = [
        "audit_id",
        "pipeline_name",
        "pipeline_layer",
        "run_id",
        "source_table",
        "target_table",
        "source_start_version",
        "source_end_version",
        "records_read",
        "valid_records",
        "invalid_records",
        "records_written",
        "run_start_timestamp",
        "run_end_timestamp",
        "run_status",
        "error_message",
        "target_start_version",
        "target_end_version"
    ]

    audit_df = spark.createDataFrame(audit_data, audit_columns).withColumn(
        "created_timestamp", F.current_timestamp()
    )

    (audit_df.write.format("delta").mode("append").saveAsTable(audit_table))

# COMMAND ----------

# DBTITLE 1,Cell 4
try:
    if starting_version > latest_bronze_version:
        print("if running")
        records_read = 0
        valid_records = 0
        invalid_records = 0
        silver_written = 0
        run_end_timestamp = datetime.utcnow()
        silver_end_version = spark.sql(
            f"describe history {silver_table} limit 1"
        ).collect()[0]["version"]

        write_pipeline_audit(
            run_status="NO_DATA",
            records_read=records_read,
            valid_records=valid_records,
            invalid_records=invalid_records,
            silver_written=silver_written,
            run_end_timestamp=(run_end_timestamp),
            error_message="",
            target_start_version=silver_start_version,
            target_end_version=silver_end_version,
        )

        print("No new Bronze changes to process.")

    else:
        print("else running")
        bronze_cdf_df = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", starting_version)
            .table(bronze_table)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        records_read = bronze_cdf_df.count()

        new_events_df = bronze_cdf_df.filter(F.col("_change_type") == "insert")

        valid_event_types = [
            "APP_OPEN",
            "SEARCH",
            "FILTER_APPLIED",
            "LISTING_IMPRESSION",
            "PRODUCT_VIEW",
            "WISHLIST",
            "ADD_TO_CART",
            "REMOVE_FROM_CART",
            "CHECKOUT",
            "PURCHASE",
        ]

        standardized_input_df = (
            new_events_df.withColumn(
                "event_type_standardized", F.upper(F.trim(F.col("event_type")))
            )
            .withColumn("product_id_standardized", F.trim(F.col("product_id")))
            .withColumn("event_date", F.to_date(F.col("event_timestamp")))
        )

        validation_df = standardized_input_df.withColumn(
            "validation_errors",
            F.array_remove(
                F.array(
                    F.when(
                        F.col("event_id").isNull()
                        | (F.length(F.trim(F.col("event_id"))) == 0),
                        F.lit("EVENT_ID_MISSING"),
                    ),
                    F.when(
                        F.col("user_id").isNull()
                        | (F.length(F.trim(F.col("user_id"))) == 0),
                        F.lit("USER_ID_MISSING"),
                    ),
                    F.when(
                        F.col("session_id").isNull()
                        | (F.length(F.trim(F.col("session_id"))) == 0),
                        F.lit("SESSION_ID_MISSING"),
                    ),
                    F.when(
                        F.col("event_type_standardized").isNull()
                        | (F.length(F.col("event_type_standardized")) == 0),
                        F.lit("EVENT_TYPE_MISSING"),
                    ),
                    F.when(
                        ~F.col("event_type_standardized").isin(valid_event_types),
                        F.lit("INVALID_EVENT_TYPE"),
                    ),
                    F.when(
                        F.col("event_timestamp").isNull(),
                        F.lit("EVENT_TIMESTAMP_MISSING"),
                    ),
                    F.when(
                        F.col("event_timestamp") > F.current_timestamp(),
                        F.lit("EVENT_TIMESTAMP_IN_FUTURE"),
                    ),
                    F.when(
                        F.col("price").isNotNull() & (F.col("price") < 0),
                        F.lit("NEGATIVE_PRICE"),
                    ),
                    F.when(
                        F.col("event_type_standardized").isin(
                            "PRODUCT_VIEW",
                            "WISHLIST",
                            "ADD_TO_CART",
                            "REMOVE_FROM_CART",
                            "CHECKOUT",
                            "PURCHASE",
                        )
                        & (
                            F.col("product_id_standardized").isNull()
                            | (F.length(F.col("product_id_standardized")) == 0)
                        ),
                        F.lit("PRODUCT_ID_MISSING"),
                    ),
                    F.when(
                        (F.col("event_type_standardized") == "LISTING_IMPRESSION")
                        & (
                            F.col("visible_products").isNull()
                            | (F.size(F.col("visible_products")) == 0)
                        ),
                        F.lit("VISIBLE_PRODUCTS_MISSING"),
                    ),
                ),
                F.lit(None),
            ),
        )

        validated_df = validation_df.withColumn(
            "is_valid",
            (F.size(F.col("validation_errors")) == 0)
            | (F.col("validation_errors").isNull()),
        ).persist(StorageLevel.MEMORY_AND_DISK)

        valid_events_df = validated_df.filter(F.col("is_valid"))

        invalid_events_df = validated_df.filter(~F.col("is_valid"))

        valid_records = valid_events_df.count()

        invalid_records = invalid_events_df.count()

        quarantine_df = invalid_events_df.select(
            "event_id",
            "user_id",
            "session_id",
            F.col("event_type_standardized").alias("event_type"),
            "event_timestamp",
            F.col("product_id_standardized").alias("product_id"),
            "product_name",
            "brand",
            "category",
            "price",
            "search_query",
            "device_type",
            "city",
            "visible_products",
            "page_name",
            "source_file",
            F.col("_commit_version").alias("bronze_commit_version"),
            F.col("_commit_timestamp").alias("bronze_commit_timestamp"),
            F.lit("QUARANTINED").alias("validation_status"),
            "validation_errors",
            F.lit(pipeline_name).alias("pipeline_name"),
            F.lit(run_id).alias("run_id"),
            F.current_timestamp().alias("quarantined_timestamp"),
            F.concat_ws(
                "_",
                F.coalesce(F.col("event_id"), F.lit("NO_EVENT_ID")),
                F.col("bronze_commit_version"),
            ).alias("quarantine_record_key"),
        )

        quarantine_delta = DeltaTable.forName(spark, quarantine_table)

        (
            quarantine_delta.alias("t")
            .merge(
                quarantine_df.alias("s"),
                "t.quarantine_record_key == s.quarantine_record_key",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        product_level_df = valid_events_df.withColumn(
            "output_product_id", F.explode_outer(F.col("visible_products"))
        ).withColumn(
            "output_product_id",
            F.coalesce(F.col("output_product_id"), F.col("product_id_standardized")),
        )

        silver_df = product_level_df.withColumn(
            "silver_event_key",
            F.concat_ws(
                "-",
                F.col("event_id"),
                F.coalesce(F.col("output_product_id"), F.lit("No_Product")),
            ),
        ).select(
            "silver_event_key",
            F.col("event_id").alias("source_event_id"),
            "user_id",
            "session_id",
            "event_type",
            "event_timestamp",
            "event_date",
            F.col("output_product_id").alias("product_id"),
            "product_name",
            "brand",
            "category",
            "price",
            "search_query",
            "device_type",
            "city",
            "page_name",
            "source_file",
            F.col("_commit_version").alias("bronze_commit_version"),
            F.col("_commit_timestamp").alias("bronze_commit_timestamp"),
            F.current_timestamp().alias("silver_processed_timestamp"),
        )

        silver_df.explain("formatted")

        silver_dedup_df = silver_df.dropDuplicates(["silver_event_key"])

        silver_dedup_df.explain("formatted")

        silver_written = silver_dedup_df.count()

        if spark.catalog.tableExists(silver_table):
            silver_delta = DeltaTable.forName(spark, silver_table)

            (
                silver_delta.alias("t")
                .merge(
                    silver_dedup_df.alias("s"),
                    "t.silver_event_key = s.silver_event_key",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            silver_dedup_df.write.format("delta").mode("overwrite").saveAsTable(
                silver_table
            )

        silver_end_version = spark.sql(
            f"describe history {silver_table} limit 1"
        ).collect()[0]["version"]

        control_schema = StructType(
            [
                StructField("pipeline_name", StringType(), True),
                StructField("source_table", StringType(), True),
                StructField("last_processed_version", LongType(), False),
                StructField("last_run_status", StringType(), True),
                StructField("last_run_id", StringType(), True),
            ]
        )

        control_delta = DeltaTable.forName(spark, control_table)

        (
            control_delta.alias("target")
            .merge(
                spark.createDataFrame(
                    [
                        (
                            pipeline_name,
                            bronze_table,
                            latest_bronze_version,
                            "SUCCESS",
                            run_id,
                        )
                    ],
                    control_schema,
                ).alias("source"),
                """
                target.pipeline_name
                =
                source.pipeline_name
                """,
            )
            .whenMatchedUpdate(
                set={
                    "source_table": "source.source_table",
                    "last_processed_version": "source.last_processed_version",
                    "last_processed_timestamp": "current_timestamp()",
                    "last_run_status": "source.last_run_status",
                    "last_run_id": "source.last_run_id",
                    "updated_timestamp": "current_timestamp()",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "pipeline_name": "source.pipeline_name",
                    "source_table": "source.source_table",
                    "last_processed_version": "source.last_processed_version",
                    "last_processed_timestamp": "current_timestamp()",
                    "last_run_status": "source.last_run_status",
                    "last_run_id": "source.last_run_id",
                    "updated_timestamp": "current_timestamp()",
                }
            )
            .execute()
        )

        run_end_timestamp = datetime.utcnow()

        write_pipeline_audit(
            run_status="SUCCESS",
            run_end_timestamp=(run_end_timestamp),
            records_read=records_read,
            valid_records=valid_records,
            invalid_records=invalid_records,
            silver_written=silver_written,
            error_message="",
            target_start_version=silver_start_version,
            target_end_version=silver_end_version,
        )

        silver_dedup_df.unpersist()
        validated_df.unpersist()
        bronze_cdf_df.unpersist()

        print("Silver pipeline " "completed successfully.")

except Exception as error:

    run_end_timestamp = datetime.utcnow()

    error_message = traceback.format_exc()

    silver_end_version = spark.sql(
        f"describe history {silver_table} limit 1"
    ).collect()[0]["version"]

    write_pipeline_audit(
        run_status="FAILED",
        run_end_timestamp=(run_end_timestamp),
        records_read=0,
        valid_records=0,
        invalid_records=0,
        silver_written=0,
        error_message=(error_message),
        target_start_version=silver_start_version,
        target_end_version=silver_end_version,
    )

    print("Silver pipeline failed.")

    raise error

# COMMAND ----------

display(spark.sql(f"describe history {silver_table} limit 1"))

# COMMAND ----------

display(spark.sql(f"select * from {catalog}.metadata.pipeline_audit"))