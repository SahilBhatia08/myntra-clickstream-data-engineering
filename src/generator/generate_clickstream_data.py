# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# ============================================================
# ENVIRONMENT
# ============================================================

dbutils.widgets.text("environment", "dev")
environment = dbutils.widgets.get("environment")

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


# ============================================================
# PATHS
# ============================================================

base_path = (
    f"abfss://{container}@"
    f"{storage_account}.dfs.core.windows.net"
)

landing_path = f"{base_path}/landing/clickstream"

bronze_table = f"{catalog}.bronze.clickstream"


# ============================================================
# LOAD TEST CONFIGURATION
# ============================================================

# Total number of events for this test
number_of_events = 500_000

# Number of simulated traffic batches
number_of_batches = 20

# Simulated production traffic window
traffic_window_minutes = 20

# Number of output partitions/files
number_of_partitions = 20

# Number of products
number_of_products = 100_000


if number_of_events % number_of_batches != 0:
    raise ValueError(
        "number_of_events must be divisible by number_of_batches"
    )


events_per_batch = (
    number_of_events // number_of_batches
)


# ============================================================
# EXISTING EVENT OFFSET
# ============================================================

event_start = (
    spark.read.table(bronze_table).count()
    if spark.catalog.tableExists(bronze_table)
    else 0
)


print("=" * 60)
print("LOAD TEST CONFIGURATION")
print("=" * 60)

print(f"Environment              : {environment}")
print(f"Total events             : {number_of_events:,}")
print(f"Number of batches        : {number_of_batches:,}")
print(f"Events per batch         : {events_per_batch:,}")
print(f"Traffic window           : {traffic_window_minutes} minutes")
print(f"Output partitions        : {number_of_partitions}")
print(f"Existing event offset    : {event_start:,}")
print(f"Landing path             : {landing_path}")

print("=" * 60)

# COMMAND ----------

# from pyspark.sql import functions as F
# from pyspark.sql.types import *
# from datetime import datetime, timedelta
# import random

# dbutils.widgets.text("environment", "dev")
# environment = dbutils.widgets.get("environment")

# if environment == "dev":
#     storage_account = "pptrainingsa"
#     container = "myntra-clickstream"

#     catalog = "myntra_clickstream_de"
# elif environment == "prod":
#     storage_account = "pptrainingsaprod"
#     container = "myntra-clickstream-prod"

#     catalog = "myntra_clickstream_de_prod"
# else:
#     raise ValueError(f"Unsupported Environment {environment}")

# COMMAND ----------

# base_path = (
#     f"abfss://{container}@"
#     f"{storage_account}.dfs.core.windows.net"
# )

# landing_path = f"{base_path}/landing/clickstream"

# # Initial functional test volume
# number_of_events = 100000

# # Number of output files
# number_of_partitions = 20


# bronze_table = f"{catalog}.bronze.clickstream"

# print(f"Landing path: {landing_path}")
# print(f"Bronze Table Location: {bronze_table}")
# print(f"Events to generate: {number_of_events:,}")

# COMMAND ----------

# Number of products in the synthetic marketplace
number_of_products = 100_000

brands = [
    "Roadster",
    "HIGHLANDER",
    "HRX",
    "WROGN",
    "Anouk",
    "DressBerry",
    "Puma",
    "Nike",
    "Baggit",
    "Fastrack",
    "Adidas",
    "Levis",
    "MastHarbour",
    "Libas",
    "Tokyo Talkies"
]

categories = [
    "Men Shirts",
    "Men T-Shirts",
    "Men Jeans",
    "Women Kurtas",
    "Women Dresses",
    "Sports Shoes",
    "Casual Shoes",
    "Handbags",
    "Watches",
    "Beauty",
    "Accessories"
]

product_types = [
    "Slim Fit Shirt",
    "Casual T-Shirt",
    "Regular Fit Jeans",
    "Printed Kurta",
    "Floral Dress",
    "Running Shoes",
    "Casual Sneakers",
    "Leather Handbag",
    "Analog Watch",
    "Sports Jacket",
    "Cotton Top"
]

# Generate product catalog
# products = []

# for i in range(1, number_of_products + 1):

#     product_id = f"P{i:07d}"

#     product = {
#         "product_id": product_id,
#         "product_name": f"{random.choice(product_types)} {i}",
#         "brand": brands[i % len(brands)],
#         "category": categories[i % len(categories)],
#         "price": random.randint(499, 4999)
#     }

#     products.append(product)

# print(f"Generated {len(products):,} products")

# COMMAND ----------

# products = [
#     {
#         "product_id": "P1001",
#         "product_name": "Slim Fit Cotton Shirt",
#         "brand": "Roadster",
#         "category": "Men Shirts",
#         "price": 1299
#     },
#     {
#         "product_id": "P1002",
#         "product_name": "Casual Checked Shirt",
#         "brand": "HIGHLANDER",
#         "category": "Men Shirts",
#         "price": 999
#     },
#     {
#         "product_id": "P1003",
#         "product_name": "Regular Fit T-Shirt",
#         "brand": "HRX",
#         "category": "Men T-Shirts",
#         "price": 799
#     },
#     {
#         "product_id": "P1004",
#         "product_name": "Skinny Fit Jeans",
#         "brand": "WROGN",
#         "category": "Men Jeans",
#         "price": 1799
#     },
#     {
#         "product_id": "P1005",
#         "product_name": "Printed Kurta",
#         "brand": "Anouk",
#         "category": "Women Kurtas",
#         "price": 1499
#     },
#     {
#         "product_id": "P1006",
#         "product_name": "Floral Dress",
#         "brand": "DressBerry",
#         "category": "Women Dresses",
#         "price": 1899
#     },
#     {
#         "product_id": "P1007",
#         "product_name": "Running Shoes",
#         "brand": "Puma",
#         "category": "Sports Shoes",
#         "price": 3499
#     },
#     {
#         "product_id": "P1008",
#         "product_name": "Casual Sneakers",
#         "brand": "Nike",
#         "category": "Casual Shoes",
#         "price": 4999
#     },
#     {
#         "product_id": "P1009",
#         "product_name": "Leather Handbag",
#         "brand": "Baggit",
#         "category": "Handbags",
#         "price": 2299
#     },
#     {
#         "product_id": "P1010",
#         "product_name": "Analog Watch",
#         "brand": "Fastrack",
#         "category": "Watches",
#         "price": 1999
#     }
# ]

cities = [
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Pune",
    "Kolkata",
    "Ahmedabad"
]

devices = [
    "ANDROID",
    "IOS",
    "WEB"
]

search_queries = [
    "men shirts",
    "women dresses",
    "running shoes",
    "casual t shirts",
    "blue jeans",
    "kurta for women",
    "sports shoes",
    "handbags",
    "watches",
    "sneakers"
]

event_types = [
    "APP_OPEN",
    "SEARCH",
    "FILTER_APPLIED",
    "LISTING_IMPRESSION",
    "PRODUCT_VIEW",
    "WISHLIST",
    "ADD_TO_CART",
    "REMOVE_FROM_CART",
    "CHECKOUT",
    "PURCHASE"
]

# COMMAND ----------

def generate_clickstream_event(
    event_number,
    batch_number,
    batch_start_time,
    batch_duration_seconds
):

    random.seed(event_number)

    # ========================================================
    # EVENT TYPE
    # ========================================================

    event_type = random.choices(
        population=event_types,
        weights=[
            5,    # APP_OPEN
            10,   # SEARCH
            5,    # FILTER_APPLIED
            40,   # LISTING_IMPRESSION
            20,   # PRODUCT_VIEW
            4,    # WISHLIST
            6,    # ADD_TO_CART
            2,    # REMOVE_FROM_CART
            3,    # CHECKOUT
            5     # PURCHASE
        ],
        k=1
    )[0]


    # ========================================================
    # PRODUCT
    # ========================================================

    product_number = random.randint(1, number_of_products)

    product_id = f"P{product_number:07d}"


    # ========================================================
    # EVENT TIMESTAMP
    # ========================================================

    random_offset = random.randint(
        0,
        batch_duration_seconds - 1
    )

    event_time = (
        batch_start_time
        + timedelta(seconds=random_offset)
    )


    # ========================================================
    # USER
    # ========================================================

    user_number = random.randint(
        1,
        10_000
    )

    user_id = f"USER_{user_number:08d}"


    # ========================================================
    # SESSION
    # ========================================================

    session_number = random.randint(
        1,
        5
    )

    session_id = (
        f"SESSION_{user_number:08d}_"
        f"{session_number:03d}"
    )


    # ========================================================
    # LISTING IMPRESSION
    # ========================================================

    visible_products = []

    if event_type == "LISTING_IMPRESSION":

        number_of_visible_products = random.randint(
            4,
            6
        )

        visible_product_numbers = random.sample(
            range(1, number_of_products + 1),
            number_of_visible_products
        )

        visible_products = [
            f"P{product_number:07d}"
            for product_number in visible_product_numbers
        ]

        product_id = None
        product_name = None
        brand = None
        category = None
        price = None

    else:

        product_id = product_id
        product_type =  product_types[
            product_number % len(product_types)
        ]
        product_name = f"{product_type} {product_number}"
        brand = brands[product_number % len(brands)]
        category = categories[
            product_number % len(categories)
        ]
        price = 499 + (product_number * 37) % 4501


    # ========================================================
    # SEARCH QUERY
    # ========================================================

    if event_type in [
        "SEARCH",
        "FILTER_APPLIED",
        "LISTING_IMPRESSION"
    ]:
        search_query = random.choice(
            search_queries
        )
    else:
        search_query = None


    # ========================================================
    # INTENTIONAL DATA QUALITY ISSUE
    # ========================================================

    if event_number % 10_000 == 0:
        user_id = None


    # ========================================================
    # PAGE
    # ========================================================

    if event_type == "LISTING_IMPRESSION":
        page_name = "SEARCH_RESULTS"

    elif event_type == "PRODUCT_VIEW":
        page_name = "PRODUCT_DETAIL"

    else:
        page_name = "HOME"


    # ========================================================
    # FINAL RECORD
    # ========================================================

    return {
        "event_id": f"EVT_{event_number:012d}",
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "event_timestamp": event_time,
        "product_id": product_id,
        "product_name": product_name,
        "brand": brand,
        "category": category,
        "price": price,
        "search_query": search_query,
        "device_type": random.choice(devices),
        "city": random.choice(cities),
        "visible_products": visible_products,
        "page_name": page_name
    }

# COMMAND ----------

# def generate_clickstream_event(event_number):
    
#     random.seed(event_number)

#     event_type = random.choices(
#         population=event_types,
#         weights=[
#             5,    # APP_OPEN
#             10,   # SEARCH
#             5,    # FILTER_APPLIED
#             40,   # LISTING_IMPRESSION
#             20,   # PRODUCT_VIEW
#             4,    # WISHLIST
#             6,    # ADD_TO_CART
#             2,    # REMOVE_FROM_CART
#             3,    # CHECKOUT
#             5     # PURCHASE
#         ],
#         k=1
#     )[0]

#     selected_product = random.choice(products)

#     # Random event time during the previous 24 hours
#     event_time = (
#         datetime.utcnow()
#         - timedelta(
#             seconds=random.randint(0, 24 * 60 * 60)
#         )
#     )

#     user_number = random.randint(1, 10_000)

#     user_id = f"USER_{user_number:08d}"

#     session_number = random.randint(1, 5)

#     session_id = (
#         f"SESSION_{user_number:08d}_"
#         f"{session_number:03d}"
#     )

#     visible_products = []

#     # A listing impression contains 4–6 visible products
#     if event_type == "LISTING_IMPRESSION":

#         number_of_visible_products = random.randint(4, 6)

#         selected_products = random.sample(
#             products,
#             number_of_visible_products
#         )

#         visible_products = [
#             product["product_id"]
#             for product in selected_products
#         ]

#         # No single product is required for a batched impression
#         product_id = None
#         product_name = None
#         brand = None
#         category = None
#         price = None

#     else:

#         product_id = selected_product["product_id"]
#         product_name = selected_product["product_name"]
#         brand = selected_product["brand"]
#         category = selected_product["category"]
#         price = selected_product["price"]

#     # Search query is mainly relevant for search/listing events
#     if event_type in [
#         "SEARCH",
#         "FILTER_APPLIED",
#         "LISTING_IMPRESSION"
#     ]:
#         search_query = random.choice(search_queries)
#     else:
#         search_query = None

#     # Add a small number of intentionally invalid records.
#     # These will be used to test Silver data-quality checks.
#     if event_number % 10_000 == 0:
#         user_id = None

#     return {
#         "event_id": f"EVT_{event_number:012d}",
#         "user_id": user_id,
#         "session_id": session_id,
#         "event_type": event_type,
#         "event_timestamp": event_time,
#         "product_id": product_id,
#         "product_name": product_name,
#         "brand": brand,
#         "category": category,
#         "price": price,
#         "search_query": search_query,
#         "device_type": random.choice(devices),
#         "city": random.choice(cities),
#         "visible_products": visible_products,
#         "page_name": (
#             "SEARCH_RESULTS"
#             if event_type == "LISTING_IMPRESSION"
#             else "PRODUCT_DETAIL"
#             if event_type == "PRODUCT_VIEW"
#             else "HOME"
#         )
#     }

# COMMAND ----------

# ============================================================
# SIMULATED TRAFFIC WINDOW
# ============================================================

simulation_start = datetime.utcnow()

batch_duration_seconds = (
    traffic_window_minutes * 60
) // number_of_batches


print(
    f"Simulation start : {simulation_start}"
)

print(
    f"Batch duration   : {batch_duration_seconds} seconds"
)



# COMMAND ----------

# ============================================================
# GENERATE TRAFFIC BATCHES
# ============================================================

for batch_number in range(number_of_batches):

    batch_start_time = (
        simulation_start
        + timedelta(
            seconds=batch_number * batch_duration_seconds
        )
    )

    batch_event_start = (
        event_start
        + batch_number * events_per_batch
    )

    batch_event_end = (
        batch_event_start
        + events_per_batch
    )

    print(
        f"Generating batch "
        f"{batch_number + 1}/{number_of_batches} "
        f"| Events: "
        f"{batch_event_start:,} - "
        f"{batch_event_end - 1:,} "
        f"| Event time: "
        f"{batch_start_time}"
    )


    # --------------------------------------------------------
    # Generate events
    # --------------------------------------------------------

    batch_rdd = (
        spark.sparkContext
            .parallelize(
                range(
                    batch_event_start,
                    batch_event_end
                ),
                number_of_partitions
            )
            .map(
                lambda event_number:
                    generate_clickstream_event(
                        event_number,
                        batch_number,
                        batch_start_time,
                        batch_duration_seconds
                    )
            )
    )


    batch_df = spark.createDataFrame(
        batch_rdd
    )


    # --------------------------------------------------------
    # Write batch
    # --------------------------------------------------------

    (
        batch_df
            .repartition(number_of_partitions)
            .write
            .mode("append")
            .json(landing_path)
    )


    print(
        f"Batch {batch_number + 1} written successfully"
    )

# COMMAND ----------

# event_start = spark.read.table(bronze_table).count() if spark.catalog.tableExists(bronze_table) else 0 

# event_rdd = (
#     spark.sparkContext
#          .parallelize(
#              range(
#                  event_start,
#                  event_start + number_of_events
#              ),
#              number_of_partitions
#          )
#          .map(generate_clickstream_event)
# )

# clickstream_df = spark.createDataFrame(event_rdd)

# COMMAND ----------

# DBTITLE 1,Cell 6
# (
#     clickstream_df
#         .repartition(number_of_partitions)
#         .write
#         .mode("append")
#         .json(landing_path)
# )

# COMMAND ----------

spark.read.option("samplingRatio", 0.05).json(landing_path).printSchema()