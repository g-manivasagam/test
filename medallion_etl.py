# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Medallion ETL: Volume -> Bronze -> Silver -> Gold (Orders data)
# MAGIC Source file: `/Volumes/workspace/default/data/ETLSample/orders_sample.csv`
# MAGIC Expected columns: order_id, customer_id, product, quantity, unit_price, order_date, status
# MAGIC Run the cells in order.

# COMMAND ----------

# ---------- CONFIG ----------
CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "data"
SOURCE_SUBPATH = "ETLSample"            # folder inside the volume
SOURCE_FILE_PATTERN = "orders_sample.csv"   # only this file is read (other files in the folder are ignored)
FILE_FORMAT = "csv"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SOURCE_PATH = f"{VOLUME_ROOT}/{SOURCE_SUBPATH}".rstrip("/")
CHECKPOINT_BASE = f"{VOLUME_ROOT}/_checkpoints"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"
GOLD_DAILY_SALES = f"{CATALOG}.{SCHEMA}.gold_daily_sales"
GOLD_CUSTOMER = f"{CATALOG}.{SCHEMA}.gold_customer_summary"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset (run once)
# MAGIC Clears checkpoints and tables from earlier attempts, including the old employee tables.

# COMMAND ----------

dbutils.fs.rm(CHECKPOINT_BASE, True)
for t in [BRONZE_TABLE, SILVER_TABLE, GOLD_DAILY_SALES, GOLD_CUSTOMER,
          f"{CATALOG}.{SCHEMA}.bronze_employees", f"{CATALOG}.{SCHEMA}.silver_employees",
          f"{CATALOG}.{SCHEMA}.gold_hires_by_year", f"{CATALOG}.{SCHEMA}.gold_place_summary"]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: raw ingestion
# MAGIC Column names are cleaned (lowercase, special characters replaced by underscores). Values stay as raw strings.

# COMMAND ----------

import re
from pyspark.sql import functions as F, Window


def clean_names(cols):
    out, seen = [], {}
    for c in cols:
        if c.startswith("_"):
            n = c
        else:
            n = re.sub(r"[^0-9a-zA-Z]+", "_", c.strip()).strip("_").lower() or "col"
        if n in seen:
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        out.append(n)
    return out


reader = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", FILE_FORMAT)
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/bronze_schema")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("pathGlobFilter", SOURCE_FILE_PATTERN)
)
if FILE_FORMAT == "csv":
    reader = reader.option("header", "true")

raw_df = reader.load(SOURCE_PATH)
bronze_df = (
    raw_df.toDF(*clean_names(raw_df.columns))
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

(
    bronze_df.writeStream
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
    .awaitTermination()
)

print("Bronze rows:", spark.table(BRONZE_TABLE).count())
display(spark.table(BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC select table_catalog, table_schema, table_name, table_type, created
# MAGIC from system.information_schema.tables
# MAGIC where table_name = 'bronze_orders';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check columns
# MAGIC Confirms bronze has the columns silver needs.

# COMMAND ----------

REQUIRED = ["order_id", "customer_id", "product", "quantity", "unit_price", "order_date", "status"]
missing = [c for c in REQUIRED if c not in spark.table(BRONZE_TABLE).columns]
print("Columns found:", spark.table(BRONZE_TABLE).columns)
assert not missing, f"Missing expected columns in bronze: {missing}. Send me the column list and I'll adjust."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: cleaned, typed, de-duplicated

# COMMAND ----------

bronze = spark.table(BRONZE_TABLE)

silver_df = (
    bronze
    .select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.initcap(F.trim("product")).alias("product"),
        F.expr("try_cast(quantity AS INT)").alias("quantity"),
        F.expr("try_cast(unit_price AS DECIMAL(12,2))").alias("unit_price"),
        F.expr(
            "CAST(COALESCE(try_to_timestamp(order_date, 'yyyy-MM-dd'), "
            "try_to_timestamp(order_date, 'M/d/yyyy')) AS DATE)"
        ).alias("order_date"),
        F.lower(F.trim("status")).alias("status"),
        "_ingest_ts",
        "_source_file",
    )
    # data-quality rules
    .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") >= 0)
    .filter(F.col("order_date").isNotNull())
    # keep the latest record per order_id
    .withColumn("_rn", F.row_number().over(Window.partitionBy("order_id").orderBy(F.col("_ingest_ts").desc())))
    .filter("_rn = 1")
    .drop("_rn")
    .withColumn("total_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)"))
)

silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)

print("Silver rows:", spark.table(SILVER_TABLE).count())
display(spark.table(SILVER_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: business-level aggregates

# COMMAND ----------

silver = spark.table(SILVER_TABLE).filter(F.col("status") != "cancelled")

# Gold 1: daily sales by product
(
    silver.groupBy("order_date", "product")
    .agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units_sold"),
        F.sum("total_amount").alias("revenue"),
    )
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(GOLD_DAILY_SALES)
)

# Gold 2: customer summary
(
    silver.groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("total_amount").alias("lifetime_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(GOLD_CUSTOMER)
)

display(spark.table(GOLD_DAILY_SALES).orderBy(F.col("order_date").desc()).limit(10))
display(spark.table(GOLD_CUSTOMER).orderBy(F.col("lifetime_value").desc()).limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_orders;
# MAGIC select * from silver_orders;
# MAGIC select * from gold_customer_summary;
# MAGIC select * from gold_daily_sales;
# MAGIC