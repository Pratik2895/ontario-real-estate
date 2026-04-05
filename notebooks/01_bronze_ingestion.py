# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC Ingests all raw CSV sources into Bronze Delta tables with metadata columns.
# MAGIC
# MAGIC **Sources (1.2M+ records):**
# MAGIC | Table | Source | Records |
# MAGIC |---|---|---|
# MAGIC | bronze.property_boundaries | Toronto Open Data — CKAN | ~528K |
# MAGIC | bronze.permits_active | Toronto Open Data — CKAN | ~232K |
# MAGIC | bronze.permits_cleared | Toronto Open Data — CKAN | ~390K |
# MAGIC | bronze.building_assets | Toronto Open Data — CKAN | ~2.6K |
# MAGIC | bronze.land_assets | Toronto Open Data — CKAN | ~10.7K |
# MAGIC | bronze.apartment_registration | Toronto Open Data — CKAN | ~3.6K |
# MAGIC | bronze.apartment_evaluations | Toronto Open Data — CKAN | ~5.3K |
# MAGIC | bronze.housing_price_index | Statistics Canada | ~65K |

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

try:
    spark  # noqa: F821
except NameError:
    spark = (
        SparkSession.builder
        .appName("ontario_real_estate_bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.driver.memory", "4g")
        .master("local[*]")
        .getOrCreate()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import os

IS_DATABRICKS = os.path.exists("/dbfs")

if IS_DATABRICKS:
    RAW_BASE = "/mnt/data/raw"
    BRONZE_BASE = "/mnt/data/bronze"
    CATALOG = "ontario_real_estate"
else:
    RAW_BASE = "../data/raw"
    BRONZE_BASE = "../data/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Helper

# COMMAND ----------

def ingest_to_bronze(csv_path: str, table_name: str, source_name: str, encoding: str = "utf-8"):
    """Read a CSV and write to Bronze Delta with metadata columns."""
    print(f"\n{'='*60}")
    print(f"Ingesting: {source_name}")
    print(f"  CSV: {csv_path}")

    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("encoding", encoding)
        .csv(csv_path)
    )

    row_count = df_raw.count()
    col_count = len(df_raw.columns)
    print(f"  Raw: {row_count:,} rows x {col_count} cols")

    # Add metadata
    df_bronze = (
        df_raw
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source", lit(source_name))
        .withColumn("_source_file", lit(csv_path))
    )

    # Write Delta
    bronze_path = f"{BRONZE_BASE}/{table_name}"
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(bronze_path)
    )

    print(f"  Delta: {bronze_path}")
    print(f"  Status: OK ({row_count:,} rows written)")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest All Sources

# COMMAND ----------

total = 0

# 1. Property Boundaries (~528K)
total += ingest_to_bronze(
    f"{RAW_BASE}/toronto_property_boundaries.csv",
    "property_boundaries",
    "Toronto Open Data — Property Boundaries",
)

# COMMAND ----------

# 2. Building Permits — Active (~232K)
total += ingest_to_bronze(
    f"{RAW_BASE}/toronto_permits_active.csv",
    "permits_active",
    "Toronto Open Data — Active Building Permits",
)

# COMMAND ----------

# 3. Building Permits — Cleared (~390K)
total += ingest_to_bronze(
    f"{RAW_BASE}/toronto_permits_cleared.csv",
    "permits_cleared",
    "Toronto Open Data — Cleared Building Permits",
)

# COMMAND ----------

# 4. Building Assets (~2.6K)
total += ingest_to_bronze(
    f"{RAW_BASE}/toronto_building_assets.csv",
    "building_assets",
    "Toronto Open Data — Building Asset Inventory",
)

# COMMAND ----------

# 5. Land Assets (~10.7K)
total += ingest_to_bronze(
    f"{RAW_BASE}/toronto_land_assets.csv",
    "land_assets",
    "Toronto Open Data — Land Asset Inventory",
)

# COMMAND ----------

# 6. Apartment Registration (~3.6K)
total += ingest_to_bronze(
    f"{RAW_BASE}/toronto_apartment_registration.csv",
    "apartment_registration",
    "Toronto Open Data — RentSafeTO Registration",
)

# COMMAND ----------

# 7. Apartment Evaluations (~5.3K)
total += ingest_to_bronze(
    f"{RAW_BASE}/toronto_apartment_evaluations.csv",
    "apartment_evaluations",
    "Toronto Open Data — RentSafeTO Evaluations",
)

# COMMAND ----------

# 8. StatCan Housing Price Index (~65K)
nhpi_path = f"{RAW_BASE}/statcan_nhpi/18100205.csv" if not IS_DATABRICKS else f"{RAW_BASE}/statcan_nhpi/18100205.csv"
total += ingest_to_bronze(
    nhpi_path,
    "housing_price_index",
    "Statistics Canada — New Housing Price Index (18-10-0205-01)",
    encoding="utf-8-sig",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\n{'='*60}")
print(f"BRONZE INGESTION COMPLETE")
print(f"Total records ingested: {total:,}")
print(f"{'='*60}")

# Verify all tables
for table in [
    "property_boundaries", "permits_active", "permits_cleared",
    "building_assets", "land_assets", "apartment_registration",
    "apartment_evaluations", "housing_price_index",
]:
    df = spark.read.format("delta").load(f"{BRONZE_BASE}/{table}")
    print(f"  bronze.{table:30s} → {df.count():>10,} rows")
