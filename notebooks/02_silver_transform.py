# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleaned & Transformed
# MAGIC Reads Bronze tables, applies cleaning/typing/dedup/joins, writes Silver Delta.
# MAGIC
# MAGIC **Silver Tables:**
# MAGIC | Table | Source(s) | Description |
# MAGIC |---|---|---|
# MAGIC | silver.properties | property_boundaries + building_assets | Properties with building details |
# MAGIC | silver.building_permits | permits_active + permits_cleared | Unified permit history |
# MAGIC | silver.apartment_buildings | apartment_registration + evaluations | Apartment buildings with scores |
# MAGIC | silver.housing_price_index | housing_price_index | Ontario-filtered price index |

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, trim, upper, lower, initcap, when, year, month, quarter,
    round as spark_round, row_number, current_timestamp, lit, concat_ws,
    regexp_replace, coalesce, count, avg, sum as spark_sum,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, LongType

# COMMAND ----------

try:
    spark  # noqa: F821
except NameError:
    spark = (
        SparkSession.builder
        .appName("ontario_real_estate_silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.driver.memory", "4g")
        .master("local[*]")
        .getOrCreate()
    )

# COMMAND ----------

import os

IS_DATABRICKS = os.path.exists("/dbfs")
BRONZE_BASE = "/mnt/data/bronze" if IS_DATABRICKS else "../data/bronze"
SILVER_BASE = "/mnt/data/silver" if IS_DATABRICKS else "../data/silver"

def read_bronze(table_name: str):
    return spark.read.format("delta").load(f"{BRONZE_BASE}/{table_name}")

def write_silver(df, table_name: str):
    path = f"{SILVER_BASE}/{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    cnt = spark.read.format("delta").load(path).count()
    print(f"  silver.{table_name}: {cnt:,} rows written")
    return cnt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver 1: Properties (boundaries + building assets)

# COMMAND ----------

print("="*60)
print("Silver 1: Properties")
print("="*60)

df_prop = read_bronze("property_boundaries")
df_bldg = read_bronze("building_assets")

# Clean property boundaries
df_prop_clean = (
    df_prop
    .select(
        col("PARCELID").alias("parcel_id"),
        col("FEATURE_TYPE").alias("feature_type"),
        to_date(col("DATE_EFFECTIVE"), "yyyy/MM/dd").alias("date_effective"),
        col("STATEDAREA").cast(DoubleType()).alias("stated_area_sqm"),
        col("ADDRESS_NUMBER").alias("address_number"),
        trim(col("LINEAR_NAME_FULL")).alias("street_name"),
        col("OBJECTID").cast(LongType()).alias("object_id"),
    )
    .withColumn("full_address", concat_ws(" ", col("address_number"), col("street_name")))
    .filter(col("parcel_id").isNotNull())
)

# Clean building assets
df_bldg_clean = (
    df_bldg
    .select(
        col("FLOC_ID").alias("floc_id"),
        trim(col("Building Description")).alias("building_description"),
        trim(col("Address")).alias("building_address"),
        trim(col("Ward Name")).alias("ward_name"),
        trim(col("Former Municipality")).alias("former_municipality"),
        trim(col("District")).alias("district"),
        trim(col("Building Type")).alias("building_type"),
        trim(col("Building Status")).alias("building_status"),
        col("Gross Floor Area (M2)").cast(DoubleType()).alias("gross_floor_area_sqm"),
        col("Gross Floor Area (FT2)").cast(DoubleType()).alias("gross_floor_area_sqft"),
        col("Year Built").cast(IntegerType()).alias("year_built"),
        trim(col("WARD")).alias("ward_number"),
    )
)

# Dedup properties by parcel_id
w = Window.partitionBy("parcel_id").orderBy(col("date_effective").desc())
df_prop_dedup = (
    df_prop_clean
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# Add derived columns
df_properties = (
    df_prop_dedup
    .withColumn("stated_area_sqft", spark_round(col("stated_area_sqm") * 10.7639, 2))
    .withColumn("_silver_timestamp", current_timestamp())
)

print(f"  Properties (deduped): {df_properties.count():,}")
write_silver(df_properties, "properties")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver 2: Building Permits (active + cleared)

# COMMAND ----------

print("\n" + "="*60)
print("Silver 2: Building Permits")
print("="*60)

df_active = read_bronze("permits_active").withColumn("_permit_source", lit("active"))
df_cleared = read_bronze("permits_cleared").withColumn("_permit_source", lit("cleared"))

# Union active + cleared
df_permits_raw = df_active.unionByName(df_cleared, allowMissingColumns=True)

# Clean and type
df_permits = (
    df_permits_raw
    .select(
        trim(col("PERMIT_NUM")).alias("permit_number"),
        col("REVISION_NUM").cast(IntegerType()).alias("revision_number"),
        trim(col("PERMIT_TYPE")).alias("permit_type"),
        trim(col("STRUCTURE_TYPE")).alias("structure_type"),
        trim(col("WORK")).alias("work_type"),
        trim(col("STREET_NUM")).alias("street_number"),
        trim(col("STREET_NAME")).alias("street_name"),
        trim(col("STREET_TYPE")).alias("street_type"),
        trim(col("STREET_DIRECTION")).alias("street_direction"),
        trim(col("POSTAL")).alias("postal_code"),
        col("GEO_ID").cast(LongType()).alias("geo_id"),
        trim(col("WARD_GRID")).alias("ward"),
        to_date(col("APPLICATION_DATE"), "yyyy-MM-dd").alias("application_date"),
        to_date(col("ISSUED_DATE"), "yyyy-MM-dd").alias("issued_date"),
        to_date(col("COMPLETED_DATE"), "yyyy-MM-dd").alias("completed_date"),
        trim(col("STATUS")).alias("status"),
        trim(col("DESCRIPTION")).alias("description"),
        trim(col("CURRENT_USE")).alias("current_use"),
        trim(col("PROPOSED_USE")).alias("proposed_use"),
        col("DWELLING_UNITS_CREATED").cast(IntegerType()).alias("dwelling_units_created"),
        col("DWELLING_UNITS_LOST").cast(IntegerType()).alias("dwelling_units_lost"),
        col("EST_CONST_COST").cast(DoubleType()).alias("estimated_construction_cost"),
        col("_permit_source"),
    )
    .filter(col("permit_number").isNotNull())
)

# Add derived columns
df_permits = (
    df_permits
    .withColumn("full_address", concat_ws(" ", "street_number", "street_name", "street_type", "street_direction"))
    .withColumn("application_year", year("application_date"))
    .withColumn("application_quarter", quarter("application_date"))
    .withColumn("net_dwelling_units", col("dwelling_units_created") - col("dwelling_units_lost"))
    .withColumn("postal_prefix", col("postal_code").substr(1, 3))
    .withColumn("_silver_timestamp", current_timestamp())
)

# Dedup: keep latest revision per permit
w_permit = Window.partitionBy("permit_number").orderBy(col("revision_number").desc())
df_permits_dedup = (
    df_permits
    .withColumn("_rn", row_number().over(w_permit))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

print(f"  Permits (raw union): {df_permits.count():,}")
print(f"  Permits (deduped): {df_permits_dedup.count():,}")
write_silver(df_permits_dedup, "building_permits")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver 3: Apartment Buildings (registration + evaluations)

# COMMAND ----------

print("\n" + "="*60)
print("Silver 3: Apartment Buildings")
print("="*60)

df_reg = read_bronze("apartment_registration")
df_eval = read_bronze("apartment_evaluations")

# Clean registration data
df_reg_clean = (
    df_reg
    .select(
        col("RSN").cast(LongType()).alias("rsn"),
        trim(col("SITE_ADDRESS")).alias("address"),
        trim(col("PROPERTY_TYPE")).alias("property_type"),
        trim(col("WARD")).alias("ward"),
        col("YEAR_BUILT").cast(IntegerType()).alias("year_built"),
        col("YEAR_REGISTERED").cast(IntegerType()).alias("year_registered"),
        coalesce(col("CONFIRMED_STOREYS"), col("NO_OF_STOREYS")).cast(IntegerType()).alias("storeys"),
        coalesce(col("CONFIRMED_UNITS"), col("NO_OF_UNITS")).cast(IntegerType()).alias("units"),
        trim(col("HEATING_TYPE")).alias("heating_type"),
        trim(col("AIR_CONDITIONING_TYPE")).alias("ac_type"),
        trim(col("PARKING_TYPE")).alias("parking_type"),
        col("NO_OF_ELEVATORS").cast(IntegerType()).alias("elevators"),
        trim(col("PETS_ALLOWED")).alias("pets_allowed"),
        trim(col("LAUNDRY_ROOM")).alias("has_laundry"),
        trim(col("BIKE_PARKING")).alias("has_bike_parking"),
        trim(col("PCODE")).alias("postal_code"),
    )
)

# Clean evaluation data
df_eval_clean = (
    df_eval
    .select(
        col("RSN").cast(LongType()).alias("rsn"),
        col("YEAR EVALUATED").cast(IntegerType()).alias("year_evaluated"),
        col("CURRENT BUILDING EVAL SCORE").cast(DoubleType()).alias("eval_score"),
        col("PROACTIVE BUILDING SCORE").cast(DoubleType()).alias("proactive_score"),
        col("CURRENT REACTIVE SCORE").cast(DoubleType()).alias("reactive_score"),
        col("NO OF AREAS EVALUATED").cast(IntegerType()).alias("areas_evaluated"),
        col("CONFIRMED STOREYS").cast(IntegerType()).alias("eval_storeys"),
        col("CONFIRMED UNITS").cast(IntegerType()).alias("eval_units"),
        col("LATITUDE").cast(DoubleType()).alias("latitude"),
        col("LONGITUDE").cast(DoubleType()).alias("longitude"),
    )
)

# Get latest evaluation per building
w_eval = Window.partitionBy("rsn").orderBy(col("year_evaluated").desc())
df_eval_latest = (
    df_eval_clean
    .withColumn("_rn", row_number().over(w_eval))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# Join registration + latest evaluation
df_apartments = (
    df_reg_clean
    .join(df_eval_latest, "rsn", "left")
    .withColumn("building_age", lit(2025) - col("year_built"))
    .withColumn("units_per_storey", spark_round(col("units") / col("storeys"), 1))
    .withColumn("postal_prefix", col("postal_code").substr(1, 3))
    .withColumn("_silver_timestamp", current_timestamp())
)

print(f"  Registrations: {df_reg_clean.count():,}")
print(f"  Evaluations (latest): {df_eval_latest.count():,}")
write_silver(df_apartments, "apartment_buildings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver 4: Housing Price Index (Ontario filtered)

# COMMAND ----------

print("\n" + "="*60)
print("Silver 4: Housing Price Index")
print("="*60)

df_hpi = read_bronze("housing_price_index")

# Filter to Ontario cities and clean
ONTARIO_GEOS = [
    "Ontario", "Toronto", "Ottawa", "Hamilton",
    "Oshawa", "Kitchener", "St. Catharines", "Windsor",
    "Greater Sudbury", "Thunder Bay", "Guelph", "London",
    "Barrie", "Brantford", "Peterborough", "Belleville",
    "Ottawa-Gatineau, Ontario part",
    "Toronto, Ontario",
]

df_hpi_clean = (
    df_hpi
    .filter(
        col("GEO").isin(ONTARIO_GEOS)
        | col("GEO").contains("Ontario")
    )
    .select(
        to_date(concat_ws("-", col("REF_DATE"), lit("01")), "yyyy-MM-dd").alias("reference_date"),
        col("REF_DATE").alias("ref_period"),
        trim(col("GEO")).alias("geography"),
        trim(regexp_replace(col("New housing price indexes"), '"', '')).alias("index_type"),
        col("VALUE").cast(DoubleType()).alias("index_value"),
        trim(col("STATUS")).alias("data_quality_flag"),
    )
    .filter(col("index_value").isNotNull())
    .withColumn("ref_year", year("reference_date"))
    .withColumn("ref_month", month("reference_date"))
    .withColumn("ref_quarter", quarter("reference_date"))
    .withColumn("_silver_timestamp", current_timestamp())
)

write_silver(df_hpi_clean, "housing_price_index")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\n{'='*60}")
print("SILVER TRANSFORMATION COMPLETE")
print("="*60)
for table in ["properties", "building_permits", "apartment_buildings", "housing_price_index"]:
    df = spark.read.format("delta").load(f"{SILVER_BASE}/{table}")
    print(f"  silver.{table:30s} → {df.count():>10,} rows  ({len(df.columns)} cols)")
