# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Analytics Aggregations
# MAGIC Reads Silver tables and produces Gold analytics tables for dashboard consumption.
# MAGIC
# MAGIC **Gold Tables:**
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | gold.permit_trends | Building permit activity by year/quarter/ward/type |
# MAGIC | gold.construction_investment | Estimated construction cost aggregations |
# MAGIC | gold.apartment_scorecard | Apartment building quality KPIs by ward |
# MAGIC | gold.housing_development | Net dwelling unit creation trends |
# MAGIC | gold.price_index_trends | Ontario housing price index over time |
# MAGIC | gold.property_overview | Property boundary stats by ward/type |

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum as spark_sum, round as spark_round, max as spark_max,
    min as spark_min, when, first, percentile_approx, lag, desc, dense_rank,
    countDistinct,
)
from pyspark.sql.window import Window

# COMMAND ----------

try:
    spark  # noqa: F821
except NameError:
    spark = (
        SparkSession.builder
        .appName("ontario_real_estate_gold")
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
SILVER_BASE = "/mnt/data/silver" if IS_DATABRICKS else "../data/silver"
GOLD_BASE = "/mnt/data/gold" if IS_DATABRICKS else "../data/gold"

def read_silver(table):
    return spark.read.format("delta").load(f"{SILVER_BASE}/{table}")

def write_gold(df, table_name):
    delta_path = f"{GOLD_BASE}/{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
    # Also save as parquet for local Streamlit use
    parquet_path = f"{GOLD_BASE}/{table_name}.parquet"
    df.toPandas().to_parquet(parquet_path, index=False)
    cnt = spark.read.format("delta").load(delta_path).count()
    print(f"  gold.{table_name}: {cnt:,} rows")
    return cnt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 1: Permit Trends

# COMMAND ----------

print("="*60)
print("Gold 1: Permit Trends")
print("="*60)

df_permits = read_silver("building_permits")

df_permit_trends = (
    df_permits
    .filter(col("application_year").isNotNull() & (col("application_year") >= 2017))
    .groupBy("application_year", "application_quarter", "ward", "permit_type", "structure_type", "work_type", "status")
    .agg(
        count("*").alias("permit_count"),
        spark_round(avg("estimated_construction_cost"), 2).alias("avg_construction_cost"),
        spark_round(spark_sum("estimated_construction_cost"), 2).alias("total_construction_cost"),
        spark_sum("dwelling_units_created").alias("total_units_created"),
        spark_sum("dwelling_units_lost").alias("total_units_lost"),
        spark_sum("net_dwelling_units").alias("net_dwelling_units"),
        countDistinct("permit_number").alias("unique_permits"),
    )
    .orderBy("application_year", "application_quarter", "ward")
)

write_gold(df_permit_trends, "permit_trends")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 2: Construction Investment

# COMMAND ----------

print("\n" + "="*60)
print("Gold 2: Construction Investment")
print("="*60)

df_investment = (
    df_permits
    .filter(
        col("estimated_construction_cost").isNotNull()
        & (col("estimated_construction_cost") > 0)
        & (col("application_year") >= 2017)
    )
    .groupBy("application_year", "application_quarter", "ward", "current_use", "proposed_use")
    .agg(
        count("*").alias("project_count"),
        spark_round(spark_sum("estimated_construction_cost"), 2).alias("total_investment"),
        spark_round(avg("estimated_construction_cost"), 2).alias("avg_investment"),
        spark_round(percentile_approx("estimated_construction_cost", 0.5), 2).alias("median_investment"),
        spark_max("estimated_construction_cost").alias("max_investment"),
        spark_sum("dwelling_units_created").alias("units_created"),
    )
    .withColumn(
        "investment_category",
        when(col("avg_investment") >= 10_000_000, "Mega (>$10M)")
        .when(col("avg_investment") >= 1_000_000, "Major ($1M-$10M)")
        .when(col("avg_investment") >= 100_000, "Medium ($100K-$1M)")
        .otherwise("Small (<$100K)")
    )
    .orderBy("application_year", "application_quarter")
)

write_gold(df_investment, "construction_investment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 3: Apartment Scorecard

# COMMAND ----------

print("\n" + "="*60)
print("Gold 3: Apartment Scorecard")
print("="*60)

df_apts = read_silver("apartment_buildings")

df_apt_scorecard = (
    df_apts
    .groupBy("ward", "property_type")
    .agg(
        count("*").alias("building_count"),
        spark_round(avg("eval_score"), 1).alias("avg_eval_score"),
        spark_round(avg("proactive_score"), 1).alias("avg_proactive_score"),
        spark_round(avg("reactive_score"), 1).alias("avg_reactive_score"),
        spark_round(avg("units"), 0).alias("avg_units"),
        spark_round(avg("storeys"), 0).alias("avg_storeys"),
        spark_round(avg("building_age"), 0).alias("avg_building_age"),
        spark_sum("units").alias("total_units"),
        spark_sum("elevators").alias("total_elevators"),
        spark_round(avg("year_built"), 0).alias("avg_year_built"),
        spark_min("year_built").alias("oldest_building"),
        spark_max("year_built").alias("newest_building"),
    )
    .withColumn(
        "quality_tier",
        when(col("avg_eval_score") >= 80, "Excellent")
        .when(col("avg_eval_score") >= 60, "Good")
        .when(col("avg_eval_score") >= 40, "Fair")
        .otherwise("Needs Improvement")
    )
    .orderBy(desc("building_count"))
)

write_gold(df_apt_scorecard, "apartment_scorecard")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 4: Housing Development (Net Dwelling Units)

# COMMAND ----------

print("\n" + "="*60)
print("Gold 4: Housing Development")
print("="*60)

df_housing_dev = (
    df_permits
    .filter(
        (col("application_year") >= 2017)
        & (col("dwelling_units_created").isNotNull() | col("dwelling_units_lost").isNotNull())
    )
    .groupBy("application_year", "application_quarter", "ward", "structure_type", "proposed_use")
    .agg(
        count("*").alias("permit_count"),
        spark_sum("dwelling_units_created").alias("units_created"),
        spark_sum("dwelling_units_lost").alias("units_lost"),
        spark_sum("net_dwelling_units").alias("net_units"),
        spark_round(spark_sum("estimated_construction_cost"), 2).alias("total_cost"),
    )
    .orderBy("application_year", "application_quarter")
)

# Add cumulative net units by ward
w_cum = Window.partitionBy("ward").orderBy("application_year", "application_quarter").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
df_housing_dev = df_housing_dev.withColumn(
    "cumulative_net_units", spark_sum("net_units").over(w_cum)
)

write_gold(df_housing_dev, "housing_development")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 5: Housing Price Index Trends (Ontario)

# COMMAND ----------

print("\n" + "="*60)
print("Gold 5: Price Index Trends")
print("="*60)

df_hpi = read_silver("housing_price_index")

# Pivot index types into columns
df_price_trends = (
    df_hpi
    .groupBy("reference_date", "ref_period", "geography", "ref_year", "ref_month", "ref_quarter")
    .pivot("index_type", ["Total (house and land)", "House only", "Land only"])
    .agg(first("index_value"))
)

# Rename pivoted columns
df_price_trends = (
    df_price_trends
    .withColumnRenamed("Total (house and land)", "index_total")
    .withColumnRenamed("House only", "index_house")
    .withColumnRenamed("Land only", "index_land")
)

# Add YoY change
w_yoy = Window.partitionBy("geography").orderBy("reference_date")
df_price_trends = (
    df_price_trends
    .withColumn("index_total_12m_ago", lag("index_total", 12).over(w_yoy))
    .withColumn(
        "yoy_change_pct",
        spark_round(
            (col("index_total") - col("index_total_12m_ago")) / col("index_total_12m_ago") * 100, 2
        ),
    )
)

write_gold(df_price_trends, "price_index_trends")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 6: Property Overview

# COMMAND ----------

print("\n" + "="*60)
print("Gold 6: Property Overview")
print("="*60)

df_props = read_silver("properties")

df_prop_overview = (
    df_props
    .groupBy("feature_type")
    .agg(
        count("*").alias("property_count"),
        spark_round(avg("stated_area_sqm"), 2).alias("avg_area_sqm"),
        spark_round(avg("stated_area_sqft"), 2).alias("avg_area_sqft"),
        spark_round(percentile_approx("stated_area_sqm", 0.5), 2).alias("median_area_sqm"),
        spark_min("stated_area_sqm").alias("min_area_sqm"),
        spark_max("stated_area_sqm").alias("max_area_sqm"),
        spark_round(spark_sum("stated_area_sqm"), 2).alias("total_area_sqm"),
    )
    .orderBy(desc("property_count"))
)

write_gold(df_prop_overview, "property_overview")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\n{'='*60}")
print("GOLD AGGREGATION COMPLETE")
print("="*60)
for table in [
    "permit_trends", "construction_investment", "apartment_scorecard",
    "housing_development", "price_index_trends", "property_overview",
]:
    df = spark.read.format("delta").load(f"{GOLD_BASE}/{table}")
    print(f"  gold.{table:30s} → {df.count():>10,} rows  ({len(df.columns)} cols)")
