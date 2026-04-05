"""
Ontario Real Estate --Local Pipeline Runner (Pandas)
Runs the full Bronze -> Silver -> Gold pipeline using pandas + parquet.
No Java/Spark required.

Usage: python run_pipeline_local.py
"""

import os
import time
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore", category=FutureWarning)

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    d.mkdir(parents=True, exist_ok=True)

NOW = datetime.now().isoformat()


def elapsed(start):
    return f"{time.time() - start:.1f}s"


# =========================================================================
# BRONZE LAYER --Raw ingestion with metadata
# =========================================================================

def run_bronze():
    print("\n" + "=" * 70)
    print("BRONZE LAYER --Raw Ingestion")
    print("=" * 70)

    sources = {
        "property_boundaries": ("toronto_property_boundaries.csv", {}),
        "permits_active": ("toronto_permits_active.csv", {}),
        "permits_cleared": ("toronto_permits_cleared.csv", {}),
        "building_assets": ("toronto_building_assets.csv", {}),
        "land_assets": ("toronto_land_assets.csv", {}),
        "apartment_registration": ("toronto_apartment_registration.csv", {}),
        "apartment_evaluations": ("toronto_apartment_evaluations.csv", {}),
        "housing_price_index": ("statcan_nhpi/18100205.csv", {"encoding": "utf-8-sig"}),
    }

    total = 0
    for table_name, (filename, kwargs) in sources.items():
        t0 = time.time()
        csv_path = RAW_DIR / filename
        if not csv_path.exists():
            print(f"  SKIP {table_name}: {csv_path} not found")
            continue

        df = pd.read_csv(csv_path, low_memory=False, **kwargs)
        df["_ingestion_timestamp"] = NOW
        df["_source"] = filename

        out_path = BRONZE_DIR / f"{table_name}.parquet"
        df.to_parquet(out_path, index=False, engine="pyarrow")

        total += len(df)
        print(f"  bronze.{table_name:30s} -> {len(df):>10,} rows  [{elapsed(t0)}]")

    print(f"\n  BRONZE TOTAL: {total:,} rows")
    return total


# =========================================================================
# SILVER LAYER --Clean, type, dedup, join
# =========================================================================

def read_bronze(name):
    return pd.read_parquet(BRONZE_DIR / f"{name}.parquet")


def write_silver(df, name):
    out = SILVER_DIR / f"{name}.parquet"
    df.to_parquet(out, index=False, engine="pyarrow")
    print(f"  silver.{name:30s} -> {len(df):>10,} rows")


def silver_properties():
    print("\n  --- Properties ---")
    t0 = time.time()
    df = read_bronze("property_boundaries")

    df = df.rename(columns={
        "PARCELID": "parcel_id",
        "FEATURE_TYPE": "feature_type",
        "DATE_EFFECTIVE": "date_effective",
        "STATEDAREA": "stated_area_sqm",
        "ADDRESS_NUMBER": "address_number",
        "LINEAR_NAME_FULL": "street_name",
        "OBJECTID": "object_id",
    })

    # Select and type
    cols = ["parcel_id", "feature_type", "date_effective", "stated_area_sqm",
            "address_number", "street_name", "object_id"]
    df = df[[c for c in cols if c in df.columns]].copy()

    df["date_effective"] = pd.to_datetime(df["date_effective"], errors="coerce")
    df["stated_area_sqm"] = pd.to_numeric(df["stated_area_sqm"], errors="coerce")
    df["full_address"] = df["address_number"].astype(str).str.strip() + " " + df["street_name"].astype(str).str.strip()

    # Filter
    df = df[df["parcel_id"].notna()].copy()

    # Dedup by parcel_id, keep latest date_effective
    df = df.sort_values("date_effective", ascending=False).drop_duplicates(subset="parcel_id", keep="first")

    # Derived
    df["stated_area_sqft"] = (df["stated_area_sqm"] * 10.7639).round(2)
    df["_silver_timestamp"] = NOW

    write_silver(df, "properties")
    print(f"    [{elapsed(t0)}]")


def silver_permits():
    print("\n  --- Building Permits ---")
    t0 = time.time()

    df_active = read_bronze("permits_active").assign(_permit_source="active")
    df_cleared = read_bronze("permits_cleared").assign(_permit_source="cleared")
    df = pd.concat([df_active, df_cleared], ignore_index=True)

    rename_map = {
        "PERMIT_NUM": "permit_number",
        "REVISION_NUM": "revision_number",
        "PERMIT_TYPE": "permit_type",
        "STRUCTURE_TYPE": "structure_type",
        "WORK": "work_type",
        "STREET_NUM": "street_number",
        "STREET_NAME": "street_name",
        "STREET_TYPE": "street_type",
        "STREET_DIRECTION": "street_direction",
        "POSTAL": "postal_code",
        "GEO_ID": "geo_id",
        "WARD_GRID": "ward",
        "APPLICATION_DATE": "application_date",
        "ISSUED_DATE": "issued_date",
        "COMPLETED_DATE": "completed_date",
        "STATUS": "status",
        "DESCRIPTION": "description",
        "CURRENT_USE": "current_use",
        "PROPOSED_USE": "proposed_use",
        "DWELLING_UNITS_CREATED": "dwelling_units_created",
        "DWELLING_UNITS_LOST": "dwelling_units_lost",
        "EST_CONST_COST": "estimated_construction_cost",
    }
    df = df.rename(columns=rename_map)

    # Type casting
    for c in ["application_date", "issued_date", "completed_date"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")
    for c in ["revision_number", "dwelling_units_created", "dwelling_units_lost"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["estimated_construction_cost"] = pd.to_numeric(df["estimated_construction_cost"], errors="coerce")

    # Clean strings
    for c in ["permit_number", "permit_type", "structure_type", "work_type", "status",
              "current_use", "proposed_use", "street_number", "street_name", "street_type",
              "street_direction", "postal_code", "ward"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    df = df[df["permit_number"].notna() & (df["permit_number"] != "nan")].copy()

    # Derived
    df["full_address"] = (df["street_number"] + " " + df["street_name"] + " " +
                          df["street_type"].fillna("") + " " + df["street_direction"].fillna("")).str.strip()
    df["application_year"] = df["application_date"].dt.year
    df["application_quarter"] = df["application_date"].dt.quarter
    df["net_dwelling_units"] = df["dwelling_units_created"].fillna(0) - df["dwelling_units_lost"].fillna(0)
    df["postal_prefix"] = df["postal_code"].str[:3]

    # Dedup: keep latest revision per permit
    df = df.sort_values("revision_number", ascending=False).drop_duplicates(subset="permit_number", keep="first")
    df["_silver_timestamp"] = NOW

    write_silver(df, "building_permits")
    print(f"    [{elapsed(t0)}]")


def silver_apartments():
    print("\n  --- Apartment Buildings ---")
    t0 = time.time()

    df_reg = read_bronze("apartment_registration")
    df_eval = read_bronze("apartment_evaluations")

    # Clean registration
    reg_rename = {
        "RSN": "rsn", "SITE_ADDRESS": "address", "PROPERTY_TYPE": "property_type",
        "WARD": "ward", "YEAR_BUILT": "year_built", "YEAR_REGISTERED": "year_registered",
        "HEATING_TYPE": "heating_type", "AIR_CONDITIONING_TYPE": "ac_type",
        "PARKING_TYPE": "parking_type", "NO_OF_ELEVATORS": "elevators",
        "PETS_ALLOWED": "pets_allowed", "LAUNDRY_ROOM": "has_laundry",
        "BIKE_PARKING": "has_bike_parking", "PCODE": "postal_code",
    }
    df_r = df_reg.rename(columns=reg_rename)

    # Handle storeys/units with coalesce
    df_r["storeys"] = pd.to_numeric(
        df_r.get("CONFIRMED_STOREYS", df_r.get("NO_OF_STOREYS", pd.Series(dtype="float"))),
        errors="coerce"
    )
    df_r["units"] = pd.to_numeric(
        df_r.get("CONFIRMED_UNITS", df_r.get("NO_OF_UNITS", pd.Series(dtype="float"))),
        errors="coerce"
    )

    reg_cols = ["rsn", "address", "property_type", "ward", "year_built", "year_registered",
                "storeys", "units", "heating_type", "ac_type", "parking_type", "elevators",
                "pets_allowed", "has_laundry", "has_bike_parking", "postal_code"]
    df_r = df_r[[c for c in reg_cols if c in df_r.columns]].copy()

    for c in ["rsn", "year_built", "year_registered", "elevators"]:
        df_r[c] = pd.to_numeric(df_r[c], errors="coerce")

    # Clean evaluations
    eval_rename = {
        "RSN": "rsn", "YEAR EVALUATED": "year_evaluated",
        "CURRENT BUILDING EVAL SCORE": "eval_score",
        "PROACTIVE BUILDING SCORE": "proactive_score",
        "CURRENT REACTIVE SCORE": "reactive_score",
        "NO OF AREAS EVALUATED": "areas_evaluated",
        "CONFIRMED STOREYS": "eval_storeys",
        "CONFIRMED UNITS": "eval_units",
        "LATITUDE": "latitude", "LONGITUDE": "longitude",
    }
    df_e = df_eval.rename(columns=eval_rename)
    eval_cols = ["rsn", "year_evaluated", "eval_score", "proactive_score",
                 "reactive_score", "areas_evaluated", "latitude", "longitude"]
    df_e = df_e[[c for c in eval_cols if c in df_e.columns]].copy()

    for c in df_e.columns:
        df_e[c] = pd.to_numeric(df_e[c], errors="coerce")

    # Keep latest evaluation per RSN
    df_e = df_e.sort_values("year_evaluated", ascending=False).drop_duplicates(subset="rsn", keep="first")

    # Join
    df = df_r.merge(df_e, on="rsn", how="left")
    df["building_age"] = 2025 - df["year_built"]
    df["units_per_storey"] = (df["units"] / df["storeys"]).round(1)
    df["postal_prefix"] = df["postal_code"].astype(str).str[:3]
    df["_silver_timestamp"] = NOW

    write_silver(df, "apartment_buildings")
    print(f"    [{elapsed(t0)}]")


def silver_hpi():
    print("\n  --- Housing Price Index ---")
    t0 = time.time()

    df = read_bronze("housing_price_index")

    ontario_keywords = ["Ontario", "Toronto", "Ottawa", "Hamilton", "Oshawa",
                        "Kitchener", "St. Catharines", "Windsor", "Greater Sudbury",
                        "Thunder Bay", "Guelph", "London", "Barrie", "Brantford",
                        "Peterborough", "Belleville"]

    mask = df["GEO"].apply(lambda x: any(k in str(x) for k in ontario_keywords))
    df = df[mask].copy()

    df = df.rename(columns={
        "REF_DATE": "ref_period",
        "GEO": "geography",
        "New housing price indexes": "index_type",
        "VALUE": "index_value",
        "STATUS": "data_quality_flag",
    })

    df["index_value"] = pd.to_numeric(df["index_value"], errors="coerce")
    df = df[df["index_value"].notna()].copy()

    df["reference_date"] = pd.to_datetime(df["ref_period"] + "-01", errors="coerce")
    df["ref_year"] = df["reference_date"].dt.year
    df["ref_month"] = df["reference_date"].dt.month
    df["ref_quarter"] = df["reference_date"].dt.quarter

    df["index_type"] = df["index_type"].str.replace('"', '').str.strip()

    cols = ["reference_date", "ref_period", "geography", "index_type",
            "index_value", "data_quality_flag", "ref_year", "ref_month", "ref_quarter"]
    df = df[cols].copy()
    df["_silver_timestamp"] = NOW

    write_silver(df, "housing_price_index")
    print(f"    [{elapsed(t0)}]")


def run_silver():
    print("\n" + "=" * 70)
    print("SILVER LAYER --Clean, Transform, Deduplicate")
    print("=" * 70)

    silver_properties()
    silver_permits()
    silver_apartments()
    silver_hpi()

    print("\n  SILVER SUMMARY:")
    for name in ["properties", "building_permits", "apartment_buildings", "housing_price_index"]:
        p = SILVER_DIR / f"{name}.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            print(f"    silver.{name:30s} -> {len(df):>10,} rows  ({len(df.columns)} cols)")


# =========================================================================
# GOLD LAYER --Analytics aggregations
# =========================================================================

def read_silver(name):
    return pd.read_parquet(SILVER_DIR / f"{name}.parquet")


def write_gold(df, name):
    out = GOLD_DIR / f"{name}.parquet"
    df.to_parquet(out, index=False, engine="pyarrow")
    print(f"  gold.{name:30s} -> {len(df):>10,} rows")


def gold_permit_trends():
    print("\n  --- Permit Trends ---")
    t0 = time.time()
    df = read_silver("building_permits")
    df = df[df["application_year"].notna() & (df["application_year"] >= 2017)].copy()

    group_cols = ["application_year", "application_quarter", "ward", "permit_type",
                  "structure_type", "work_type", "status"]
    agg = df.groupby(group_cols, dropna=False).agg(
        permit_count=("permit_number", "count"),
        avg_construction_cost=("estimated_construction_cost", "mean"),
        total_construction_cost=("estimated_construction_cost", "sum"),
        total_units_created=("dwelling_units_created", "sum"),
        total_units_lost=("dwelling_units_lost", "sum"),
        net_dwelling_units=("net_dwelling_units", "sum"),
        unique_permits=("permit_number", "nunique"),
    ).reset_index()

    agg["avg_construction_cost"] = agg["avg_construction_cost"].round(2)
    agg["total_construction_cost"] = agg["total_construction_cost"].round(2)

    write_gold(agg, "permit_trends")
    print(f"    [{elapsed(t0)}]")


def gold_construction_investment():
    print("\n  --- Construction Investment ---")
    t0 = time.time()
    df = read_silver("building_permits")
    df = df[
        df["estimated_construction_cost"].notna()
        & (df["estimated_construction_cost"] > 0)
        & (df["application_year"] >= 2017)
    ].copy()

    group_cols = ["application_year", "application_quarter", "ward", "current_use", "proposed_use"]
    agg = df.groupby(group_cols, dropna=False).agg(
        project_count=("permit_number", "count"),
        total_investment=("estimated_construction_cost", "sum"),
        avg_investment=("estimated_construction_cost", "mean"),
        median_investment=("estimated_construction_cost", "median"),
        max_investment=("estimated_construction_cost", "max"),
        units_created=("dwelling_units_created", "sum"),
    ).reset_index()

    agg["total_investment"] = agg["total_investment"].round(2)
    agg["avg_investment"] = agg["avg_investment"].round(2)
    agg["median_investment"] = agg["median_investment"].round(2)

    agg["investment_category"] = pd.cut(
        agg["avg_investment"],
        bins=[-np.inf, 100_000, 1_000_000, 10_000_000, np.inf],
        labels=["Small (<$100K)", "Medium ($100K-$1M)", "Major ($1M-$10M)", "Mega (>$10M)"],
    )

    write_gold(agg, "construction_investment")
    print(f"    [{elapsed(t0)}]")


def gold_apartment_scorecard():
    print("\n  --- Apartment Scorecard ---")
    t0 = time.time()
    df = read_silver("apartment_buildings")

    agg = df.groupby(["ward", "property_type"], dropna=False).agg(
        building_count=("rsn", "count"),
        avg_eval_score=("eval_score", "mean"),
        avg_proactive_score=("proactive_score", "mean"),
        avg_reactive_score=("reactive_score", "mean"),
        avg_units=("units", "mean"),
        avg_storeys=("storeys", "mean"),
        avg_building_age=("building_age", "mean"),
        total_units=("units", "sum"),
        total_elevators=("elevators", "sum"),
        avg_year_built=("year_built", "mean"),
        oldest_building=("year_built", "min"),
        newest_building=("year_built", "max"),
    ).reset_index()

    for c in ["avg_eval_score", "avg_proactive_score", "avg_reactive_score",
              "avg_units", "avg_storeys", "avg_building_age", "avg_year_built"]:
        agg[c] = agg[c].round(1)

    agg["quality_tier"] = pd.cut(
        agg["avg_eval_score"],
        bins=[-np.inf, 40, 60, 80, np.inf],
        labels=["Needs Improvement", "Fair", "Good", "Excellent"],
    )

    write_gold(agg, "apartment_scorecard")
    print(f"    [{elapsed(t0)}]")


def gold_housing_development():
    print("\n  --- Housing Development ---")
    t0 = time.time()
    df = read_silver("building_permits")
    df = df[
        (df["application_year"] >= 2017)
        & (df["dwelling_units_created"].notna() | df["dwelling_units_lost"].notna())
    ].copy()

    group_cols = ["application_year", "application_quarter", "ward", "structure_type", "proposed_use"]
    agg = df.groupby(group_cols, dropna=False).agg(
        permit_count=("permit_number", "count"),
        units_created=("dwelling_units_created", "sum"),
        units_lost=("dwelling_units_lost", "sum"),
        net_units=("net_dwelling_units", "sum"),
        total_cost=("estimated_construction_cost", "sum"),
    ).reset_index()

    agg["total_cost"] = agg["total_cost"].round(2)

    # Cumulative net units by ward
    agg = agg.sort_values(["ward", "application_year", "application_quarter"])
    agg["cumulative_net_units"] = agg.groupby("ward")["net_units"].cumsum()

    write_gold(agg, "housing_development")
    print(f"    [{elapsed(t0)}]")


def gold_price_index():
    print("\n  --- Price Index Trends ---")
    t0 = time.time()
    df = read_silver("housing_price_index")

    # Pivot index types into columns
    pivot = df.pivot_table(
        index=["reference_date", "ref_period", "geography", "ref_year", "ref_month", "ref_quarter"],
        columns="index_type",
        values="index_value",
        aggfunc="first",
    ).reset_index()

    pivot.columns.name = None

    rename = {}
    for c in pivot.columns:
        if "Total" in str(c) and "land" in str(c).lower():
            rename[c] = "index_total"
        elif "House only" in str(c):
            rename[c] = "index_house"
        elif "Land only" in str(c):
            rename[c] = "index_land"
    pivot = pivot.rename(columns=rename)

    # YoY change
    if "index_total" in pivot.columns:
        pivot = pivot.sort_values(["geography", "reference_date"])
        pivot["index_total_12m_ago"] = pivot.groupby("geography")["index_total"].shift(12)
        pivot["yoy_change_pct"] = (
            (pivot["index_total"] - pivot["index_total_12m_ago"]) / pivot["index_total_12m_ago"] * 100
        ).round(2)

    write_gold(pivot, "price_index_trends")
    print(f"    [{elapsed(t0)}]")


def gold_property_overview():
    print("\n  --- Property Overview ---")
    t0 = time.time()
    df = read_silver("properties")

    agg = df.groupby("feature_type").agg(
        property_count=("parcel_id", "count"),
        avg_area_sqm=("stated_area_sqm", "mean"),
        avg_area_sqft=("stated_area_sqft", "mean"),
        median_area_sqm=("stated_area_sqm", "median"),
        min_area_sqm=("stated_area_sqm", "min"),
        max_area_sqm=("stated_area_sqm", "max"),
        total_area_sqm=("stated_area_sqm", "sum"),
    ).reset_index()

    for c in ["avg_area_sqm", "avg_area_sqft", "median_area_sqm", "total_area_sqm"]:
        agg[c] = agg[c].round(2)

    write_gold(agg, "property_overview")
    print(f"    [{elapsed(t0)}]")


def run_gold():
    print("\n" + "=" * 70)
    print("GOLD LAYER --Analytics Aggregations")
    print("=" * 70)

    gold_permit_trends()
    gold_construction_investment()
    gold_apartment_scorecard()
    gold_housing_development()
    gold_price_index()
    gold_property_overview()

    print("\n  GOLD SUMMARY:")
    for name in ["permit_trends", "construction_investment", "apartment_scorecard",
                  "housing_development", "price_index_trends", "property_overview"]:
        p = GOLD_DIR / f"{name}.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            print(f"    gold.{name:30s} -> {len(df):>10,} rows  ({len(df.columns)} cols)")


# =========================================================================
# MAIN
# =========================================================================

if __name__ == "__main__":
    start = time.time()

    print("=" * 70)
    print("  Ontario Real Estate -- Local Pipeline (Pandas + Parquet)")
    print("=" * 70)

    run_bronze()
    run_silver()
    run_gold()

    print(f"\n{'=' * 70}")
    print(f"PIPELINE COMPLETE --Total time: {elapsed(start)}")
    print(f"{'=' * 70}")
    print(f"\nNext: cd streamlit_app && streamlit run app.py")
