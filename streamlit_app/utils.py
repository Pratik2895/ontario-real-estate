"""Utility functions for the Ontario Real Estate Streamlit dashboard."""

import pandas as pd
from pathlib import Path

GOLD_DIR = Path(__file__).resolve().parent.parent / "data" / "gold"


def load_gold(table_name: str) -> pd.DataFrame:
    """Load a Gold table from local parquet."""
    path = GOLD_DIR / f"{table_name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Gold table not found: {path}. Run the pipeline first.")
    return pd.read_parquet(path)


def load_permit_trends() -> pd.DataFrame:
    return load_gold("permit_trends")


def load_construction_investment() -> pd.DataFrame:
    return load_gold("construction_investment")


def load_apartment_scorecard() -> pd.DataFrame:
    return load_gold("apartment_scorecard")


def load_housing_development() -> pd.DataFrame:
    return load_gold("housing_development")


def load_price_index_trends() -> pd.DataFrame:
    df = load_gold("price_index_trends")
    if "reference_date" in df.columns:
        df["reference_date"] = pd.to_datetime(df["reference_date"])
    return df


def load_property_overview() -> pd.DataFrame:
    return load_gold("property_overview")


def format_currency(val: float) -> str:
    """Format a number as CAD currency."""
    if pd.isna(val):
        return "N/A"
    if abs(val) >= 1_000_000_000:
        return f"${val / 1_000_000_000:.2f}B"
    if abs(val) >= 1_000_000:
        return f"${val / 1_000_000:.1f}M"
    if abs(val) >= 1_000:
        return f"${val / 1_000:.0f}K"
    return f"${val:,.0f}"


def format_number(val: float) -> str:
    """Format a large number with suffix."""
    if pd.isna(val):
        return "N/A"
    if abs(val) >= 1_000_000:
        return f"{val / 1_000_000:.1f}M"
    if abs(val) >= 1_000:
        return f"{val / 1_000:.1f}K"
    return f"{val:,.0f}"
