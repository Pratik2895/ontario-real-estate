"""Tests for Silver layer transform logic."""

import pandas as pd
import numpy as np
import pytest


class TestPermitTransforms:
    """Tests for building permit cleaning logic."""

    def test_dedup_keeps_latest_revision(self):
        df = pd.DataFrame({
            "permit_number": ["P001", "P001", "P002"],
            "revision_number": [0, 1, 0],
            "estimated_construction_cost": [100000, 150000, 200000],
        })
        deduped = (
            df.sort_values("revision_number", ascending=False)
            .drop_duplicates(subset="permit_number", keep="first")
        )
        assert len(deduped) == 2
        p001 = deduped[deduped["permit_number"] == "P001"].iloc[0]
        assert p001["estimated_construction_cost"] == 150000

    def test_net_dwelling_units(self):
        df = pd.DataFrame({
            "dwelling_units_created": [10, 0, 5, None],
            "dwelling_units_lost": [2, 3, 0, None],
        })
        df["net"] = df["dwelling_units_created"] - df["dwelling_units_lost"]
        assert df.loc[0, "net"] == 8
        assert df.loc[1, "net"] == -3
        assert df.loc[2, "net"] == 5

    def test_postal_prefix(self):
        df = pd.DataFrame({"postal_code": ["M5V 2H1", "K1A 0B1", None, "L4M"]})
        df["postal_prefix"] = df["postal_code"].str[:3]
        assert df.loc[0, "postal_prefix"] == "M5V"
        assert df.loc[1, "postal_prefix"] == "K1A"
        assert pd.isna(df.loc[2, "postal_prefix"])

    def test_filter_null_permits(self):
        df = pd.DataFrame({
            "permit_number": ["P001", None, "P003", "", "P005"],
        })
        valid = df[df["permit_number"].notna() & (df["permit_number"] != "")]
        assert len(valid) == 3


class TestApartmentTransforms:
    """Tests for apartment building cleaning logic."""

    def test_building_age(self):
        df = pd.DataFrame({"year_built": [1970, 2000, 2020]})
        df["building_age"] = 2025 - df["year_built"]
        assert df.loc[0, "building_age"] == 55
        assert df.loc[2, "building_age"] == 5

    def test_units_per_storey(self):
        df = pd.DataFrame({"units": [100, 24, 0], "storeys": [10, 3, 5]})
        df["units_per_storey"] = df["units"] / df["storeys"]
        assert df.loc[0, "units_per_storey"] == 10.0
        assert df.loc[1, "units_per_storey"] == 8.0

    def test_quality_tier(self):
        scores = [85, 65, 45, 25]
        tiers = []
        for s in scores:
            if s >= 80:
                tiers.append("Excellent")
            elif s >= 60:
                tiers.append("Good")
            elif s >= 40:
                tiers.append("Fair")
            else:
                tiers.append("Needs Improvement")
        assert tiers == ["Excellent", "Good", "Fair", "Needs Improvement"]

    def test_eval_dedup_keeps_latest(self):
        df = pd.DataFrame({
            "rsn": [1001, 1001, 1002],
            "year_evaluated": [2022, 2023, 2023],
            "eval_score": [60.0, 75.0, 80.0],
        })
        latest = (
            df.sort_values("year_evaluated", ascending=False)
            .drop_duplicates(subset="rsn", keep="first")
        )
        assert len(latest) == 2
        r1001 = latest[latest["rsn"] == 1001].iloc[0]
        assert r1001["eval_score"] == 75.0


class TestHousingPriceIndex:
    """Tests for housing price index cleaning."""

    def test_ontario_filter(self):
        geos = ["Ontario", "Toronto", "British Columbia", "Alberta", "Ottawa-Gatineau, Ontario part"]
        ontario_keywords = ["Ontario", "Toronto", "Ottawa", "Hamilton"]
        filtered = [g for g in geos if any(k in g for k in ontario_keywords)]
        assert "British Columbia" not in filtered
        assert "Alberta" not in filtered
        assert "Ontario" in filtered
        assert "Ottawa-Gatineau, Ontario part" in filtered

    def test_yoy_change(self):
        current = 120.5
        prior = 110.0
        yoy = (current - prior) / prior * 100
        assert round(yoy, 2) == 9.55

    def test_null_index_filter(self):
        df = pd.DataFrame({
            "index_value": [100.0, None, 105.0, np.nan, 110.0],
        })
        valid = df[df["index_value"].notna()]
        assert len(valid) == 3


class TestPropertyTransforms:
    """Tests for property boundary cleaning."""

    def test_area_conversion(self):
        sqm = 100.0
        sqft = sqm * 10.7639
        assert round(sqft, 2) == 1076.39

    def test_property_dedup(self):
        df = pd.DataFrame({
            "parcel_id": ["A", "A", "B"],
            "date_effective": pd.to_datetime(["2023-01-01", "2024-01-01", "2023-06-01"]),
            "stated_area_sqm": [500.0, 520.0, 300.0],
        })
        deduped = (
            df.sort_values("date_effective", ascending=False)
            .drop_duplicates(subset="parcel_id", keep="first")
        )
        assert len(deduped) == 2
        a_row = deduped[deduped["parcel_id"] == "A"].iloc[0]
        assert a_row["stated_area_sqm"] == 520.0
