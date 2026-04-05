"""
Download authentic Ontario real estate data from open data portals.
Sources:
  - Toronto Open Data (CKAN): Property boundaries, building permits, apartments
  - Statistics Canada: New Housing Price Index
Run: python notebooks/00_download_data.py
"""

import os
import zipfile
import urllib.request
from pathlib import Path

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Data sources — all publicly available under open government licences
# ---------------------------------------------------------------------------

SOURCES = {
    # Toronto Open Data — CKAN
    "toronto_property_boundaries.csv": (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/"
        "1acaa8b0-f235-4df6-8305-02025ccdeb07/resource/"
        "23d1f792-018f-4069-ac5d-443e932e1b78/download/property-boundaries-4326.csv"
    ),
    "toronto_permits_active.csv": (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/"
        "108c2bd1-6945-46f6-af92-02f5658ee7f7/resource/"
        "dfce3b7b-4f17-4a9d-9155-5e390a5ffa97/download/building-permits-active-permits.csv"
    ),
    "toronto_permits_cleared.csv": (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/"
        "9e42a85b-180f-4dc5-b0d7-d46661a6c0ec/resource/"
        "b41c3e9e-4d2d-4b09-a789-9569d8da407c/download/cleared-building-permits-since-2017.csv"
    ),
    "toronto_building_assets.csv": (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/"
        "a1561e68-446a-4141-9c4e-7fb376db6a20/resource/"
        "5af250fd-a0d1-4c4f-ae7b-eed72bb1d965/download/building-asset-inventory-4326.csv"
    ),
    "toronto_land_assets.csv": (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/"
        "a1561e68-446a-4141-9c4e-7fb376db6a20/resource/"
        "278d29e2-5807-4253-86e2-2a3f3d6d1c1c/download/land-asset-inventory-4326.csv"
    ),
    "toronto_apartment_registration.csv": (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/"
        "2b98b3f3-4f3a-42a4-a4e9-b44d3026595a/resource/"
        "97b8b7a4-baca-49c7-915d-335322dbcf95/download/apartment-building-registration-data.csv"
    ),
    "toronto_apartment_evaluations.csv": (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/"
        "4ef82789-e038-44ef-a478-a8f3590c3eb1/resource/"
        "7fa98ab2-7412-43cd-9270-cb44dd75b573/download/"
        "apartment-building-evaluations-2023-current.csv"
    ),
}

STATCAN_ZIP_URL = "https://www150.statcan.gc.ca/n1/tbl/csv/18100205-eng.zip"


def download_file(url: str, dest: Path):
    """Download a file with progress indication."""
    if dest.exists():
        print(f"  SKIP (exists): {dest.name}")
        return
    print(f"  Downloading: {dest.name} ...")
    urllib.request.urlretrieve(url, dest)
    size_mb = dest.stat().st_size / (1024 * 1024)
    print(f"  Done: {dest.name} ({size_mb:.1f} MB)")


def main():
    print("=" * 60)
    print("Ontario Real Estate — Data Download")
    print("=" * 60)

    # --- Toronto Open Data files ---
    print("\n[1/2] Toronto Open Data (CKAN)")
    for filename, url in SOURCES.items():
        download_file(url, RAW_DIR / filename)

    # --- Statistics Canada ---
    print("\n[2/2] Statistics Canada — New Housing Price Index")
    zip_path = RAW_DIR / "statcan_nhpi.zip"
    nhpi_dir = RAW_DIR / "statcan_nhpi"
    nhpi_csv = nhpi_dir / "18100205.csv"

    if nhpi_csv.exists():
        print(f"  SKIP (exists): {nhpi_csv.name}")
    else:
        download_file(STATCAN_ZIP_URL, zip_path)
        nhpi_dir.mkdir(exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(nhpi_dir)
        print(f"  Extracted to: {nhpi_dir}")

    # --- Summary ---
    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    total_rows = 0
    for f in sorted(RAW_DIR.glob("*.csv")):
        with open(f, "r", encoding="utf-8", errors="replace") as fh:
            rows = sum(1 for _ in fh) - 1  # minus header
        total_rows += rows
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name:45s} {rows:>10,} rows  {size_mb:>7.1f} MB")

    if nhpi_csv.exists():
        with open(nhpi_csv, "r", encoding="utf-8-sig") as fh:
            rows = sum(1 for _ in fh) - 1
        total_rows += rows
        size_mb = nhpi_csv.stat().st_size / (1024 * 1024)
        print(f"  {'statcan_nhpi/18100205.csv':45s} {rows:>10,} rows  {size_mb:>7.1f} MB")

    print(f"\n  TOTAL: {total_rows:,} records")
    print("=" * 60)


if __name__ == "__main__":
    main()
