"""
Ontario Real Estate Analytics Dashboard
Data: 1.2M+ records from Toronto Open Data & Statistics Canada
Run: streamlit run streamlit_app/app.py
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils import (
    load_permit_trends,
    load_construction_investment,
    load_apartment_scorecard,
    load_housing_development,
    load_price_index_trends,
    load_property_overview,
    format_currency,
    format_number,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Ontario Real Estate Analytics",
    page_icon="🏠",
    layout="wide",
)

st.title("Ontario Real Estate Analytics")
st.caption(
    "1.2M+ records | Sources: Toronto Open Data (CKAN), Statistics Canada | "
    "Architecture: Bronze → Silver → Gold (Delta Lake)"
)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def get_all_data():
    return {
        "permits": load_permit_trends(),
        "investment": load_construction_investment(),
        "apartments": load_apartment_scorecard(),
        "housing_dev": load_housing_development(),
        "price_index": load_price_index_trends(),
        "properties": load_property_overview(),
    }

data = get_all_data()
df_permits = data["permits"]
df_invest = data["investment"]
df_apts = data["apartments"]
df_housing = data["housing_dev"]
df_price = data["price_index"]
df_props = data["properties"]

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
st.sidebar.header("Filters")

# Year filter (applies to permit-based tabs)
all_years = sorted(df_permits["application_year"].dropna().unique())
year_range = st.sidebar.slider(
    "Year Range",
    min_value=int(min(all_years)),
    max_value=int(max(all_years)),
    value=(int(min(all_years)), int(max(all_years))),
)

# Ward filter
all_wards = sorted(df_permits["ward"].dropna().unique())
sel_wards = st.sidebar.multiselect("Ward", all_wards)

# Permit type filter
all_permit_types = sorted(df_permits["permit_type"].dropna().unique())
sel_permit_types = st.sidebar.multiselect("Permit Type", all_permit_types)

# Apply filters
def filter_permits(df):
    df = df[df["application_year"].between(*year_range)]
    if sel_wards:
        df = df[df["ward"].isin(sel_wards)]
    if sel_permit_types:
        df = df[df["permit_type"].isin(sel_permit_types)]
    return df

df_permits_f = filter_permits(df_permits)
df_invest_f = df_invest[df_invest["application_year"].between(*year_range)]
if sel_wards:
    df_invest_f = df_invest_f[df_invest_f["ward"].isin(sel_wards)]

df_housing_f = df_housing[df_housing["application_year"].between(*year_range)]
if sel_wards:
    df_housing_f = df_housing_f[df_housing_f["ward"].isin(sel_wards)]

# ---------------------------------------------------------------------------
# KPI Cards
# ---------------------------------------------------------------------------
st.markdown("---")

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

total_permits = int(df_permits_f["permit_count"].sum())
total_investment = df_invest_f["total_investment"].sum()
net_units = int(df_housing_f["net_units"].sum())
avg_apt_score = df_apts["avg_eval_score"].mean() if len(df_apts) > 0 else 0
total_properties = int(df_props["property_count"].sum())

kpi1.metric("Total Permits", format_number(total_permits))
kpi2.metric("Construction Investment", format_currency(total_investment))
kpi3.metric("Net Dwelling Units", format_number(net_units))
kpi4.metric("Avg Apartment Score", f"{avg_apt_score:.1f}/100")
kpi5.metric("Total Properties", format_number(total_properties))

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Permit Trends",
    "Construction Investment",
    "Housing Development",
    "Apartment Quality",
    "Price Index",
    "Property Overview",
])

# ---- Tab 1: Permit Trends ------------------------------------------------
with tab1:
    st.subheader("Building Permit Activity Over Time")

    # Permits by year
    yearly = (
        df_permits_f
        .groupby("application_year")
        .agg({"permit_count": "sum", "unique_permits": "sum"})
        .reset_index()
    )

    fig_yr = px.bar(
        yearly,
        x="application_year",
        y="permit_count",
        labels={"permit_count": "Permits", "application_year": "Year"},
        color_discrete_sequence=["#3498db"],
    )
    fig_yr.update_layout(height=400)
    st.plotly_chart(fig_yr, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("By Permit Type")
        by_type = (
            df_permits_f
            .groupby("permit_type")["permit_count"]
            .sum()
            .reset_index()
            .sort_values("permit_count", ascending=False)
        )
        fig_type = px.pie(by_type, values="permit_count", names="permit_type", hole=0.4)
        fig_type.update_layout(height=400)
        st.plotly_chart(fig_type, use_container_width=True)

    with col2:
        st.subheader("By Structure Type")
        by_struct = (
            df_permits_f
            .groupby("structure_type")["permit_count"]
            .sum()
            .reset_index()
            .sort_values("permit_count", ascending=False)
            .head(10)
        )
        fig_struct = px.bar(
            by_struct, x="permit_count", y="structure_type",
            orientation="h", color="permit_count",
            color_continuous_scale="Viridis",
        )
        fig_struct.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_struct, use_container_width=True)

    # Top wards by permit count
    st.subheader("Top 15 Wards by Permit Volume")
    by_ward = (
        df_permits_f
        .groupby("ward")["permit_count"]
        .sum()
        .reset_index()
        .sort_values("permit_count", ascending=True)
        .tail(15)
    )
    fig_ward = px.bar(
        by_ward, x="permit_count", y="ward", orientation="h",
        color="permit_count", color_continuous_scale="YlOrRd",
    )
    fig_ward.update_layout(height=500, showlegend=False)
    st.plotly_chart(fig_ward, use_container_width=True)

# ---- Tab 2: Construction Investment --------------------------------------
with tab2:
    st.subheader("Construction Investment Trends")

    inv_yearly = (
        df_invest_f
        .groupby("application_year")
        .agg({"total_investment": "sum", "project_count": "sum", "avg_investment": "mean"})
        .reset_index()
    )

    fig_inv = go.Figure()
    fig_inv.add_trace(go.Bar(
        x=inv_yearly["application_year"],
        y=inv_yearly["total_investment"],
        name="Total Investment ($)",
        marker_color="#2ecc71",
    ))
    fig_inv.add_trace(go.Scatter(
        x=inv_yearly["application_year"],
        y=inv_yearly["project_count"],
        name="Project Count",
        yaxis="y2",
        marker_color="#e74c3c",
        mode="lines+markers",
    ))
    fig_inv.update_layout(
        height=450,
        yaxis=dict(title="Total Investment ($)"),
        yaxis2=dict(title="Projects", overlaying="y", side="right"),
        legend=dict(x=0.01, y=0.99),
    )
    st.plotly_chart(fig_inv, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("By Investment Category")
        if "investment_category" in df_invest_f.columns:
            by_cat = (
                df_invest_f
                .groupby("investment_category")
                .agg({"project_count": "sum", "total_investment": "sum"})
                .reset_index()
            )
            fig_cat = px.pie(
                by_cat, values="total_investment", names="investment_category",
                hole=0.4, color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig_cat.update_layout(height=400)
            st.plotly_chart(fig_cat, use_container_width=True)

    with col2:
        st.subheader("Top Current Uses by Investment")
        by_use = (
            df_invest_f
            .groupby("current_use")["total_investment"]
            .sum()
            .reset_index()
            .dropna()
            .sort_values("total_investment", ascending=True)
            .tail(10)
        )
        fig_use = px.bar(
            by_use, x="total_investment", y="current_use", orientation="h",
            color="total_investment", color_continuous_scale="Plasma",
        )
        fig_use.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_use, use_container_width=True)

# ---- Tab 3: Housing Development ------------------------------------------
with tab3:
    st.subheader("Net Dwelling Unit Creation")

    dev_yearly = (
        df_housing_f
        .groupby("application_year")
        .agg({"units_created": "sum", "units_lost": "sum", "net_units": "sum"})
        .reset_index()
    )

    fig_dev = go.Figure()
    fig_dev.add_trace(go.Bar(
        x=dev_yearly["application_year"], y=dev_yearly["units_created"],
        name="Units Created", marker_color="#2ecc71",
    ))
    fig_dev.add_trace(go.Bar(
        x=dev_yearly["application_year"], y=-dev_yearly["units_lost"].abs(),
        name="Units Lost", marker_color="#e74c3c",
    ))
    fig_dev.add_trace(go.Scatter(
        x=dev_yearly["application_year"], y=dev_yearly["net_units"],
        name="Net Units", mode="lines+markers", marker_color="#3498db",
        line=dict(width=3),
    ))
    fig_dev.update_layout(height=450, barmode="relative")
    st.plotly_chart(fig_dev, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("By Structure Type")
        dev_struct = (
            df_housing_f
            .groupby("structure_type")["net_units"]
            .sum()
            .reset_index()
            .dropna()
            .sort_values("net_units", ascending=True)
            .tail(10)
        )
        fig_ds = px.bar(
            dev_struct, x="net_units", y="structure_type", orientation="h",
            color="net_units", color_continuous_scale="RdYlGn",
        )
        fig_ds.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_ds, use_container_width=True)

    with col2:
        st.subheader("Top Wards by Net Units")
        dev_ward = (
            df_housing_f
            .groupby("ward")["net_units"]
            .sum()
            .reset_index()
            .sort_values("net_units", ascending=True)
            .tail(15)
        )
        fig_dw = px.bar(
            dev_ward, x="net_units", y="ward", orientation="h",
            color="net_units", color_continuous_scale="Viridis",
        )
        fig_dw.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_dw, use_container_width=True)

# ---- Tab 4: Apartment Quality --------------------------------------------
with tab4:
    st.subheader("Apartment Building Quality by Ward (RentSafeTO)")

    if len(df_apts) > 0:
        # Scorecard table
        display_cols = [
            "ward", "property_type", "building_count", "avg_eval_score",
            "avg_proactive_score", "avg_reactive_score", "total_units",
            "avg_building_age", "quality_tier",
        ]
        available_cols = [c for c in display_cols if c in df_apts.columns]
        df_display = df_apts[available_cols].sort_values("building_count", ascending=False)

        st.dataframe(
            df_display.style.format({
                "avg_eval_score": "{:.1f}",
                "avg_proactive_score": "{:.1f}",
                "avg_reactive_score": "{:.1f}",
                "avg_building_age": "{:.0f}",
            }),
            use_container_width=True,
            height=500,
        )

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Quality Tier Distribution")
            if "quality_tier" in df_apts.columns:
                tier_counts = df_apts.groupby("quality_tier")["building_count"].sum().reset_index()
                fig_tier = px.pie(
                    tier_counts, values="building_count", names="quality_tier",
                    color="quality_tier",
                    color_discrete_map={
                        "Excellent": "#27ae60", "Good": "#2ecc71",
                        "Fair": "#f39c12", "Needs Improvement": "#e74c3c",
                    },
                    hole=0.4,
                )
                fig_tier.update_layout(height=400)
                st.plotly_chart(fig_tier, use_container_width=True)

        with col2:
            st.subheader("Avg Score vs Building Age")
            fig_scatter = px.scatter(
                df_apts.dropna(subset=["avg_eval_score", "avg_building_age"]),
                x="avg_building_age",
                y="avg_eval_score",
                size="total_units",
                color="quality_tier",
                color_discrete_map={
                    "Excellent": "#27ae60", "Good": "#2ecc71",
                    "Fair": "#f39c12", "Needs Improvement": "#e74c3c",
                },
                labels={"avg_building_age": "Avg Building Age (yrs)", "avg_eval_score": "Eval Score"},
            )
            fig_scatter.update_layout(height=400)
            st.plotly_chart(fig_scatter, use_container_width=True)

# ---- Tab 5: Price Index --------------------------------------------------
with tab5:
    st.subheader("Ontario New Housing Price Index (Statistics Canada)")

    if len(df_price) > 0:
        # Geography filter for this tab
        all_geos = sorted(df_price["geography"].unique())
        sel_geos = st.multiselect("Select Geographies", all_geos, default=all_geos[:5])

        df_price_f = df_price[df_price["geography"].isin(sel_geos)] if sel_geos else df_price

        # Total index over time
        if "index_total" in df_price_f.columns:
            fig_idx = px.line(
                df_price_f.dropna(subset=["index_total"]),
                x="reference_date",
                y="index_total",
                color="geography",
                labels={"index_total": "Index (Dec 2016 = 100)", "reference_date": "Date"},
            )
            fig_idx.update_layout(height=500, hovermode="x unified")
            st.plotly_chart(fig_idx, use_container_width=True)

        # House vs Land index
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("House-Only Index")
            if "index_house" in df_price_f.columns:
                fig_h = px.line(
                    df_price_f.dropna(subset=["index_house"]),
                    x="reference_date", y="index_house", color="geography",
                )
                fig_h.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig_h, use_container_width=True)

        with col2:
            st.subheader("Land-Only Index")
            if "index_land" in df_price_f.columns:
                fig_l = px.line(
                    df_price_f.dropna(subset=["index_land"]),
                    x="reference_date", y="index_land", color="geography",
                )
                fig_l.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig_l, use_container_width=True)

        # YoY change
        st.subheader("Year-over-Year Change (%)")
        if "yoy_change_pct" in df_price_f.columns:
            fig_yoy = px.bar(
                df_price_f.dropna(subset=["yoy_change_pct"]),
                x="reference_date", y="yoy_change_pct", color="geography",
                barmode="group",
            )
            fig_yoy.update_layout(height=400)
            st.plotly_chart(fig_yoy, use_container_width=True)

# ---- Tab 6: Property Overview ---------------------------------------------
with tab6:
    st.subheader("Property Parcel Overview (528K+ Properties)")

    if len(df_props) > 0:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Properties by Feature Type")
            fig_pt = px.bar(
                df_props.sort_values("property_count", ascending=True),
                x="property_count", y="feature_type", orientation="h",
                color="property_count", color_continuous_scale="Viridis",
            )
            fig_pt.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig_pt, use_container_width=True)

        with col2:
            st.subheader("Avg Parcel Size by Type")
            fig_area = px.bar(
                df_props.sort_values("avg_area_sqft", ascending=True),
                x="avg_area_sqft", y="feature_type", orientation="h",
                color="avg_area_sqft", color_continuous_scale="YlOrRd",
                labels={"avg_area_sqft": "Avg Area (sqft)"},
            )
            fig_area.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig_area, use_container_width=True)

        # Summary stats table
        st.subheader("Detailed Statistics")
        st.dataframe(
            df_props.style.format({
                "avg_area_sqm": "{:,.1f}",
                "avg_area_sqft": "{:,.1f}",
                "median_area_sqm": "{:,.1f}",
                "total_area_sqm": "{:,.0f}",
                "property_count": "{:,}",
            }),
            use_container_width=True,
        )

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown(
    "**Data Sources:** "
    "[Toronto Open Data](https://open.toronto.ca) (Open Government Licence - Toronto) | "
    "[Statistics Canada](https://www150.statcan.gc.ca) (Open Government Licence - Canada)  \n"
    "**Architecture:** Bronze → Silver → Gold (Delta Lake) | "
    "**Records:** 1.2M+ across 8 source tables"
)
