-- Gold Layer DDL — Ontario Real Estate
-- Run in Databricks SQL Editor

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.permit_trends (
    application_year           INT,
    application_quarter        INT,
    ward                       STRING,
    permit_type                STRING,
    structure_type             STRING,
    work_type                  STRING,
    status                     STRING,
    permit_count               BIGINT,
    avg_construction_cost      DOUBLE,
    total_construction_cost    DOUBLE,
    total_units_created        BIGINT,
    total_units_lost           BIGINT,
    net_dwelling_units         BIGINT,
    unique_permits             BIGINT
)
USING DELTA
COMMENT 'Building permit activity by year/quarter/ward/type';

CREATE TABLE IF NOT EXISTS gold.construction_investment (
    application_year       INT,
    application_quarter    INT,
    ward                   STRING,
    current_use            STRING,
    proposed_use           STRING,
    project_count          BIGINT,
    total_investment       DOUBLE,
    avg_investment         DOUBLE,
    median_investment      DOUBLE,
    max_investment         DOUBLE,
    units_created          BIGINT,
    investment_category    STRING
)
USING DELTA
COMMENT 'Construction investment aggregations';

CREATE TABLE IF NOT EXISTS gold.apartment_scorecard (
    ward                   STRING,
    property_type          STRING,
    building_count         BIGINT,
    avg_eval_score         DOUBLE,
    avg_proactive_score    DOUBLE,
    avg_reactive_score     DOUBLE,
    avg_units              DOUBLE,
    avg_storeys            DOUBLE,
    avg_building_age       DOUBLE,
    total_units            BIGINT,
    total_elevators        BIGINT,
    avg_year_built         DOUBLE,
    oldest_building        INT,
    newest_building        INT,
    quality_tier           STRING
)
USING DELTA
COMMENT 'Apartment building quality KPIs by ward';

CREATE TABLE IF NOT EXISTS gold.housing_development (
    application_year       INT,
    application_quarter    INT,
    ward                   STRING,
    structure_type         STRING,
    proposed_use           STRING,
    permit_count           BIGINT,
    units_created          BIGINT,
    units_lost             BIGINT,
    net_units              BIGINT,
    total_cost             DOUBLE,
    cumulative_net_units   BIGINT
)
USING DELTA
COMMENT 'Net dwelling unit creation trends';

CREATE TABLE IF NOT EXISTS gold.price_index_trends (
    reference_date         DATE,
    ref_period             STRING,
    geography              STRING,
    ref_year               INT,
    ref_month              INT,
    ref_quarter            INT,
    index_total            DOUBLE,
    index_house            DOUBLE,
    index_land             DOUBLE,
    index_total_12m_ago    DOUBLE,
    yoy_change_pct         DOUBLE
)
USING DELTA
COMMENT 'Ontario housing price index with YoY trends';

CREATE TABLE IF NOT EXISTS gold.property_overview (
    feature_type           STRING,
    property_count         BIGINT,
    avg_area_sqm           DOUBLE,
    avg_area_sqft          DOUBLE,
    median_area_sqm        DOUBLE,
    min_area_sqm           DOUBLE,
    max_area_sqm           DOUBLE,
    total_area_sqm         DOUBLE
)
USING DELTA
COMMENT 'Property parcel overview by feature type';
