-- Silver Layer DDL — Ontario Real Estate
-- Run in Databricks SQL Editor

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.properties (
    parcel_id          STRING,
    feature_type       STRING,
    date_effective     DATE,
    stated_area_sqm    DOUBLE,
    address_number     STRING,
    street_name        STRING,
    full_address       STRING,
    object_id          BIGINT,
    stated_area_sqft   DOUBLE,
    _silver_timestamp  TIMESTAMP
)
USING DELTA
COMMENT 'Cleaned Toronto property parcels — deduped by parcel_id';

CREATE TABLE IF NOT EXISTS silver.building_permits (
    permit_number               STRING,
    revision_number             INT,
    permit_type                 STRING,
    structure_type              STRING,
    work_type                   STRING,
    street_number               STRING,
    street_name                 STRING,
    street_type                 STRING,
    street_direction            STRING,
    postal_code                 STRING,
    geo_id                      BIGINT,
    ward                        STRING,
    application_date            DATE,
    issued_date                 DATE,
    completed_date              DATE,
    status                      STRING,
    description                 STRING,
    current_use                 STRING,
    proposed_use                STRING,
    dwelling_units_created      INT,
    dwelling_units_lost         INT,
    estimated_construction_cost DOUBLE,
    full_address                STRING,
    application_year            INT,
    application_quarter         INT,
    net_dwelling_units          INT,
    postal_prefix               STRING,
    _permit_source              STRING,
    _silver_timestamp           TIMESTAMP
)
USING DELTA
COMMENT 'Unified active+cleared building permits — deduped by permit_number';

CREATE TABLE IF NOT EXISTS silver.apartment_buildings (
    rsn                 BIGINT,
    address             STRING,
    property_type       STRING,
    ward                STRING,
    year_built          INT,
    year_registered     INT,
    storeys             INT,
    units               INT,
    heating_type        STRING,
    ac_type             STRING,
    parking_type        STRING,
    elevators           INT,
    pets_allowed        STRING,
    has_laundry         STRING,
    has_bike_parking    STRING,
    postal_code         STRING,
    year_evaluated      INT,
    eval_score          DOUBLE,
    proactive_score     DOUBLE,
    reactive_score      DOUBLE,
    areas_evaluated     INT,
    latitude            DOUBLE,
    longitude           DOUBLE,
    building_age        INT,
    units_per_storey    DOUBLE,
    postal_prefix       STRING,
    _silver_timestamp   TIMESTAMP
)
USING DELTA
COMMENT 'Apartment buildings with registration + latest evaluation scores';

CREATE TABLE IF NOT EXISTS silver.housing_price_index (
    reference_date      DATE,
    ref_period          STRING,
    geography           STRING,
    index_type          STRING,
    index_value         DOUBLE,
    data_quality_flag   STRING,
    ref_year            INT,
    ref_month           INT,
    ref_quarter         INT,
    _silver_timestamp   TIMESTAMP
)
USING DELTA
COMMENT 'Ontario-filtered New Housing Price Index — Statistics Canada';
