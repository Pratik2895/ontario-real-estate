-- Bronze Layer DDL — Ontario Real Estate
-- Run in Databricks SQL Editor

CREATE SCHEMA IF NOT EXISTS bronze;

-- 528K+ Toronto property parcels
CREATE TABLE IF NOT EXISTS bronze.property_boundaries (
    PARCELID             STRING,
    FEATURE_TYPE         STRING,
    DATE_EFFECTIVE       STRING,
    DATE_EXPIRY          STRING,
    STATEDAREA           STRING,
    ADDRESS_NUMBER       STRING,
    LINEAR_NAME_FULL     STRING,
    OBJECTID             STRING,
    TRANS_ID_CREATE      STRING,
    TRANS_ID_EXPIRE      STRING,
    geometry             STRING,
    _ingestion_timestamp TIMESTAMP,
    _source              STRING,
    _source_file         STRING
)
USING DELTA
COMMENT 'Raw Toronto property boundary parcels — Toronto Open Data';

-- 232K+ active building permits
CREATE TABLE IF NOT EXISTS bronze.permits_active (
    PERMIT_NUM                STRING,
    REVISION_NUM              STRING,
    PERMIT_TYPE               STRING,
    STRUCTURE_TYPE            STRING,
    WORK                      STRING,
    STREET_NUM                STRING,
    STREET_NAME               STRING,
    STREET_TYPE               STRING,
    STREET_DIRECTION          STRING,
    POSTAL                    STRING,
    GEO_ID                    STRING,
    WARD_GRID                 STRING,
    APPLICATION_DATE          STRING,
    ISSUED_DATE               STRING,
    COMPLETED_DATE            STRING,
    STATUS                    STRING,
    DESCRIPTION               STRING,
    CURRENT_USE               STRING,
    PROPOSED_USE              STRING,
    DWELLING_UNITS_CREATED    STRING,
    DWELLING_UNITS_LOST       STRING,
    EST_CONST_COST            STRING,
    ASSEMBLY                  STRING,
    INSTITUTIONAL             STRING,
    RESIDENTIAL               STRING,
    BUSINESS_AND_PERSONAL_SERVICES STRING,
    MERCANTILE                STRING,
    INDUSTRIAL                STRING,
    INTERIOR_ALTERATIONS      STRING,
    DEMOLITION                STRING,
    BUILDER_NAME              STRING,
    _ingestion_timestamp      TIMESTAMP,
    _source                   STRING,
    _source_file              STRING
)
USING DELTA
COMMENT 'Raw active building permits — Toronto Open Data';

-- 390K+ cleared building permits
CREATE TABLE IF NOT EXISTS bronze.permits_cleared LIKE bronze.permits_active
COMMENT 'Raw cleared building permits (since 2017) — Toronto Open Data';

-- 2.6K building assets
CREATE TABLE IF NOT EXISTS bronze.building_assets (
    FLOC_ID              STRING,
    `Building Description` STRING,
    Address              STRING,
    City                 STRING,
    `Ward Name`          STRING,
    `Former Municipality` STRING,
    District             STRING,
    `City Cluster`       STRING,
    `Building Type`      STRING,
    `Building Status`    STRING,
    Jurisdiction         STRING,
    Management           STRING,
    Owner                STRING,
    `Gross Floor Area (M2)` STRING,
    `Gross Floor Area (FT2)` STRING,
    `Year Built`         STRING,
    WARD                 STRING,
    geometry             STRING,
    _ingestion_timestamp TIMESTAMP,
    _source              STRING,
    _source_file         STRING
)
USING DELTA
COMMENT 'Raw city-owned building asset inventory — Toronto Open Data';

-- 10.7K land assets
CREATE TABLE IF NOT EXISTS bronze.land_assets (
    FLOC_ID              STRING,
    `Property Description` STRING,
    Address              STRING,
    City                 STRING,
    `Ward Name`          STRING,
    `Former Municipality` STRING,
    District             STRING,
    `Property Type`      STRING,
    `Property Status`    STRING,
    `Area (M2)`          STRING,
    `Area (FT2)`         STRING,
    `Frontage (M)`       STRING,
    `Depth (M)`          STRING,
    OBJECTID             STRING,
    geometry             STRING,
    _ingestion_timestamp TIMESTAMP,
    _source              STRING,
    _source_file         STRING
)
USING DELTA
COMMENT 'Raw city-owned land asset inventory — Toronto Open Data';

-- 3.6K apartment registrations
CREATE TABLE IF NOT EXISTS bronze.apartment_registration (
    RSN                  STRING,
    SITE_ADDRESS         STRING,
    PROPERTY_TYPE        STRING,
    WARD                 STRING,
    YEAR_BUILT           STRING,
    YEAR_REGISTERED      STRING,
    CONFIRMED_UNITS      STRING,
    CONFIRMED_STOREYS    STRING,
    HEATING_TYPE         STRING,
    AIR_CONDITIONING_TYPE STRING,
    PCODE                STRING,
    -- additional columns omitted for brevity
    _ingestion_timestamp TIMESTAMP,
    _source              STRING,
    _source_file         STRING
)
USING DELTA
COMMENT 'Raw RentSafeTO apartment registration — Toronto Open Data';

-- 5.3K apartment evaluations
CREATE TABLE IF NOT EXISTS bronze.apartment_evaluations (
    RSN                           STRING,
    `YEAR EVALUATED`              STRING,
    `CURRENT BUILDING EVAL SCORE` STRING,
    `PROACTIVE BUILDING SCORE`    STRING,
    `CURRENT REACTIVE SCORE`      STRING,
    `NO OF AREAS EVALUATED`       STRING,
    LATITUDE                      STRING,
    LONGITUDE                     STRING,
    -- additional score columns omitted for brevity
    _ingestion_timestamp          TIMESTAMP,
    _source                       STRING,
    _source_file                  STRING
)
USING DELTA
COMMENT 'Raw RentSafeTO apartment evaluations — Toronto Open Data';

-- 65K StatCan housing price index rows
CREATE TABLE IF NOT EXISTS bronze.housing_price_index (
    REF_DATE                    STRING,
    GEO                         STRING,
    DGUID                       STRING,
    `New housing price indexes` STRING,
    UOM                         STRING,
    VALUE                       STRING,
    STATUS                      STRING,
    _ingestion_timestamp        TIMESTAMP,
    _source                     STRING,
    _source_file                STRING
)
USING DELTA
COMMENT 'Raw New Housing Price Index (Table 18-10-0205-01) — Statistics Canada';
