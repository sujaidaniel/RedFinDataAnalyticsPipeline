-- Drop the database if it already exists and create a fresh one
DROP DATABASE IF EXISTS real_estate_db;
CREATE DATABASE real_estate_db;

-- Create schema for organizing database objects
CREATE SCHEMA data_schema;

-- Clear the existing table if it exists
TRUNCATE TABLE IF EXISTS real_estate_db.data_schema.property_data_table;

-- Define the table structure
CREATE OR REPLACE TABLE real_estate_db.data_schema.property_data_table (
    duration INT,
    location_city STRING,
    location_state STRING,
    property_category STRING,
    avg_sale_price FLOAT,
    avg_price_per_sqft FLOAT,
    total_homes_sold FLOAT,
    total_inventory FLOAT,
    supply_months FLOAT,
    avg_days_on_market FLOAT,
    percent_sold_above_list FLOAT,
    end_year INT,
    end_month STRING
);

-- Query to view initial data (empty table)
SELECT * 
FROM real_estate_db.data_schema.property_data_table 
LIMIT 50;

-- Check the total record count
SELECT COUNT(*) 
FROM real_estate_db.data_schema.property_data_table;

-- Create a schema for file format definitions
CREATE SCHEMA file_format_schema;

-- Define a Parquet file format with Snappy compression
CREATE OR REPLACE FILE FORMAT real_estate_db.file_format_schema.parquet_format
TYPE = 'PARQUET'
COMPRESSION = 'SNAPPY';

-- Create a schema for external stages
CREATE SCHEMA staging_schema;

-- Define an external stage for loading data from S3
CREATE OR REPLACE STAGE real_estate_db.staging_schema.property_stage
URL = "s3://real-estate-data/property-zone/property_data.parquet/"
CREDENTIALS = (
    AWS_KEY_ID = 'your_aws_key_id'
    AWS_SECRET_KEY = 'your_aws_secret_key'
)
FILE_FORMAT = real_estate_db.file_format_schema.parquet_format;

-- List files in the external stage to verify access
LIST @real_estate_db.staging_schema.property_stage;

-- Create a schema for Snowpipe configuration
CREATE OR REPLACE SCHEMA pipeline_schema;

-- Define a Snowpipe for auto-ingestion from the external stage into the table
CREATE OR REPLACE PIPE real_estate_db.pipeline_schema.auto_ingest_pipe
AUTO_INGEST = TRUE
AS
COPY INTO real_estate_db.data_schema.property_data_table
FROM @real_estate_db.staging_schema.property_stage
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Describe the Snowpipe configuration
DESC PIPE real_estate_db.pipeline_schema.auto_ingest_pipe;
