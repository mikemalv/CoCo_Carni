-- =============================================================================
-- CARNIVAL CASINO SLOT ANALYTICS - DATABASE SETUP
-- =============================================================================
-- This script creates the database, schema, and required stages/warehouses
-- Run this FIRST before any other setup scripts
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Create database and schema
CREATE DATABASE IF NOT EXISTS CARNIVAL_CASINO;
CREATE SCHEMA IF NOT EXISTS CARNIVAL_CASINO.SLOT_ANALYTICS;

-- Use the schema for all subsequent operations
USE SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;

-- Create warehouse if not exists
CREATE WAREHOUSE IF NOT EXISTS GEN2_SMALL
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE WAREHOUSE GEN2_SMALL;

-- Create stage for Streamlit app files
CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Create stage for ML model artifacts (optional, for external model storage)
CREATE STAGE IF NOT EXISTS ML_ARTIFACTS_STAGE
    DIRECTORY = (ENABLE = TRUE);

SHOW SCHEMAS IN DATABASE CARNIVAL_CASINO;
