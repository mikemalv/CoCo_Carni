-- =============================================================================
-- CARNIVAL CASINO SLOT ANALYTICS - DATA TABLE CREATION
-- =============================================================================
-- This script creates all base data tables with proper structures
-- Run this AFTER 01_create_database.sql
-- =============================================================================

USE SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;
USE WAREHOUSE GEN2_SMALL;

-- =============================================================================
-- TABLE 1: MEMBER_DEMOGRAPHICS
-- Core member information including loyalty tier and spending profile
-- =============================================================================
CREATE OR REPLACE TABLE MEMBER_DEMOGRAPHICS (
    MEMBER_ID NUMBER PRIMARY KEY,
    AGE NUMBER,
    GENDER VARCHAR(1),
    MEMBERSHIP_TIER VARCHAR(20),
    HOME_STATE VARCHAR(2),
    ENROLLMENT_DATE DATE,
    TOTAL_CRUISES NUMBER,
    LIFETIME_SPEND FLOAT,
    RISK_APPETITE VARCHAR(20),
    MARITAL_STATUS VARCHAR(20),
    INCOME_BRACKET VARCHAR(20)
);

-- =============================================================================
-- TABLE 2: SLOT_PLAY_HISTORY
-- Individual slot machine session records
-- =============================================================================
CREATE OR REPLACE TABLE SLOT_PLAY_HISTORY (
    PLAY_ID NUMBER PRIMARY KEY,
    MEMBER_ID NUMBER REFERENCES MEMBER_DEMOGRAPHICS(MEMBER_ID),
    PLAY_DATE DATE,
    SHIP_NAME VARCHAR(50),
    DENOMINATION NUMBER(10,2),
    GAME_NAME VARCHAR(50),
    GAME_TYPE VARCHAR(50),
    NUM_SPINS NUMBER(10,0),
    BET_PER_SPIN NUMBER(10,2),
    TOTAL_WAGERED NUMBER(18,2),
    TOTAL_WON NUMBER(18,2),
    NET_RESULT NUMBER(18,2),
    SESSION_DURATION_MINS NUMBER,
    TIME_OF_DAY VARCHAR(20)
);

-- =============================================================================
-- TABLE 3: CASINO_POLICIES
-- Policy documents for Cortex Search RAG functionality
-- =============================================================================
CREATE OR REPLACE TABLE CASINO_POLICIES (
    POLICY_ID NUMBER PRIMARY KEY,
    CATEGORY VARCHAR(50),
    TITLE VARCHAR(200),
    CONTENT TEXT,
    LAST_UPDATED DATE
);

-- =============================================================================
-- TABLE 4: SHIP_BANK_DEMAND
-- Ship-level denomination demand with passenger demographics (for Bank Allocator)
-- Populated in 03_generate_synthetic_data.sql after denomination redistribution
-- =============================================================================
CREATE OR REPLACE TABLE SHIP_BANK_DEMAND (
    SHIP_NAME VARCHAR(50),
    DENOMINATION NUMBER(5,2),
    SESSION_COUNT NUMBER(18,0),
    DEMAND_PCT NUMBER(28,2),
    TOTAL_WAGERED NUMBER(38,2),
    WAGERED_PCT NUMBER(38,2),
    AVG_SPINS NUMBER(29,0),
    AVG_BET NUMBER(29,2),
    UNIQUE_PLAYERS NUMBER(18,0),
    AVG_PASSENGER_AGE NUMBER(21,1),
    PCT_MALE NUMBER(21,3),
    PCT_HIGH_TIER NUMBER(21,3),
    PCT_HIGH_INCOME NUMBER(21,3),
    PCT_HIGH_RISK NUMBER(21,3),
    AVG_CRUISES NUMBER(21,1),
    AVG_LIFETIME_SPEND FLOAT
);

-- =============================================================================
-- TABLE 5: MODEL_EVALUATION_SUMMARY
-- Stores ML model performance metrics
-- =============================================================================
CREATE OR REPLACE TABLE MODEL_EVALUATION_SUMMARY (
    MODEL_NAME VARCHAR(100),
    MODEL_TYPE VARCHAR(50),
    TARGET VARCHAR(100),
    TEST_SIZE NUMBER,
    MODEL_ACCURACY_PCT NUMBER(5,2),
    BASELINE_ACCURACY_PCT NUMBER(5,2),
    BASELINE_METHOD VARCHAR(100),
    NOTES TEXT
);

-- Show created tables
SHOW TABLES IN SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;
