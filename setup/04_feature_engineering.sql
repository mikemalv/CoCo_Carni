-- =============================================================================
-- CARNIVAL CASINO SLOT ANALYTICS - FEATURE ENGINEERING
-- =============================================================================
-- This script creates aggregated features for ML modeling
-- Run this AFTER 03_generate_synthetic_data.sql
-- =============================================================================

USE SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;
USE WAREHOUSE GEN2_SMALL;

-- =============================================================================
-- MEMBER_SLOT_FEATURES: Aggregated play statistics per member
-- =============================================================================
-- This table combines demographic data with behavioral play statistics
-- to create features suitable for machine learning models

CREATE OR REPLACE TABLE MEMBER_SLOT_FEATURES AS
WITH play_stats AS (
    SELECT 
        MEMBER_ID,
        COUNT(*) AS TOTAL_SESSIONS,
        SUM(NUM_SPINS) AS TOTAL_SPINS,
        ROUND(AVG(BET_PER_SPIN), 2) AS AVG_BET_PER_SPIN,
        ROUND(SUM(TOTAL_WAGERED), 2) AS TOTAL_WAGERED,
        ROUND(SUM(TOTAL_WON), 2) AS TOTAL_WON,
        ROUND(SUM(NET_RESULT), 2) AS NET_RESULT,
        ROUND(AVG(SESSION_DURATION_MINS), 0) AS AVG_SESSION_DURATION,
        ROUND(AVG(NUM_SPINS), 0) AS AVG_SPINS_PER_SESSION,
        MODE(DENOMINATION) AS PREFERRED_DENOMINATION,
        MODE(GAME_TYPE) AS PREFERRED_GAME_TYPE,
        MODE(GAME_NAME) AS PREFERRED_GAME,
        MODE(TIME_OF_DAY) AS PREFERRED_TIME_OF_DAY,
        MODE(SHIP_NAME) AS MOST_PLAYED_SHIP,
        COUNT(DISTINCT SHIP_NAME) AS SHIPS_PLAYED,
        COUNT(DISTINCT GAME_NAME) AS GAMES_PLAYED,
        DATEDIFF(DAY, MIN(PLAY_DATE), MAX(PLAY_DATE)) AS PLAY_SPAN_DAYS,
        COUNT(DISTINCT DENOMINATION) AS DENOMINATIONS_PLAYED,
        ROUND(STDDEV(BET_PER_SPIN), 2) AS BET_STDDEV,
        ROUND(SUM(TOTAL_WON) / NULLIF(SUM(TOTAL_WAGERED), 0) * 100, 2) AS WIN_RATE_PCT
    FROM SLOT_PLAY_HISTORY
    GROUP BY MEMBER_ID
)
SELECT 
    m.MEMBER_ID,
    m.AGE,
    m.GENDER,
    m.MEMBERSHIP_TIER,
    m.HOME_STATE,
    m.TOTAL_CRUISES,
    m.LIFETIME_SPEND,
    m.RISK_APPETITE,
    m.MARITAL_STATUS,
    m.INCOME_BRACKET,
    DATEDIFF(DAY, m.ENROLLMENT_DATE, CURRENT_DATE()) AS MEMBERSHIP_DAYS,
    ps.TOTAL_SESSIONS,
    ps.TOTAL_SPINS,
    ps.AVG_BET_PER_SPIN,
    ps.TOTAL_WAGERED,
    ps.TOTAL_WON,
    ps.NET_RESULT,
    ps.AVG_SESSION_DURATION,
    ps.AVG_SPINS_PER_SESSION,
    ps.PREFERRED_DENOMINATION,
    ps.PREFERRED_GAME_TYPE,
    ps.PREFERRED_GAME,
    ps.PREFERRED_TIME_OF_DAY,
    ps.MOST_PLAYED_SHIP,
    ps.SHIPS_PLAYED,
    ps.GAMES_PLAYED,
    ps.PLAY_SPAN_DAYS,
    ps.DENOMINATIONS_PLAYED,
    ps.BET_STDDEV,
    ps.WIN_RATE_PCT
FROM MEMBER_DEMOGRAPHICS m
JOIN play_stats ps ON m.MEMBER_ID = ps.MEMBER_ID;

-- =============================================================================
-- ML_TRAINING_DATA: Encoded features ready for ML algorithms
-- =============================================================================
-- This table encodes categorical variables as numeric values and adds
-- a train/validation/test split column (70/15/15 split)

CREATE OR REPLACE TABLE ML_TRAINING_DATA AS
SELECT 
    MEMBER_ID,
    AGE,
    -- Encode categorical features
    CASE GENDER WHEN 'M' THEN 1 ELSE 0 END AS GENDER_ENCODED,
    CASE MEMBERSHIP_TIER 
        WHEN 'Basic' THEN 0 WHEN 'Bronze' THEN 1 WHEN 'Silver' THEN 2 
        WHEN 'Gold' THEN 3 WHEN 'Platinum' THEN 4 
    END AS TIER_ENCODED,
    CASE RISK_APPETITE 
        WHEN 'Low' THEN 0 WHEN 'Medium' THEN 1 
        WHEN 'High' THEN 2 WHEN 'VIP' THEN 3 
    END AS RISK_ENCODED,
    CASE MARITAL_STATUS 
        WHEN 'Single' THEN 0 WHEN 'Married' THEN 1 
        WHEN 'Divorced' THEN 2 WHEN 'Widowed' THEN 3 
    END AS MARITAL_ENCODED,
    CASE INCOME_BRACKET
        WHEN 'Under $50K' THEN 0 WHEN '$50K-$75K' THEN 1 
        WHEN '$75K-$100K' THEN 2 WHEN '$100K-$150K' THEN 3 
        WHEN 'Over $150K' THEN 4
    END AS INCOME_ENCODED,
    -- Numeric features
    MEMBERSHIP_DAYS,
    TOTAL_CRUISES,
    LIFETIME_SPEND,
    TOTAL_SESSIONS,
    TOTAL_SPINS,
    AVG_SESSION_DURATION,
    AVG_SPINS_PER_SESSION,
    SHIPS_PLAYED,
    GAMES_PLAYED,
    DENOMINATIONS_PLAYED,
    BET_STDDEV,
    WIN_RATE_PCT,
    TOTAL_WAGERED,
    TOTAL_WON,
    PLAY_SPAN_DAYS,
    -- Target variables
    PREFERRED_DENOMINATION,
    AVG_BET_PER_SPIN,
    -- Train/Validation/Test split (70/15/15)
    CASE 
        WHEN RANDOM() < 0.7 THEN 'TRAIN'
        WHEN RANDOM() < 0.5 THEN 'VALIDATION'
        ELSE 'TEST'
    END AS SPLIT
FROM MEMBER_SLOT_FEATURES;

-- =============================================================================
-- VERIFY FEATURE ENGINEERING
-- =============================================================================
SELECT 
    'MEMBER_SLOT_FEATURES' AS TABLE_NAME, 
    COUNT(*) AS ROW_COUNT,
    COUNT(DISTINCT MEMBER_ID) AS UNIQUE_MEMBERS
FROM MEMBER_SLOT_FEATURES
UNION ALL
SELECT 
    'ML_TRAINING_DATA', 
    COUNT(*),
    COUNT(DISTINCT MEMBER_ID)
FROM ML_TRAINING_DATA;

-- Check split distribution
SELECT SPLIT, COUNT(*) AS COUNT, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE
FROM ML_TRAINING_DATA
GROUP BY SPLIT
ORDER BY SPLIT;
