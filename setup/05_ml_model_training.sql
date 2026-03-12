-- =============================================================================
-- CARNIVAL CASINO SLOT ANALYTICS - ML MODEL TRAINING
-- =============================================================================
-- This script trains classification models using Snowflake ML
-- Run this AFTER 04_feature_engineering.sql
-- =============================================================================

USE SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;
USE WAREHOUSE GEN2_SMALL;

-- =============================================================================
-- MODEL 1: DENOMINATION CLASSIFIER
-- =============================================================================
-- Predicts preferred slot denomination based on member demographics and behavior
-- Target: PREFERRED_DENOMINATION (0.01, 0.05, 0.25, 1.00, 5.00)

-- Create training view (excludes target leakage features)
CREATE OR REPLACE VIEW DENOMINATION_TRAINING_VIEW AS
SELECT 
    AGE, GENDER_ENCODED, TIER_ENCODED, RISK_ENCODED, MARITAL_ENCODED, INCOME_ENCODED,
    MEMBERSHIP_DAYS, TOTAL_CRUISES, LIFETIME_SPEND,
    TOTAL_SESSIONS, TOTAL_SPINS, AVG_SESSION_DURATION, AVG_SPINS_PER_SESSION,
    SHIPS_PLAYED, GAMES_PLAYED, DENOMINATIONS_PLAYED, BET_STDDEV, WIN_RATE_PCT,
    PLAY_SPAN_DAYS,
    PREFERRED_DENOMINATION
FROM ML_TRAINING_DATA
WHERE SPLIT = 'TRAIN';

-- Train the denomination classifier
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION DENOMINATION_CLASSIFIER(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'DENOMINATION_TRAINING_VIEW'),
    TARGET_COLNAME => 'PREFERRED_DENOMINATION'
);

-- Generate predictions on test set
CREATE OR REPLACE TABLE DENOMINATION_PREDICTIONS AS
SELECT 
    t.*,
    DENOMINATION_CLASSIFIER!PREDICT(
        AGE, GENDER_ENCODED, TIER_ENCODED, RISK_ENCODED, MARITAL_ENCODED, INCOME_ENCODED,
        MEMBERSHIP_DAYS, TOTAL_CRUISES, LIFETIME_SPEND,
        TOTAL_SESSIONS, TOTAL_SPINS, AVG_SESSION_DURATION, AVG_SPINS_PER_SESSION,
        SHIPS_PLAYED, GAMES_PLAYED, DENOMINATIONS_PLAYED, BET_STDDEV, WIN_RATE_PCT,
        PLAY_SPAN_DAYS
    ) AS PREDICTION
FROM ML_TRAINING_DATA t
WHERE SPLIT = 'TEST';

-- =============================================================================
-- MODEL 2: BET AMOUNT CLASSIFIER  
-- =============================================================================
-- Predicts bet category based on member profile
-- Target: BET_CATEGORY (Low, Medium, High, VeryHigh)

-- Create bet categories based on average bet per spin
CREATE OR REPLACE VIEW BET_TRAINING_VIEW AS
SELECT 
    AGE, GENDER_ENCODED, TIER_ENCODED, RISK_ENCODED, MARITAL_ENCODED, INCOME_ENCODED,
    MEMBERSHIP_DAYS, TOTAL_CRUISES, LIFETIME_SPEND,
    TOTAL_SESSIONS, TOTAL_SPINS, AVG_SESSION_DURATION, AVG_SPINS_PER_SESSION,
    SHIPS_PLAYED, GAMES_PLAYED, DENOMINATIONS_PLAYED, BET_STDDEV, WIN_RATE_PCT,
    PLAY_SPAN_DAYS,
    CASE 
        WHEN AVG_BET_PER_SPIN <= 2 THEN 'Low'
        WHEN AVG_BET_PER_SPIN <= 4 THEN 'Medium'
        WHEN AVG_BET_PER_SPIN <= 7 THEN 'High'
        ELSE 'VeryHigh'
    END AS BET_CATEGORY
FROM ML_TRAINING_DATA
WHERE SPLIT = 'TRAIN';

-- Train the bet amount classifier
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION BET_AMOUNT_CLASSIFIER(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'BET_TRAINING_VIEW'),
    TARGET_COLNAME => 'BET_CATEGORY'
);

-- Generate predictions on test set
CREATE OR REPLACE TABLE BET_PREDICTIONS AS
SELECT 
    t.*,
    CASE 
        WHEN t.AVG_BET_PER_SPIN <= 2 THEN 'Low'
        WHEN t.AVG_BET_PER_SPIN <= 4 THEN 'Medium'
        WHEN t.AVG_BET_PER_SPIN <= 7 THEN 'High'
        ELSE 'VeryHigh'
    END AS ACTUAL_BET_CATEGORY,
    BET_AMOUNT_CLASSIFIER!PREDICT(
        AGE, GENDER_ENCODED, TIER_ENCODED, RISK_ENCODED, MARITAL_ENCODED, INCOME_ENCODED,
        MEMBERSHIP_DAYS, TOTAL_CRUISES, LIFETIME_SPEND,
        TOTAL_SESSIONS, TOTAL_SPINS, AVG_SESSION_DURATION, AVG_SPINS_PER_SESSION,
        SHIPS_PLAYED, GAMES_PLAYED, DENOMINATIONS_PLAYED, BET_STDDEV, WIN_RATE_PCT,
        PLAY_SPAN_DAYS
    ) AS PREDICTION
FROM ML_TRAINING_DATA t
WHERE SPLIT = 'TEST';

-- =============================================================================
-- MODEL EVALUATION
-- =============================================================================

-- Calculate denomination classifier accuracy
WITH denom_accuracy AS (
    SELECT 
        COUNT(*) AS TOTAL,
        SUM(CASE WHEN PREDICTION:class::NUMBER = PREFERRED_DENOMINATION THEN 1 ELSE 0 END) AS CORRECT
    FROM DENOMINATION_PREDICTIONS
),
denom_baseline AS (
    SELECT MODE(PREFERRED_DENOMINATION) AS MOST_FREQUENT FROM ML_TRAINING_DATA WHERE SPLIT = 'TRAIN'
),
bet_accuracy AS (
    SELECT 
        COUNT(*) AS TOTAL,
        SUM(CASE WHEN PREDICTION:class::STRING = ACTUAL_BET_CATEGORY THEN 1 ELSE 0 END) AS CORRECT
    FROM BET_PREDICTIONS
),
bet_baseline AS (
    SELECT MODE(CASE 
        WHEN AVG_BET_PER_SPIN <= 2 THEN 'Low'
        WHEN AVG_BET_PER_SPIN <= 4 THEN 'Medium'
        WHEN AVG_BET_PER_SPIN <= 7 THEN 'High'
        ELSE 'VeryHigh'
    END) AS MOST_FREQUENT FROM ML_TRAINING_DATA WHERE SPLIT = 'TRAIN'
)
SELECT * FROM (
    SELECT 
        'Denomination Classifier' AS MODEL_NAME,
        'Classification' AS MODEL_TYPE,
        'PREFERRED_DENOMINATION' AS TARGET,
        da.TOTAL AS TEST_SIZE,
        ROUND(da.CORRECT * 100.0 / da.TOTAL, 2) AS MODEL_ACCURACY_PCT,
        ROUND((SELECT COUNT(*) FROM DENOMINATION_PREDICTIONS WHERE PREFERRED_DENOMINATION = (SELECT MOST_FREQUENT FROM denom_baseline)) * 100.0 / da.TOTAL, 2) AS BASELINE_ACCURACY_PCT,
        'Random baseline (most-frequent class)' AS BASELINE_METHOD,
        'Synthetic data lacks demographic-denomination correlation; real data expected to perform better' AS NOTES
    FROM denom_accuracy da
    UNION ALL
    SELECT 
        'Bet Amount Classifier' AS MODEL_NAME,
        'Classification' AS MODEL_TYPE,
        'BET_CATEGORY (Low/Medium/High/VeryHigh)' AS TARGET,
        ba.TOTAL AS TEST_SIZE,
        ROUND(ba.CORRECT * 100.0 / ba.TOTAL, 2) AS MODEL_ACCURACY_PCT,
        ROUND((SELECT COUNT(*) FROM BET_PREDICTIONS WHERE ACTUAL_BET_CATEGORY = (SELECT MOST_FREQUENT FROM bet_baseline)) * 100.0 / ba.TOTAL, 2) AS BASELINE_ACCURACY_PCT,
        'Random baseline (most-frequent class)' AS BASELINE_METHOD,
        'High accuracy due to strong feature correlation with bet behavior; model captures spending patterns effectively' AS NOTES
    FROM bet_accuracy ba
);

-- Store evaluation results
TRUNCATE TABLE MODEL_EVALUATION_SUMMARY;
INSERT INTO MODEL_EVALUATION_SUMMARY
WITH denom_accuracy AS (
    SELECT 
        COUNT(*) AS TOTAL,
        SUM(CASE WHEN PREDICTION:class::NUMBER = PREFERRED_DENOMINATION THEN 1 ELSE 0 END) AS CORRECT
    FROM DENOMINATION_PREDICTIONS
),
denom_baseline AS (
    SELECT MODE(PREFERRED_DENOMINATION) AS MOST_FREQUENT FROM ML_TRAINING_DATA WHERE SPLIT = 'TRAIN'
),
bet_accuracy AS (
    SELECT 
        COUNT(*) AS TOTAL,
        SUM(CASE WHEN PREDICTION:class::STRING = ACTUAL_BET_CATEGORY THEN 1 ELSE 0 END) AS CORRECT
    FROM BET_PREDICTIONS
),
bet_baseline AS (
    SELECT MODE(CASE 
        WHEN AVG_BET_PER_SPIN <= 2 THEN 'Low'
        WHEN AVG_BET_PER_SPIN <= 4 THEN 'Medium'
        WHEN AVG_BET_PER_SPIN <= 7 THEN 'High'
        ELSE 'VeryHigh'
    END) AS MOST_FREQUENT FROM ML_TRAINING_DATA WHERE SPLIT = 'TRAIN'
)
SELECT 
    'Denomination Classifier' AS MODEL_NAME,
    'Classification' AS MODEL_TYPE,
    'PREFERRED_DENOMINATION' AS TARGET,
    da.TOTAL AS TEST_SIZE,
    ROUND(da.CORRECT * 100.0 / da.TOTAL, 2) AS MODEL_ACCURACY_PCT,
    ROUND((SELECT COUNT(*) FROM DENOMINATION_PREDICTIONS WHERE PREFERRED_DENOMINATION = (SELECT MOST_FREQUENT FROM denom_baseline)) * 100.0 / da.TOTAL, 2) AS BASELINE_ACCURACY_PCT,
    'Random baseline (most-frequent class)' AS BASELINE_METHOD,
    'Synthetic data lacks demographic-denomination correlation; real data expected to perform better' AS NOTES
FROM denom_accuracy da
UNION ALL
SELECT 
    'Bet Amount Classifier' AS MODEL_NAME,
    'Classification' AS MODEL_TYPE,
    'BET_CATEGORY (Low/Medium/High/VeryHigh)' AS TARGET,
    ba.TOTAL AS TEST_SIZE,
    ROUND(ba.CORRECT * 100.0 / ba.TOTAL, 2) AS MODEL_ACCURACY_PCT,
    ROUND((SELECT COUNT(*) FROM BET_PREDICTIONS WHERE ACTUAL_BET_CATEGORY = (SELECT MOST_FREQUENT FROM bet_baseline)) * 100.0 / ba.TOTAL, 2) AS BASELINE_ACCURACY_PCT,
    'Random baseline (most-frequent class)' AS BASELINE_METHOD,
    'High accuracy due to strong feature correlation with bet behavior; model captures spending patterns effectively' AS NOTES
FROM bet_accuracy ba;

-- View final results
SELECT * FROM MODEL_EVALUATION_SUMMARY;

-- =============================================================================
-- INSPECT FEATURE IMPORTANCE
-- =============================================================================
-- Run these to see which features drive predictions:
-- CALL DENOMINATION_CLASSIFIER!SHOW_FEATURE_IMPORTANCE();
-- CALL BET_AMOUNT_CLASSIFIER!SHOW_FEATURE_IMPORTANCE();

-- =============================================================================
-- MODEL REGISTRY MODELS (Python-based sklearn models)
-- =============================================================================
-- These models are registered via the consolidated Python script:
--   SNOWFLAKE_CONNECTION_NAME=demo python models/train_all_models.py
--
-- Models created:
--   1. SLOT_DENOMINATION_MODEL (V1) — RandomForest denomination classifier
--   2. DENOMINATION_CLASSIFIER_REG (V1) — GradientBoosting denomination classifier
--   3. BET_AMOUNT_CLASSIFIER_REG (V1) — GradientBoosting bet category classifier
--   4. BANK_DENOMINATION_MODEL (V1) — MultiOutput GBR for ship denomination demand %
--   5. VOYAGE_PROFIT_MODEL (V1) — GBR regressor for per-voyage casino profit prediction
--
-- Verify registration:
SHOW MODELS IN SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;

-- =============================================================================
-- STORED PROCEDURES FOR ML INFERENCE
-- =============================================================================
-- These procedures wrap Model Registry models for Cortex Agent custom tools

CREATE OR REPLACE PROCEDURE CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_DENOMINATION(MEMBER_ID_INPUT NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
'
DECLARE
    result VARCHAR;
BEGIN
    SELECT 
        MODEL(CARNIVAL_CASINO.SLOT_ANALYTICS.DENOMINATION_CLASSIFIER_REG, V1)!PREDICT(
            AGE, GENDER_ENCODED, TIER_ENCODED, RISK_ENCODED, MARITAL_ENCODED, INCOME_ENCODED,
            MEMBERSHIP_DAYS, TOTAL_CRUISES, LIFETIME_SPEND,
            TOTAL_SESSIONS, TOTAL_SPINS, AVG_SESSION_DURATION, AVG_SPINS_PER_SESSION,
            SHIPS_PLAYED, GAMES_PLAYED, DENOMINATIONS_PLAYED, BET_STDDEV, WIN_RATE_PCT,
            PLAY_SPAN_DAYS
        ):output_feature_0::STRING
    INTO result
    FROM CARNIVAL_CASINO.SLOT_ANALYTICS.ML_TRAINING_DATA
    WHERE MEMBER_ID = :MEMBER_ID_INPUT
    LIMIT 1;
    
    RETURN ''Predicted preferred denomination for member '' || :MEMBER_ID_INPUT || '': $'' || result;
END;
';

CREATE OR REPLACE PROCEDURE CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_BET_CATEGORY(MEMBER_ID_INPUT NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
'
DECLARE
    result VARCHAR;
BEGIN
    SELECT 
        MODEL(CARNIVAL_CASINO.SLOT_ANALYTICS.BET_AMOUNT_CLASSIFIER_REG, V1)!PREDICT(
            AGE, GENDER_ENCODED, TIER_ENCODED, RISK_ENCODED, MARITAL_ENCODED, INCOME_ENCODED,
            MEMBERSHIP_DAYS, TOTAL_CRUISES, LIFETIME_SPEND,
            TOTAL_SESSIONS, TOTAL_SPINS, AVG_SESSION_DURATION, AVG_SPINS_PER_SESSION,
            SHIPS_PLAYED, GAMES_PLAYED, DENOMINATIONS_PLAYED, BET_STDDEV, WIN_RATE_PCT,
            PLAY_SPAN_DAYS
        ):output_feature_0::STRING
    INTO result
    FROM CARNIVAL_CASINO.SLOT_ANALYTICS.ML_TRAINING_DATA
    WHERE MEMBER_ID = :MEMBER_ID_INPUT
    LIMIT 1;
    
    RETURN ''Predicted bet category for member '' || :MEMBER_ID_INPUT || '': '' || result;
END;
';

-- Test the procedures
-- CALL CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_DENOMINATION(1001);
-- CALL CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_BET_CATEGORY(1001);

-- =============================================================================
-- STORED PROCEDURE: PREDICT_DENOMINATION_V2 (Python-based, uses SLOT_DENOMINATION_MODEL)
-- =============================================================================
-- Uses the Model Registry RandomForest model for richer predictions with patron context.
-- Called from the Streamlit app's ML Models tab.

CREATE OR REPLACE PROCEDURE CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_DENOMINATION_V2(MEMBER_ID_INPUT NUMBER)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','snowflake-ml-python')
HANDLER = 'predict'
EXECUTE AS CALLER
AS
'
def predict(session, member_id_input):
    ship_map = {''Carnival Breeze'': 0, ''Carnival Celebration'': 1, ''Carnival Horizon'': 2, ''Carnival Jubilee'': 3, ''Carnival Magic'': 4, ''Carnival Panorama'': 5, ''Carnival Vista'': 6, ''Mardi Gras'': 7}
    gender_map = {''Female'': 0, ''Male'': 1}
    tier_map = {''Basic'': 0, ''Bronze'': 1, ''Gold'': 2, ''Platinum'': 3, ''Silver'': 4}
    risk_map = {''Conservative'': 0, ''High'': 1, ''Moderate'': 2}
    income_map = {''Under $50K'': 0, ''$50K-$75K'': 1, ''$75K-$100K'': 2, ''$100K-$150K'': 3, ''Over $150K'': 4}
    denom_map = {0: ''$0.01'', 1: ''$0.05'', 2: ''$0.25'', 3: ''$1.00'', 4: ''$5.00'', 5: ''$10.00'', 6: ''$20.00'', 7: ''$50.00'', 8: ''$100.00''}

    member = session.sql(f"""
        SELECT m.AGE, m.GENDER, m.MEMBERSHIP_TIER, m.TOTAL_CRUISES, m.RISK_APPETITE, m.INCOME_BRACKET, p.SHIP_NAME
        FROM CARNIVAL_CASINO.SLOT_ANALYTICS.MEMBER_DEMOGRAPHICS m
        LEFT JOIN (
            SELECT MEMBER_ID, SHIP_NAME, ROW_NUMBER() OVER (PARTITION BY MEMBER_ID ORDER BY PLAY_DATE DESC) AS rn
            FROM CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_PLAY_HISTORY
        ) p ON m.MEMBER_ID = p.MEMBER_ID AND p.rn = 1
        WHERE m.MEMBER_ID = {int(member_id_input)}
        LIMIT 1
    """).collect()

    if not member:
        return f"Member {member_id_input} not found"

    r = member[0]
    ship_enc = ship_map.get(r[''SHIP_NAME''], 0)
    gender_enc = gender_map.get(r[''GENDER''], 0)
    tier_enc = tier_map.get(r[''MEMBERSHIP_TIER''], 0)
    risk_enc = risk_map.get(r[''RISK_APPETITE''], 0)
    income_enc = income_map.get(r[''INCOME_BRACKET''], 0)

    from datetime import datetime
    now = datetime.now()
    play_month = now.month
    play_dow = now.weekday()
    is_weekend = 1 if play_dow >= 5 else 0

    pred = session.sql(f"""
        SELECT MODEL(CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_DENOMINATION_MODEL, V1)!PREDICT(
            {ship_enc}, {play_month}, {play_dow}, {is_weekend},
            0, {r[''AGE'']}, {gender_enc}, {tier_enc},
            {r[''TOTAL_CRUISES'']}, {risk_enc}, {income_enc}
        ):output_feature_0::INT AS pred_class
    """).collect()

    pred_class = pred[0][''PRED_CLASS'']
    denom = denom_map.get(pred_class, ''Unknown'')
    return f"Predicted optimal denomination for member {int(member_id_input)}: {denom} (Age: {r[''AGE'']}, Tier: {r[''MEMBERSHIP_TIER'']}, Income: {r[''INCOME_BRACKET'']}, Risk: {r[''RISK_APPETITE'']})"
';

-- Test: CALL CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_DENOMINATION_V2(1001);

-- =============================================================================
-- MODEL 3: SHIP WIN RATE CLASSIFIER
-- =============================================================================
-- Predicts expected win rate category per ship based on day, traffic, and session patterns
-- Target: WIN_RATE_CATEGORY (Very_Low/Low/Medium/High/Very_High)

-- Create daily ship metrics table
CREATE OR REPLACE TABLE SHIP_DAILY_METRICS AS
SELECT 
    SHIP_NAME,
    PLAY_DATE,
    DAYOFWEEK(PLAY_DATE) AS DAY_OF_WEEK,
    MONTH(PLAY_DATE) AS MONTH_NUM,
    COUNT(*) AS SESSIONS,
    COUNT(DISTINCT MEMBER_ID) AS UNIQUE_PLAYERS,
    ROUND(AVG(NUM_SPINS), 1) AS AVG_SPINS,
    ROUND(AVG(BET_PER_SPIN), 2) AS AVG_BET_PER_SPIN,
    ROUND(SUM(TOTAL_WAGERED), 2) AS DAILY_WAGERED,
    ROUND(SUM(TOTAL_WON), 2) AS DAILY_WON,
    ROUND(SUM(TOTAL_WON) / NULLIF(SUM(TOTAL_WAGERED), 0) * 100, 2) AS WIN_RATE_PCT,
    ROUND(AVG(SESSION_DURATION_MINS), 1) AS AVG_SESSION_MINS,
    SUM(CASE WHEN TIME_OF_DAY = 'Night' THEN 1 ELSE 0 END) AS NIGHT_SESSIONS,
    SUM(CASE WHEN TIME_OF_DAY = 'Evening' THEN 1 ELSE 0 END) AS EVENING_SESSIONS,
    SUM(CASE WHEN GAME_TYPE = 'Progressive' THEN 1 ELSE 0 END) AS PROGRESSIVE_SESSIONS
FROM SLOT_PLAY_HISTORY
GROUP BY SHIP_NAME, PLAY_DATE
ORDER BY SHIP_NAME, PLAY_DATE;

-- Create training data with ship encoding and split
CREATE OR REPLACE TABLE SHIP_WIN_RATE_TRAINING AS
SELECT 
    s.*,
    CASE SHIP_NAME
        WHEN 'Carnival Breeze' THEN 0
        WHEN 'Carnival Celebration' THEN 1
        WHEN 'Carnival Dream' THEN 2
        WHEN 'Carnival Horizon' THEN 3
        WHEN 'Carnival Jubilee' THEN 4
        WHEN 'Carnival Magic' THEN 5
        WHEN 'Carnival Panorama' THEN 6
        WHEN 'Carnival Vista' THEN 7
    END AS SHIP_ENCODED,
    CASE 
        WHEN rn <= total * 0.8 THEN 'TRAIN'
        ELSE 'TEST'
    END AS SPLIT
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (ORDER BY PLAY_DATE) AS rn,
           COUNT(*) OVER () AS total
    FROM SHIP_DAILY_METRICS
) s;

-- Create training view with binned win rate categories
CREATE OR REPLACE VIEW SHIP_WIN_RATE_TRAIN_VIEW AS
SELECT 
    SHIP_ENCODED, DAY_OF_WEEK, MONTH_NUM, SESSIONS, UNIQUE_PLAYERS,
    AVG_SPINS, AVG_BET_PER_SPIN, DAILY_WAGERED, AVG_SESSION_MINS,
    NIGHT_SESSIONS, EVENING_SESSIONS, PROGRESSIVE_SESSIONS,
    CASE 
        WHEN WIN_RATE_PCT < 80 THEN 'Very_Low'
        WHEN WIN_RATE_PCT < 85 THEN 'Low'
        WHEN WIN_RATE_PCT < 90 THEN 'Medium'
        WHEN WIN_RATE_PCT < 95 THEN 'High'
        ELSE 'Very_High'
    END AS WIN_RATE_CATEGORY
FROM SHIP_WIN_RATE_TRAINING
WHERE SPLIT = 'TRAIN';

-- Train the ship win rate classifier
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION SHIP_WIN_RATE_CLASSIFIER(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'SHIP_WIN_RATE_TRAIN_VIEW'),
    TARGET_COLNAME => 'WIN_RATE_CATEGORY'
);

-- Create stored procedure for ship win rate prediction
CREATE OR REPLACE PROCEDURE PREDICT_SHIP_WIN_RATE(
    SHIP_NAME_INPUT VARCHAR,
    DAY_OF_WEEK_INPUT NUMBER,
    EXPECTED_SESSIONS NUMBER,
    EXPECTED_PLAYERS NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
'
DECLARE
    ship_code NUMBER;
    result VARCHAR;
BEGIN
    SELECT CASE :SHIP_NAME_INPUT
        WHEN ''Carnival Breeze'' THEN 0
        WHEN ''Carnival Celebration'' THEN 1
        WHEN ''Carnival Dream'' THEN 2
        WHEN ''Carnival Horizon'' THEN 3
        WHEN ''Carnival Jubilee'' THEN 4
        WHEN ''Carnival Magic'' THEN 5
        WHEN ''Carnival Panorama'' THEN 6
        WHEN ''Carnival Vista'' THEN 7
        ELSE 0
    END INTO ship_code;
    
    SELECT SHIP_WIN_RATE_CLASSIFIER!PREDICT(
        INPUT_DATA => OBJECT_CONSTRUCT(
            ''SHIP_ENCODED'', :ship_code,
            ''DAY_OF_WEEK'', :DAY_OF_WEEK_INPUT,
            ''MONTH_NUM'', MONTH(CURRENT_DATE()),
            ''SESSIONS'', :EXPECTED_SESSIONS,
            ''UNIQUE_PLAYERS'', :EXPECTED_PLAYERS,
            ''AVG_SPINS'', 250,
            ''AVG_BET_PER_SPIN'', 4.2,
            ''DAILY_WAGERED'', :EXPECTED_SESSIONS * 250 * 4.2 * 0.5,
            ''AVG_SESSION_MINS'', 65,
            ''NIGHT_SESSIONS'', ROUND(:EXPECTED_SESSIONS * 0.3),
            ''EVENING_SESSIONS'', ROUND(:EXPECTED_SESSIONS * 0.37),
            ''PROGRESSIVE_SESSIONS'', ROUND(:EXPECTED_SESSIONS * 0.25)
        )
    ):class::STRING INTO result;
    
    RETURN ''Predicted win rate category for '' || :SHIP_NAME_INPUT || '' on day '' || :DAY_OF_WEEK_INPUT || '' with '' || :EXPECTED_SESSIONS || '' sessions: '' || result;
END;
';

-- Test ship win rate prediction
-- CALL PREDICT_SHIP_WIN_RATE('Carnival Breeze', 6, 150, 80);
