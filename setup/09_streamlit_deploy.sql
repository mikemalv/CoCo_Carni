-- =============================================================================
-- CARNIVAL CASINO SLOT ANALYTICS - STREAMLIT DEPLOYMENT
-- =============================================================================
-- Deploys the Streamlit analytics dashboard to Snowflake
-- Run this AFTER all other setup scripts
-- =============================================================================

USE SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;
USE WAREHOUSE GEN2_SMALL;

-- =============================================================================
-- UPLOAD STREAMLIT APP TO STAGE
-- =============================================================================
-- First, upload the streamlit_app.py file to the stage
-- Run this from your local machine or use Snowsight file upload:

-- PUT 'file:///path/to/CoCo_Carnival/streamlit_app.py' @STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Verify the file was uploaded
LIST @STREAMLIT_STAGE;

-- =============================================================================
-- CREATE STREAMLIT APP
-- =============================================================================
CREATE OR REPLACE STREAMLIT CASINO_ANALYTICS_APP
    FROM '@STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'GEN2_SMALL';

-- Verify the Streamlit app was created
SHOW STREAMLITS IN SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;

-- =============================================================================
-- ACCESS THE APP
-- =============================================================================
-- The app is now available in Snowsight:
-- Go to Projects > Streamlit > CASINO_ANALYTICS_APP
-- Or construct the URL:
-- https://app.snowflake.com/<ACCOUNT>/#/streamlit-apps/CARNIVAL_CASINO.SLOT_ANALYTICS.CASINO_ANALYTICS_APP
