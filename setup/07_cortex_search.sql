-- =============================================================================
-- CARNIVAL CASINO SLOT ANALYTICS - CORTEX SEARCH SERVICE
-- =============================================================================
-- Creates a Cortex Search service for RAG on casino policies
-- Run this AFTER 03_generate_synthetic_data.sql
-- =============================================================================

USE SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;
USE WAREHOUSE GEN2_SMALL;

-- =============================================================================
-- CREATE CORTEX SEARCH SERVICE
-- =============================================================================
-- This service enables semantic search over casino policy documents
-- Used by the Cortex Agent for answering policy-related questions

CREATE OR REPLACE CORTEX SEARCH SERVICE CASINO_POLICIES_SEARCH
    ON CONTENT
    ATTRIBUTES CATEGORY, TITLE
    WAREHOUSE = GEN2_SMALL
    TARGET_LAG = '1 hour'
AS (
    SELECT 
        CONTENT,
        CATEGORY,
        TITLE,
        LAST_UPDATED::VARCHAR AS LAST_UPDATED
    FROM CARNIVAL_CASINO.SLOT_ANALYTICS.CASINO_POLICIES
);

-- Verify the search service was created
SHOW CORTEX SEARCH SERVICES IN SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;

-- Grant access for agent usage
GRANT USAGE ON CORTEX SEARCH SERVICE CARNIVAL_CASINO.SLOT_ANALYTICS.CASINO_POLICIES_SEARCH TO ROLE PUBLIC;

-- =============================================================================
-- TEST THE SEARCH SERVICE
-- =============================================================================
-- Example query to test the search service:
/*
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'CARNIVAL_CASINO.SLOT_ANALYTICS.CASINO_POLICIES_SEARCH',
    '{
        "query": "What is the minimum age to gamble?",
        "columns": ["CONTENT", "CATEGORY", "TITLE"],
        "limit": 3
    }'
);
*/
