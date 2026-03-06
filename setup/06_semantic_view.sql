-- =============================================================================
-- CARNIVAL CASINO SLOT ANALYTICS - SEMANTIC VIEW CREATION
-- =============================================================================
-- Creates a Semantic View for Cortex Analyst text-to-SQL capabilities
-- Run this AFTER 04_feature_engineering.sql
-- =============================================================================

USE SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;
USE WAREHOUSE GEN2_SMALL;

-- =============================================================================
-- CREATE SEMANTIC VIEW FROM YAML
-- =============================================================================
-- The semantic view enables natural language queries against the slot analytics data

SELECT SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
    'SLOT_ANALYTICS',  -- Schema name
    $$
name: CASINO_SLOT_SEMANTIC_VIEW
description: Semantic view for Carnival Casino slot analytics including member demographics, play history, and ML features

tables:
  - name: MEMBER_DEMOGRAPHICS
    description: Core member information including loyalty tier and spending profile
    base_table:
      database: CARNIVAL_CASINO
      schema: SLOT_ANALYTICS
      table: MEMBER_DEMOGRAPHICS
    primary_key: MEMBER_ID
    dimensions:
      - name: MEMBER_ID
        description: Unique member identifier
        expr: MEMBER_ID
        data_type: NUMBER
      - name: GENDER
        description: Member gender (M/F)
        expr: GENDER
        data_type: VARCHAR
      - name: MEMBERSHIP_TIER
        description: Loyalty tier (Basic, Bronze, Silver, Gold, Platinum)
        expr: MEMBERSHIP_TIER
        data_type: VARCHAR
      - name: HOME_STATE
        description: Member home state abbreviation
        expr: HOME_STATE
        data_type: VARCHAR
      - name: RISK_APPETITE
        description: Gambling risk profile (Conservative, Moderate, Aggressive, High Roller)
        expr: RISK_APPETITE
        data_type: VARCHAR
      - name: MARITAL_STATUS
        description: Marital status
        expr: MARITAL_STATUS
        data_type: VARCHAR
      - name: INCOME_BRACKET
        description: Income range category
        expr: INCOME_BRACKET
        data_type: VARCHAR
    time_dimensions:
      - name: ENROLLMENT_DATE
        description: Date member enrolled in loyalty program
        expr: ENROLLMENT_DATE
        data_type: DATE
    facts:
      - name: AGE
        description: Member age in years
        expr: AGE
        data_type: NUMBER
      - name: TOTAL_CRUISES
        description: Number of cruises taken
        expr: TOTAL_CRUISES
        data_type: NUMBER
      - name: LIFETIME_SPEND
        description: Total lifetime spending in dollars
        expr: LIFETIME_SPEND
        data_type: FLOAT

  - name: SLOT_PLAY_HISTORY
    description: Individual slot machine session records
    base_table:
      database: CARNIVAL_CASINO
      schema: SLOT_ANALYTICS
      table: SLOT_PLAY_HISTORY
    primary_key: PLAY_ID
    dimensions:
      - name: PLAY_ID
        description: Unique play session identifier
        expr: PLAY_ID
        data_type: NUMBER
      - name: MEMBER_ID
        description: Member who played (foreign key)
        expr: MEMBER_ID
        data_type: NUMBER
      - name: SHIP_NAME
        description: Name of cruise ship
        expr: SHIP_NAME
        data_type: VARCHAR
      - name: GAME_NAME
        description: Name of slot game played
        expr: GAME_NAME
        data_type: VARCHAR
      - name: GAME_TYPE
        description: Category of slot game
        expr: GAME_TYPE
        data_type: VARCHAR
      - name: TIME_OF_DAY
        description: Time period (Morning, Afternoon, Evening, Night)
        expr: TIME_OF_DAY
        data_type: VARCHAR
    time_dimensions:
      - name: PLAY_DATE
        description: Date of play session
        expr: PLAY_DATE
        data_type: DATE
    facts:
      - name: DENOMINATION
        description: Slot denomination in dollars
        expr: DENOMINATION
        data_type: NUMBER
      - name: NUM_SPINS
        description: Number of spins in session
        expr: NUM_SPINS
        data_type: NUMBER
      - name: BET_PER_SPIN
        description: Bet amount per spin
        expr: BET_PER_SPIN
        data_type: NUMBER
      - name: TOTAL_WAGERED
        description: Total amount wagered in session
        expr: TOTAL_WAGERED
        data_type: FLOAT
      - name: TOTAL_WON
        description: Total amount won in session
        expr: TOTAL_WON
        data_type: FLOAT
      - name: SESSION_DURATION_MINS
        description: Session duration in minutes
        expr: SESSION_DURATION_MINS
        data_type: NUMBER
    metrics:
      - name: TOTAL_SESSIONS
        description: Count of play sessions
        expr: COUNT(PLAY_ID)
        data_type: NUMBER
      - name: TOTAL_WAGERED_SUM
        description: Sum of all amounts wagered
        expr: SUM(TOTAL_WAGERED)
        data_type: FLOAT
      - name: TOTAL_WON_SUM
        description: Sum of all amounts won
        expr: SUM(TOTAL_WON)
        data_type: FLOAT
      - name: AVG_BET
        description: Average bet per spin
        expr: AVG(BET_PER_SPIN)
        data_type: FLOAT
      - name: UNIQUE_PLAYERS
        description: Count of unique players
        expr: COUNT(DISTINCT MEMBER_ID)
        data_type: NUMBER

  - name: MEMBER_SLOT_FEATURES
    description: Aggregated play statistics and ML features per member
    base_table:
      database: CARNIVAL_CASINO
      schema: SLOT_ANALYTICS
      table: MEMBER_SLOT_FEATURES
    primary_key: MEMBER_ID
    dimensions:
      - name: MEMBER_ID
        description: Unique member identifier
        expr: MEMBER_ID
        data_type: NUMBER
      - name: GENDER
        description: Member gender
        expr: GENDER
        data_type: VARCHAR
      - name: MEMBERSHIP_TIER
        description: Loyalty tier
        expr: MEMBERSHIP_TIER
        data_type: VARCHAR
      - name: HOME_STATE
        description: Home state
        expr: HOME_STATE
        data_type: VARCHAR
      - name: RISK_APPETITE
        description: Risk profile
        expr: RISK_APPETITE
        data_type: VARCHAR
      - name: PREFERRED_GAME_TYPE
        description: Most played game type
        expr: PREFERRED_GAME_TYPE
        data_type: VARCHAR
      - name: PREFERRED_GAME
        description: Most played game
        expr: PREFERRED_GAME
        data_type: VARCHAR
      - name: PREFERRED_TIME_OF_DAY
        description: Preferred playing time
        expr: PREFERRED_TIME_OF_DAY
        data_type: VARCHAR
      - name: MOST_PLAYED_SHIP
        description: Ship with most play sessions
        expr: MOST_PLAYED_SHIP
        data_type: VARCHAR
    facts:
      - name: AGE
        description: Member age
        expr: AGE
        data_type: NUMBER
      - name: TOTAL_SESSIONS
        description: Total play sessions
        expr: TOTAL_SESSIONS
        data_type: NUMBER
      - name: TOTAL_SPINS
        description: Lifetime total spins
        expr: TOTAL_SPINS
        data_type: NUMBER
      - name: AVG_BET_PER_SPIN
        description: Average bet per spin
        expr: AVG_BET_PER_SPIN
        data_type: NUMBER
      - name: TOTAL_WAGERED
        description: Total amount wagered
        expr: TOTAL_WAGERED
        data_type: FLOAT
      - name: TOTAL_WON
        description: Total amount won
        expr: TOTAL_WON
        data_type: FLOAT
      - name: WIN_RATE_PCT
        description: Win rate percentage
        expr: WIN_RATE_PCT
        data_type: FLOAT
      - name: PREFERRED_DENOMINATION
        description: Most played denomination
        expr: PREFERRED_DENOMINATION
        data_type: NUMBER

relationships:
  - name: play_to_member
    left_table: SLOT_PLAY_HISTORY
    right_table: MEMBER_DEMOGRAPHICS
    relationship_type: many_to_one
    join_columns:
      - left_column: MEMBER_ID
        right_column: MEMBER_ID
  - name: features_to_member
    left_table: MEMBER_SLOT_FEATURES
    right_table: MEMBER_DEMOGRAPHICS
    relationship_type: one_to_one
    join_columns:
      - left_column: MEMBER_ID
        right_column: MEMBER_ID

verified_queries:
  - name: total_revenue_by_ship
    question: What is the total revenue by ship?
    verified_sql: |
      SELECT SHIP_NAME, SUM(TOTAL_WAGERED) AS TOTAL_REVENUE
      FROM CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_PLAY_HISTORY
      GROUP BY SHIP_NAME ORDER BY TOTAL_REVENUE DESC
  - name: member_count_by_tier
    question: How many members are in each tier?
    verified_sql: |
      SELECT MEMBERSHIP_TIER, COUNT(*) AS MEMBER_COUNT
      FROM CARNIVAL_CASINO.SLOT_ANALYTICS.MEMBER_DEMOGRAPHICS
      GROUP BY MEMBERSHIP_TIER ORDER BY MEMBER_COUNT DESC
  - name: top_games_by_sessions
    question: What are the top 10 most popular games?
    verified_sql: |
      SELECT GAME_NAME, COUNT(*) AS SESSION_COUNT
      FROM CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_PLAY_HISTORY
      GROUP BY GAME_NAME ORDER BY SESSION_COUNT DESC LIMIT 10
  - name: avg_bet_by_denomination
    question: What is the average bet by denomination?
    verified_sql: |
      SELECT DENOMINATION, ROUND(AVG(BET_PER_SPIN), 2) AS AVG_BET
      FROM CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_PLAY_HISTORY
      GROUP BY DENOMINATION ORDER BY DENOMINATION
$$
);

-- Verify the semantic view was created
SHOW SEMANTIC VIEWS IN SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;

-- Grant access for Cortex Analyst usage
GRANT SELECT ON SEMANTIC VIEW CARNIVAL_CASINO.SLOT_ANALYTICS.CASINO_SLOT_SEMANTIC_VIEW TO ROLE PUBLIC;
