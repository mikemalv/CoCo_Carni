-- =============================================================================
-- CARNIVAL CASINO SLOT ANALYTICS - CORTEX AGENT
-- =============================================================================
-- Creates a Cortex Agent for Snowflake Intelligence with 5 tools:
-- 1. Cortex Analyst (text-to-SQL via semantic view)
-- 2. Cortex Search (RAG for policy documents)
-- 3. Predict Denomination (ML custom tool via stored procedure)
-- 4. Predict Bet Category (ML custom tool via stored procedure)
-- 5. Predict Ship Win Rate (ML custom tool via stored procedure)
--
-- Run this AFTER 05_ml_model_training.sql (for stored procedures),
-- 06_semantic_view.sql, and 07_cortex_search.sql
-- =============================================================================

USE WAREHOUSE GEN2_SMALL;

-- =============================================================================
-- CREATE THE CORTEX AGENT
-- =============================================================================

CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.CARNIVAL_CASINO_AGENT
    COMMENT = 'Carnival Casino Slot Analytics Agent with 5 tools including ship win rate predictions'
    PROFILE = '{"display_name": "Carnival Casino Analyst"}'
    FROM SPECIFICATION $$
    {
        "models": {
            "orchestration": "claude-sonnet-4-6"
        },
        "instructions": {
            "system": "You are the Carnival Casino Analytics Assistant. You help analyze slot machine data, member demographics, casino policies, and provide ML-based predictions.\n\nWhen users ask about:\n- Data queries (revenue, members, games, ships): Use slot_data tool\n- Policies or rules: Use policy_search tool\n- Predicting a member's preferred denomination: Use predict_denomination tool\n- Predicting a member's bet category: Use predict_bet_category tool\n- Predicting expected win rate for a ship: Use predict_ship_win_rate tool\n\nAlways be helpful and provide clear explanations of the data."
        },
        "tools": [
            {
                "tool_spec": {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "slot_data",
                    "description": "Query slot machine data, member demographics, play history, and aggregated features. Use for questions about revenue, player counts, game popularity, member tiers, wagering patterns, and time-based trends."
                }
            },
            {
                "tool_spec": {
                    "type": "cortex_search",
                    "name": "policy_search",
                    "description": "Search casino policies and rules. Use for questions about age requirements, responsible gaming, slot rules, rewards program, casino hours, payment methods, conduct, and tournaments."
                }
            },
            {
                "tool_spec": {
                    "type": "generic",
                    "name": "predict_denomination",
                    "description": "Predict the preferred slot denomination for a member based on their profile and behavior. Takes a member ID and returns the predicted denomination ($0.01, $0.05, $0.25, $1.00, or $5.00).",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "MEMBER_ID_INPUT": {
                                "type": "number",
                                "description": "The member ID to predict denomination for"
                            }
                        },
                        "required": ["MEMBER_ID_INPUT"]
                    }
                }
            },
            {
                "tool_spec": {
                    "type": "generic",
                    "name": "predict_bet_category",
                    "description": "Predict the bet category for a member based on their profile and behavior. Takes a member ID and returns the predicted bet category (Low, Medium, High, or VeryHigh).",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "MEMBER_ID_INPUT": {
                                "type": "number",
                                "description": "The member ID to predict bet category for"
                            }
                        },
                        "required": ["MEMBER_ID_INPUT"]
                    }
                }
            },
            {
                "tool_spec": {
                    "type": "generic",
                    "name": "predict_ship_win_rate",
                    "description": "Predict the expected win rate category for a ship based on day of week, expected sessions, and expected players. Returns Very_Low, Low, Medium, High, or Very_High win rate prediction.",
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "SHIP_NAME_INPUT": {
                                "type": "string",
                                "description": "Ship name (e.g., Carnival Breeze, Carnival Dream, Carnival Magic, Carnival Vista, Carnival Horizon, Carnival Panorama, Carnival Celebration, Carnival Jubilee)"
                            },
                            "DAY_OF_WEEK_INPUT": {
                                "type": "number",
                                "description": "Day of week (0=Sunday, 1=Monday, ..., 6=Saturday)"
                            },
                            "EXPECTED_SESSIONS": {
                                "type": "number",
                                "description": "Expected number of slot sessions for the day"
                            },
                            "EXPECTED_PLAYERS": {
                                "type": "number",
                                "description": "Expected number of unique players for the day"
                            }
                        },
                        "required": ["SHIP_NAME_INPUT", "DAY_OF_WEEK_INPUT", "EXPECTED_SESSIONS", "EXPECTED_PLAYERS"]
                    }
                }
            }
        ],
        "tool_resources": {
            "slot_data": {
                "semantic_view": "CARNIVAL_CASINO.SLOT_ANALYTICS.CASINO_SLOT_SEMANTIC_VIEW",
                "execution_environment": {
                    "type": "warehouse",
                    "warehouse": "GEN2_SMALL"
                }
            },
            "policy_search": {
                "search_service": "CARNIVAL_CASINO.SLOT_ANALYTICS.CASINO_POLICIES_SEARCH",
                "max_results": 5
            },
            "predict_denomination": {
                "type": "procedure",
                "identifier": "CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_DENOMINATION",
                "execution_environment": {
                    "type": "warehouse",
                    "warehouse": "GEN2_SMALL"
                }
            },
            "predict_bet_category": {
                "type": "procedure",
                "identifier": "CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_BET_CATEGORY",
                "execution_environment": {
                    "type": "warehouse",
                    "warehouse": "GEN2_SMALL"
                }
            },
            "predict_ship_win_rate": {
                "type": "procedure",
                "identifier": "CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_SHIP_WIN_RATE",
                "execution_environment": {
                    "type": "warehouse",
                    "warehouse": "GEN2_SMALL"
                }
            }
        }
    }
    $$;

-- Grant access to the agent
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.CARNIVAL_CASINO_AGENT TO ROLE PUBLIC;

-- =============================================================================
-- VERIFY AGENT CREATION
-- =============================================================================
SHOW AGENTS IN SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS LIKE 'CARNIVAL_CASINO_AGENT';

-- =============================================================================
-- EXAMPLE QUERIES FOR THE AGENT
-- =============================================================================
/*
Test the agent in Snowsight > AI & ML > Snowflake Intelligence with these questions:

DATA QUERIES (uses cortex_analyst tool):
- "What is the total revenue by ship?"
- "How many members are in each tier?"
- "What are the top 10 most popular slot games?"
- "Show me the average bet by denomination"
- "What is the monthly wagering trend?"
- "Which ships have the most unique players?"

POLICY QUERIES (uses cortex_search tool):
- "What is the minimum age to gamble?"
- "How does the rewards program work?"
- "What are the rules for progressive jackpots?"
- "Can I reserve a slot machine?"
- "What forms of payment are accepted?"

ML PREDICTION QUERIES (uses generic/procedure tools):
- "Predict the preferred denomination for member 1001"
- "What bet category would member 2500 fall into?"
- "Predict denomination preference for member ID 3000"
- "What is the predicted bet category for member 1500?"
- "What win rate should we expect for Carnival Breeze on Saturday with 150 sessions?"
- "Predict win rate for Carnival Dream on day 3 with 200 sessions and 100 players"
*/
