# Carnival Casino Slot Analytics

An end-to-end casino slot machine analytics system built on Snowflake, featuring synthetic data generation, ship-specific denomination profiles, machine learning models, an interactive Streamlit dashboard with a bank allocation tool, semantic views for natural language queries, and a Cortex Agent for Snowflake Intelligence.

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Model](#data-model)
- [Ship Denomination Profiles](#ship-denomination-profiles)
- [Machine Learning Models](#machine-learning-models)
- [Components](#components)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Project Structure](#project-structure)

---

## Overview

This project demonstrates a complete analytics solution for a cruise ship casino operation, simulating Carnival Corporation's slot machine ecosystem. It includes:

| Component | Description |
|-----------|-------------|
| **Synthetic Data** | 5,000 members + 100,000 slot play sessions with ship-specific denomination profiles |
| **ML Models** | 4 Model Registry models + 3 built-in Snowflake ML classifiers |
| **Bank Allocator** | Denomination mix optimizer for wired machine banks per ship |
| **Streamlit Dashboard** | Interactive 4-tab analytics application with Carnival branding |
| **Semantic View** | Natural language SQL via Cortex Analyst |
| **Cortex Search** | RAG search over 15 casino policies |
| **Cortex Agent** | Full-stack AI assistant for Snowflake Intelligence (5 tools) |

---

## Architecture

```
+-----------------------------------------------------------------------------+
|                           CARNIVAL_CASINO Database                           |
+-----------------------------------------------------------------------------+
|  SLOT_ANALYTICS Schema                                                      |
|  +----------------------------------------------------------------------+   |
|  |  Data Layer                                                          |   |
|  |  +-- MEMBER_DEMOGRAPHICS (5,000 members)                             |   |
|  |  +-- SLOT_PLAY_HISTORY (100,000 sessions, ship-weighted denominations)|  |
|  |  +-- SHIP_BANK_DEMAND (72 rows: 8 ships x 9 denominations)          |   |
|  |  +-- CASINO_POLICIES (15 documents)                                  |   |
|  +----------------------------------------------------------------------+   |
|                              |                                              |
|                              v                                              |
|  +----------------------------------------------------------------------+   |
|  |  Feature Engineering Layer                                           |   |
|  |  +-- MEMBER_SLOT_FEATURES (aggregated behavioral features)           |   |
|  |  +-- ML_TRAINING_DATA (encoded + split: 70/15/15)                    |   |
|  |  +-- SHIP_DAILY_METRICS (daily ship-level aggregates)                |   |
|  +----------------------------------------------------------------------+   |
|                              |                                              |
|                              v                                              |
|  +----------------------------------------------------------------------+   |
|  |  ML Models                                                           |   |
|  |  +-- Built-in ML Classification (SNOWFLAKE.ML.CLASSIFICATION)        |   |
|  |  |   +-- DENOMINATION_CLASSIFIER -> DENOMINATION_PREDICTIONS         |   |
|  |  |   +-- BET_AMOUNT_CLASSIFIER -> BET_PREDICTIONS                    |   |
|  |  |   +-- SHIP_WIN_RATE_CLASSIFIER -> win rate predictions            |   |
|  |  +-- Model Registry (sklearn)                                        |   |
|  |      +-- SLOT_DENOMINATION_MODEL (V1) - RandomForest                 |   |
|  |      +-- DENOMINATION_CLASSIFIER_REG (V1) - GradientBoosting         |   |
|  |      +-- BET_AMOUNT_CLASSIFIER_REG (V1) - GradientBoosting           |   |
|  |      +-- BANK_DENOMINATION_MODEL (V1) - MultiOutput GBR              |   |
|  +----------------------------------------------------------------------+   |
|                              |                                              |
|                              v                                              |
|  +----------------------------------------------------------------------+   |
|  |  AI/Analytics Layer                                                  |   |
|  |  +-- CASINO_SLOT_SEMANTIC_VIEW (Cortex Analyst)                      |   |
|  |  +-- CASINO_POLICIES_SEARCH (Cortex Search)                          |   |
|  |  +-- CASINO_ANALYTICS_APP (Streamlit in Snowflake)                   |   |
|  +----------------------------------------------------------------------+   |
+-----------------------------------------------------------------------------+
                              |
                              v
+-----------------------------------------------------------------------------+
|  SNOWFLAKE_INTELLIGENCE.AGENTS                                              |
|  +-- CARNIVAL_CASINO_AGENT (5 tools: Analyst + Search + 3 ML)              |
+-----------------------------------------------------------------------------+
```

---

## Data Model

### Table Descriptions

| Table | Rows | Description |
|-------|------|-------------|
| `MEMBER_DEMOGRAPHICS` | 5,000 | Core member profiles with loyalty tier, demographics, risk appetite |
| `SLOT_PLAY_HISTORY` | 100,000 | Individual slot sessions across 8 ships, 15 games, 9 denominations (ship-weighted) |
| `SLOT_PLAY_HISTORY_OLD` | 100,000 | Backup of original uniform denomination distribution |
| `SHIP_BANK_DEMAND` | 72 | Denomination demand % per ship with passenger demographics (8 ships x 9 denoms) |
| `MEMBER_SLOT_FEATURES` | 5,000 | Aggregated behavioral features per member for ML |
| `ML_TRAINING_DATA` | 5,000 | Encoded features with 70/15/15 train/val/test split |
| `SHIP_DAILY_METRICS` | ~5,840 | Daily ship-level metrics for win rate classification |
| `CASINO_POLICIES` | 15 | Policy documents for Cortex Search RAG |
| `MODEL_EVALUATION_SUMMARY` | 2 | ML model performance metrics |

### Denominations (9 classes)

| Denomination | Label |
|-------------|-------|
| $0.01 | Penny |
| $0.05 | Nickel |
| $0.25 | Quarter |
| $1.00 | Dollar |
| $5.00 | Five Dollar |
| $10.00 | Ten Dollar |
| $20.00 | Twenty Dollar |
| $50.00 | Fifty Dollar |
| $100.00 | Hundred Dollar |

---

## Ship Denomination Profiles

Each ship has a distinct passenger demographic that drives different denomination demand.
Denominations are redistributed using a CDF-based sampling approach to create realistic
ship-specific profiles:

| Ship | Primary Denominations | Profile |
|------|----------------------|---------|
| **Carnival Breeze** | Penny (22%), Quarter (20%), Nickel (19%) | Budget / Family |
| **Mardi Gras** | $50 (21%), $20 (18%), $100 (17%) | Flagship / Premium |
| **Carnival Jubilee** | $20 (18%), $10 (17%), $50 (16%) | High Rollers |
| **Carnival Horizon** | $1 (18%), $5 (16%), Quarter (15%) | Balanced Mid-Range |
| **Carnival Magic** | Penny (20%), Nickel (17%), Quarter (15%) | Budget / Traditional |
| **Carnival Vista** | Penny (15%), $1 (14%), Nickel (14%) | Balanced Low-Mid |
| **Carnival Panorama** | $1 (15%), $5 (15%), $10 (14%) | Mid-High Range |
| **Carnival Celebration** | $1 (15%), $5 (14%), Quarter (13%) | Balanced |

This approach models the real-world casino operations concept where slot machines are grouped
in **banks** and priced at the ship level based on passenger demographics, not individually.

---

## Machine Learning Models

### Built-in Snowflake ML Classification

| Model | Target | Accuracy | Baseline |
|-------|--------|----------|----------|
| DENOMINATION_CLASSIFIER | PREFERRED_DENOMINATION (9 classes) | ~10% | ~11% (most-frequent) |
| BET_AMOUNT_CLASSIFIER | BET_CATEGORY (Low/Medium/High/VeryHigh) | ~76% | ~59% |
| SHIP_WIN_RATE_CLASSIFIER | WIN_RATE_CATEGORY (Very_Low to Very_High) | N/A | N/A |

### Snowflake Model Registry (sklearn)

| Model | Registry Name | Type | Description |
|-------|---------------|------|-------------|
| Denomination Predictor | `SLOT_DENOMINATION_MODEL` | RandomForestClassifier | Predicts denomination from ship/patron context |
| Denomination Classifier | `DENOMINATION_CLASSIFIER_REG` | GradientBoostingClassifier | Predicts denomination from member features |
| Bet Amount Classifier | `BET_AMOUNT_CLASSIFIER_REG` | GradientBoostingClassifier | Predicts bet category from member features |
| Bank Demand Model | `BANK_DENOMINATION_MODEL` | MultiOutputRegressor (GBR) | Predicts denomination demand % per ship |

### Stored Procedures

| Procedure | Description |
|-----------|-------------|
| `PREDICT_DENOMINATION(MEMBER_ID)` | GradientBoosting denomination prediction (Cortex Agent tool) |
| `PREDICT_DENOMINATION_V2(MEMBER_ID)` | Python-based RandomForest prediction with richer context |
| `PREDICT_BET_CATEGORY(MEMBER_ID)` | GradientBoosting bet category prediction (Cortex Agent tool) |
| `PREDICT_SHIP_WIN_RATE(SHIP, DAY, SESSIONS, PLAYERS)` | Daily win rate category prediction |

### Training All Models

All Python-based models are consolidated in a single script:

```bash
SNOWFLAKE_CONNECTION_NAME=demo python models/train_all_models.py

# Or train individual models:
SNOWFLAKE_CONNECTION_NAME=demo python models/train_all_models.py --model denomination
SNOWFLAKE_CONNECTION_NAME=demo python models/train_all_models.py --model registry
SNOWFLAKE_CONNECTION_NAME=demo python models/train_all_models.py --model bank
```

---

## Components

### 1. Streamlit Dashboard

A 4-tab interactive analytics application with Carnival brand styling:

| Tab | Features |
|-----|----------|
| **Member Overview** | KPIs, tier distribution (donut), age by gender (grouped bars), income/state charts |
| **Slot Analytics** | Ship filter, revenue by denomination, top games, time patterns, monthly trend |
| **ML Models** | Model performance cards, confusion matrix heatmap, feature importance, live predictions, interactive denomination predictor, **Bank Denomination Allocator** |
| **Policies & Info** | Expandable policy documents by category |

#### Bank Denomination Allocator

The ML Models tab includes a bank allocation tool that:
1. Selects a ship and bank size (number of machines)
2. Queries `SHIP_BANK_DEMAND` for the ship's denomination demand profile
3. Allocates machines using `floor()` + remainder-based rounding
4. Displays a bar chart and allocation summary ensuring exact machine count

### 2. Semantic View

Enables natural language queries via Cortex Analyst:

```
Tables: MEMBER_DEMOGRAPHICS, SLOT_PLAY_HISTORY, MEMBER_SLOT_FEATURES
Relationships: play_to_member (many:1), features_to_member (1:1)
Verified Queries: revenue by ship, members by tier, top games, avg bet by denomination
```

### 3. Cortex Search Service

RAG-enabled search over casino policies:
- **Search Column**: CONTENT (policy text)
- **Attributes**: CATEGORY, TITLE
- **Target Lag**: 1 hour

### 4. Cortex Agent

Full-stack agent for Snowflake Intelligence with **five tools**:

| Tool | Type | Use Case |
|------|------|----------|
| `slot_data` | cortex_analyst_text_to_sql | Data queries via semantic view |
| `policy_search` | cortex_search | Policy questions via RAG |
| `predict_denomination` | generic (procedure) | ML prediction: preferred denomination |
| `predict_bet_category` | generic (procedure) | ML prediction: bet category |
| `predict_ship_win_rate` | generic (procedure) | ML prediction: ship win rate |

---

## Setup Instructions

### Prerequisites

- Snowflake account with ACCOUNTADMIN role
- Warehouse (SMALL or larger recommended)
- Python 3.11+ with `snowflake-snowpark-python`, `snowflake-ml-python`, `scikit-learn`
- Snowflake Intelligence enabled (for Cortex Agent)

### Quick Start

**Step 1: Run SQL setup scripts in order**

```bash
# Using Snowflake CLI (snow sql) or SnowSQL:
snow sql -f setup/01_create_database.sql -c demo
snow sql -f setup/02_create_data_tables.sql -c demo
snow sql -f setup/03_generate_synthetic_data.sql -c demo
snow sql -f setup/04_feature_engineering.sql -c demo
snow sql -f setup/05_ml_model_training.sql -c demo
snow sql -f setup/06_semantic_view.sql -c demo
snow sql -f setup/07_cortex_search.sql -c demo
snow sql -f setup/08_cortex_agent.sql -c demo
snow sql -f setup/09_streamlit_deploy.sql -c demo
```

**Step 2: Train and register Python ML models**

```bash
SNOWFLAKE_CONNECTION_NAME=demo python models/train_all_models.py
```

**Step 3: Upload Streamlit app to stage**

```sql
PUT 'file:///path/to/CoCo_Carnival/streamlit_app.py' @CARNIVAL_CASINO.SLOT_ANALYTICS.STREAMLIT_STAGE 
    AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

---

## Usage

### Streamlit App

**In Snowflake:**
Navigate to Projects > Streamlit > `CASINO_ANALYTICS_APP`

**Locally:**
```bash
SNOWFLAKE_CONNECTION_NAME=demo streamlit run streamlit_app.py
```

### Cortex Agent

Access in Snowsight: AI & ML > Snowflake Intelligence > Select "Carnival Casino Analyst"

**Example Questions:**
- "What is the total revenue by ship?"
- "What is the minimum age to gamble?"
- "Which games are most popular on Carnival Breeze?"
- "Predict the preferred denomination for member 1001"
- "What bet category would member 2500 fall into?"
- "What win rate should we expect for Carnival Breeze on Saturday with 150 sessions?"

### Direct SQL Queries

```sql
-- Ship denomination demand profile
SELECT * FROM CARNIVAL_CASINO.SLOT_ANALYTICS.SHIP_BANK_DEMAND
WHERE SHIP_NAME = 'Mardi Gras' ORDER BY DENOMINATION;

-- ML predictions
CALL CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_DENOMINATION(1001);
CALL CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_BET_CATEGORY(1001);
CALL CARNIVAL_CASINO.SLOT_ANALYTICS.PREDICT_DENOMINATION_V2(1001);

-- Feature importance
CALL CARNIVAL_CASINO.SLOT_ANALYTICS.DENOMINATION_CLASSIFIER!SHOW_FEATURE_IMPORTANCE();
```

---

## Project Structure

```
CoCo_Carnival/
|-- README.md                           # This file
|-- streamlit_app.py                    # Streamlit dashboard (main app)
|-- snowpark_session.py                 # Snowpark session helper
|-- slot_analytics_semantic_model.yaml  # Semantic model YAML definition
|-- denomination_classifier.pkl         # Pickled RandomForest model artifact
|-- bank_allocation_model.pkl           # Pickled MultiOutput GBR artifact
|-- models/
|   +-- train_all_models.py            # Consolidated model training & registration
|-- setup/
|   |-- 01_create_database.sql          # Database, schema, stages, warehouse
|   |-- 02_create_data_tables.sql       # Table DDL (including SHIP_BANK_DEMAND)
|   |-- 03_generate_synthetic_data.sql  # Data generation + denomination redistribution
|   |-- 04_feature_engineering.sql      # Aggregated features + ML training data
|   |-- 05_ml_model_training.sql        # Built-in ML + stored procedures + registry models
|   |-- 06_semantic_view.sql            # Cortex Analyst semantic view
|   |-- 07_cortex_search.sql            # Cortex Search RAG service
|   |-- 08_cortex_agent.sql             # Snowflake Intelligence agent (5 tools)
|   +-- 09_streamlit_deploy.sql         # Streamlit app deployment
+-- assets/
    +-- carnival-logo.svg               # Carnival brand logo
```

---

## Snowflake Objects Summary

| Object Type | Name | Location |
|-------------|------|----------|
| Database | `CARNIVAL_CASINO` | Account |
| Schema | `SLOT_ANALYTICS` | CARNIVAL_CASINO |
| Tables (13) | MEMBER_DEMOGRAPHICS, SLOT_PLAY_HISTORY, SHIP_BANK_DEMAND, CASINO_POLICIES, MEMBER_SLOT_FEATURES, ML_TRAINING_DATA, SHIP_DAILY_METRICS, SHIP_WIN_RATE_TRAINING, DENOMINATION_PREDICTIONS, BET_PREDICTIONS, SHIP_WIN_RATE_PREDICTIONS, MODEL_EVALUATION_SUMMARY, SLOT_PLAY_HISTORY_OLD | SLOT_ANALYTICS |
| ML Models (Built-in) | DENOMINATION_CLASSIFIER, BET_AMOUNT_CLASSIFIER, SHIP_WIN_RATE_CLASSIFIER | SLOT_ANALYTICS |
| ML Models (Registry) | SLOT_DENOMINATION_MODEL, DENOMINATION_CLASSIFIER_REG, BET_AMOUNT_CLASSIFIER_REG, BANK_DENOMINATION_MODEL | SLOT_ANALYTICS |
| Stored Procedures | PREDICT_DENOMINATION, PREDICT_DENOMINATION_V2, PREDICT_BET_CATEGORY, PREDICT_SHIP_WIN_RATE | SLOT_ANALYTICS |
| Semantic View | CASINO_SLOT_SEMANTIC_VIEW | SLOT_ANALYTICS |
| Cortex Search | CASINO_POLICIES_SEARCH | SLOT_ANALYTICS |
| Streamlit | CASINO_ANALYTICS_APP | SLOT_ANALYTICS |
| Agent | CARNIVAL_CASINO_AGENT (5 tools) | SNOWFLAKE_INTELLIGENCE.AGENTS |

---

## License

This project is for demonstration purposes. Data is synthetically generated.

---

*Built with Snowflake Cortex Code*
