"""
Carnival Casino Slot Analytics - Consolidated Model Training & Registration

This script trains and registers all Python-based ML models to the Snowflake
Model Registry. Run after the SQL setup scripts (01-05) have been executed.

Models:
  1. SLOT_DENOMINATION_MODEL (V1) - RandomForest: predicts denomination from patron context
  2. DENOMINATION_CLASSIFIER_REG (V1) - GradientBoosting: predicts denomination from member features
  3. BET_AMOUNT_CLASSIFIER_REG (V1) - GradientBoosting: predicts bet category from member features
  4. BANK_DENOMINATION_MODEL (V1) - MultiOutput GBR: predicts denomination demand % per ship

Usage:
  SNOWFLAKE_CONNECTION_NAME=demo python models/train_all_models.py
  SNOWFLAKE_CONNECTION_NAME=demo python models/train_all_models.py --model denomination
  SNOWFLAKE_CONNECTION_NAME=demo python models/train_all_models.py --model bank
  SNOWFLAKE_CONNECTION_NAME=demo python models/train_all_models.py --model all
"""

import os
import sys
import argparse
import pickle
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.multioutput import MultiOutputRegressor
from sklearn.model_selection import train_test_split, LeaveOneOut, cross_val_predict
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, mean_absolute_error, r2_score
from snowflake.snowpark import Session
from snowflake.ml.registry import Registry

DB = "CARNIVAL_CASINO"
SCHEMA = "SLOT_ANALYTICS"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)


def get_session():
    connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME") or "demo"
    session = Session.builder.config("connection_name", connection_name).create()
    session.use_database(DB)
    session.use_schema(SCHEMA)
    return session


def get_registry(session):
    return Registry(session=session, database_name=DB, schema_name=SCHEMA)


# ---------------------------------------------------------------------------
# Model 1: SLOT_DENOMINATION_MODEL (RandomForest)
# ---------------------------------------------------------------------------
def train_slot_denomination_model(session, reg):
    print("\n" + "=" * 70)
    print("MODEL 1: SLOT_DENOMINATION_MODEL (RandomForest)")
    print("=" * 70)

    print("Loading data from Snowflake...")
    query = f"""
    SELECT 
        p.SHIP_NAME, p.PLAY_DATE, p.DENOMINATION, p.GAME_TYPE,
        m.AGE, m.GENDER, m.MEMBERSHIP_TIER, m.TOTAL_CRUISES,
        m.RISK_APPETITE, m.INCOME_BRACKET
    FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY p
    JOIN {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS m ON p.MEMBER_ID = m.MEMBER_ID
    """
    df = session.sql(query).to_pandas()
    print(f"Loaded {len(df)} rows")

    df['PLAY_MONTH'] = pd.to_datetime(df['PLAY_DATE']).dt.month
    df['PLAY_DAY_OF_WEEK'] = pd.to_datetime(df['PLAY_DATE']).dt.dayofweek
    df['IS_WEEKEND'] = df['PLAY_DAY_OF_WEEK'].isin([5, 6]).astype(int)

    features = [
        'SHIP_NAME', 'PLAY_MONTH', 'PLAY_DAY_OF_WEEK', 'IS_WEEKEND',
        'GAME_TYPE', 'AGE', 'GENDER', 'MEMBERSHIP_TIER',
        'TOTAL_CRUISES', 'RISK_APPETITE', 'INCOME_BRACKET'
    ]

    label_encoders = {}
    df_encoded = df.copy()
    categorical_cols = ['SHIP_NAME', 'GAME_TYPE', 'GENDER', 'MEMBERSHIP_TIER',
                        'RISK_APPETITE', 'INCOME_BRACKET']

    for col in categorical_cols:
        le = LabelEncoder()
        df_encoded[col] = le.fit_transform(df_encoded[col].astype(str))
        label_encoders[col] = le

    denom_le = LabelEncoder()
    df_encoded['DENOMINATION_CLASS'] = denom_le.fit_transform(df_encoded['DENOMINATION'].astype(str))
    label_encoders['DENOMINATION'] = denom_le

    X = df_encoded[features]
    y = df_encoded['DENOMINATION_CLASS']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    print("Training RandomForest...")
    model = RandomForestClassifier(n_estimators=100, max_depth=15, min_samples_split=10, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    accuracy = accuracy_score(y_test, model.predict(X_test))
    print(f"Model accuracy: {accuracy:.2%}")

    pkl_path = os.path.join(PROJECT_DIR, 'denomination_classifier.pkl')
    with open(pkl_path, 'wb') as f:
        pickle.dump({
            'model': model,
            'label_encoders': label_encoders,
            'features': features,
            'categorical_cols': categorical_cols
        }, f)
    print(f"Pickle saved to {pkl_path}")

    print("Registering to Snowflake Model Registry...")
    sample_input = X_train.head(10)
    try:
        mv = reg.log_model(
            model,
            model_name="SLOT_DENOMINATION_MODEL",
            version_name="V1",
            sample_input_data=sample_input,
            conda_dependencies=["scikit-learn"],
            target_platforms=["SNOWPARK_CONTAINER_SERVICES"],
            comment="RandomForest classifier to predict optimal slot denomination based on patron demographics"
        )
        print(f"Registered: {mv.model_name} version {mv.version_name}")
    except Exception as e:
        print(f"Registry error: {e}")


# ---------------------------------------------------------------------------
# Models 2 & 3: DENOMINATION_CLASSIFIER_REG + BET_AMOUNT_CLASSIFIER_REG
# ---------------------------------------------------------------------------
def train_registry_classifiers(session, reg):
    print("\n" + "=" * 70)
    print("MODELS 2 & 3: DENOMINATION_CLASSIFIER_REG + BET_AMOUNT_CLASSIFIER_REG")
    print("=" * 70)

    print("Loading ML training data...")
    df = session.sql(f"SELECT * FROM {DB}.{SCHEMA}.ML_TRAINING_DATA").to_pandas()
    print(f"Loaded {len(df)} rows")

    feature_cols = [
        'AGE', 'GENDER_ENCODED', 'TIER_ENCODED', 'RISK_ENCODED',
        'MARITAL_ENCODED', 'INCOME_ENCODED', 'MEMBERSHIP_DAYS',
        'TOTAL_CRUISES', 'LIFETIME_SPEND', 'TOTAL_SESSIONS',
        'TOTAL_SPINS', 'AVG_SESSION_DURATION', 'AVG_SPINS_PER_SESSION',
        'SHIPS_PLAYED', 'GAMES_PLAYED', 'DENOMINATIONS_PLAYED',
        'BET_STDDEV', 'WIN_RATE_PCT', 'PLAY_SPAN_DAYS'
    ]

    train = df[df['SPLIT'] == 'TRAIN']
    test = df[df['SPLIT'] == 'TEST']

    X_train = train[feature_cols].fillna(0)
    X_test = test[feature_cols].fillna(0)

    # --- Denomination Classifier ---
    print("\nTraining Denomination Classifier (GradientBoosting)...")
    y_train_denom = train['PREFERRED_DENOMINATION'].astype(str)
    y_test_denom = test['PREFERRED_DENOMINATION'].astype(str)

    denom_model = GradientBoostingClassifier(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42)
    denom_model.fit(X_train, y_train_denom)
    denom_acc = accuracy_score(y_test_denom, denom_model.predict(X_test))
    print(f"  Accuracy: {denom_acc:.2%}")

    sample_input = X_train.head(10)
    try:
        mv = reg.log_model(
            denom_model,
            model_name="DENOMINATION_CLASSIFIER_REG",
            version_name="V1",
            sample_input_data=sample_input,
            conda_dependencies=["scikit-learn"],
            target_platforms=["WAREHOUSE", "SNOWPARK_CONTAINER_SERVICES"],
            metrics={"accuracy": float(denom_acc)},
            comment="GradientBoosting denomination classifier for Cortex Agent custom tool"
        )
        print(f"  Registered: {mv.model_name} V1")
    except Exception as e:
        print(f"  Registry error (trying SPCS only): {e}")
        mv = reg.log_model(
            denom_model,
            model_name="DENOMINATION_CLASSIFIER_REG",
            version_name="V1",
            sample_input_data=sample_input,
            pip_requirements=["scikit-learn"],
            target_platforms=["SNOWPARK_CONTAINER_SERVICES"],
            comment="GradientBoosting denomination classifier for Cortex Agent custom tool"
        )
        print(f"  Registered (SPCS only): {mv.model_name} V1")

    # --- Bet Amount Classifier ---
    print("\nTraining Bet Amount Classifier (GradientBoosting)...")
    def bet_category(avg_bet):
        if avg_bet <= 2: return 'Low'
        if avg_bet <= 4: return 'Medium'
        if avg_bet <= 7: return 'High'
        return 'VeryHigh'

    y_train_bet = train['AVG_BET_PER_SPIN'].apply(bet_category)
    y_test_bet = test['AVG_BET_PER_SPIN'].apply(bet_category)

    bet_model = GradientBoostingClassifier(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42)
    bet_model.fit(X_train, y_train_bet)
    bet_acc = accuracy_score(y_test_bet, bet_model.predict(X_test))
    print(f"  Accuracy: {bet_acc:.2%}")

    try:
        mv = reg.log_model(
            bet_model,
            model_name="BET_AMOUNT_CLASSIFIER_REG",
            version_name="V1",
            sample_input_data=sample_input,
            conda_dependencies=["scikit-learn"],
            target_platforms=["WAREHOUSE", "SNOWPARK_CONTAINER_SERVICES"],
            metrics={"accuracy": float(bet_acc)},
            comment="GradientBoosting bet category classifier for Cortex Agent custom tool"
        )
        print(f"  Registered: {mv.model_name} V1")
    except Exception as e:
        print(f"  Registry error (trying SPCS only): {e}")
        mv = reg.log_model(
            bet_model,
            model_name="BET_AMOUNT_CLASSIFIER_REG",
            version_name="V1",
            sample_input_data=sample_input,
            pip_requirements=["scikit-learn"],
            target_platforms=["SNOWPARK_CONTAINER_SERVICES"],
            comment="GradientBoosting bet category classifier for Cortex Agent custom tool"
        )
        print(f"  Registered (SPCS only): {mv.model_name} V1")


# ---------------------------------------------------------------------------
# Model 4: BANK_DENOMINATION_MODEL (MultiOutput GBR)
# ---------------------------------------------------------------------------
def train_bank_model(session, reg):
    print("\n" + "=" * 70)
    print("MODEL 4: BANK_DENOMINATION_MODEL (MultiOutput GBR)")
    print("=" * 70)

    print("Loading ship-level demand data...")
    df = session.sql(f"""
        SELECT 
            SHIP_NAME, DENOMINATION, DEMAND_PCT,
            AVG_PASSENGER_AGE, PCT_HIGH_TIER, PCT_HIGH_INCOME,
            PCT_HIGH_RISK, AVG_CRUISES, AVG_LIFETIME_SPEND, UNIQUE_PLAYERS
        FROM {DB}.{SCHEMA}.SHIP_BANK_DEMAND
        ORDER BY SHIP_NAME, DENOMINATION
    """).to_pandas()

    print(f"Loaded {len(df)} rows across {df['SHIP_NAME'].nunique()} ships")

    denominations = sorted(df['DENOMINATION'].unique())
    denom_labels = {d: f'PCT_{str(d).replace(".", "_")}' for d in denominations}

    pivot = df.pivot_table(index='SHIP_NAME', columns='DENOMINATION', values='DEMAND_PCT', aggfunc='first').reset_index()
    pivot.columns = ['SHIP_NAME'] + [denom_labels[d] for d in denominations]

    features_df = df.groupby('SHIP_NAME').first().reset_index()[[
        'SHIP_NAME', 'AVG_PASSENGER_AGE', 'PCT_HIGH_TIER', 'PCT_HIGH_INCOME',
        'PCT_HIGH_RISK', 'AVG_CRUISES', 'AVG_LIFETIME_SPEND', 'UNIQUE_PLAYERS'
    ]]

    ship_le = LabelEncoder()
    features_df['SHIP_ENCODED'] = ship_le.fit_transform(features_df['SHIP_NAME'])

    train_df = features_df.merge(pivot, on='SHIP_NAME')

    feature_cols = [
        'SHIP_ENCODED', 'AVG_PASSENGER_AGE', 'PCT_HIGH_TIER', 'PCT_HIGH_INCOME',
        'PCT_HIGH_RISK', 'AVG_CRUISES', 'AVG_LIFETIME_SPEND', 'UNIQUE_PLAYERS'
    ]
    target_cols = [denom_labels[d] for d in denominations]

    X = train_df[feature_cols].values
    y = train_df[target_cols].values

    print(f"Features shape: {X.shape}, Targets shape: {y.shape}")

    print("Training multi-output GBR with LOO cross-validation...")
    base_model = GradientBoostingRegressor(
        n_estimators=200, max_depth=4, learning_rate=0.1,
        min_samples_split=2, random_state=42
    )
    model = MultiOutputRegressor(base_model)

    loo = LeaveOneOut()
    y_pred_loo = cross_val_predict(model, X, y, cv=loo)
    y_pred_loo = np.clip(y_pred_loo, 0, 100)
    row_sums = y_pred_loo.sum(axis=1, keepdims=True)
    y_pred_loo = y_pred_loo / row_sums * 100

    mae = mean_absolute_error(y, y_pred_loo)
    r2 = r2_score(y, y_pred_loo)
    print(f"LOO CV — MAE: {mae:.2f}%, R²: {r2:.4f}")

    print("Training final model on all data...")
    model.fit(X, y)

    pkl_path = os.path.join(PROJECT_DIR, 'bank_allocation_model.pkl')
    with open(pkl_path, 'wb') as f:
        pickle.dump({
            'model': model,
            'ship_label_encoder': ship_le,
            'feature_cols': feature_cols,
            'target_cols': target_cols,
            'denominations': denominations,
            'denom_labels': denom_labels,
            'ship_profiles': train_df.to_dict('records'),
        }, f)
    print(f"Pickle saved to {pkl_path}")

    print("Registering to Snowflake Model Registry...")
    sample_input = pd.DataFrame(X[:3], columns=feature_cols)
    try:
        mv = reg.log_model(
            model,
            model_name="BANK_DENOMINATION_MODEL",
            version_name="V1",
            sample_input_data=sample_input,
            conda_dependencies=["scikit-learn"],
            target_platforms=["SNOWPARK_CONTAINER_SERVICES"],
            comment="Multi-output regressor: predicts denomination demand % per ship for bank allocation"
        )
        print(f"Registered: {mv.model_name} V1")
    except Exception as e:
        print(f"Registry error: {e}")
        print("Model saved locally — use SQL lookups from SHIP_BANK_DEMAND for Streamlit inference")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Train and register Carnival Casino ML models")
    parser.add_argument("--model", choices=["denomination", "registry", "bank", "all"], default="all",
                        help="Which model(s) to train: denomination, registry, bank, or all (default: all)")
    args = parser.parse_args()

    session = get_session()
    reg = get_registry(session)

    if args.model in ("denomination", "all"):
        train_slot_denomination_model(session, reg)

    if args.model in ("registry", "all"):
        train_registry_classifiers(session, reg)

    if args.model in ("bank", "all"):
        train_bank_model(session, reg)

    print("\nDone!")
    session.close()


if __name__ == "__main__":
    main()
