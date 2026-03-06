import os
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, classification_report
from snowpark_session import create_snowpark_session
from snowflake.ml.registry import Registry

session = create_snowpark_session()
session.use_database("CARNIVAL_CASINO")
session.use_schema("SLOT_ANALYTICS")

print("Connected. Loading training data...")

train_df = session.sql("""
    SELECT AGE, GENDER_ENCODED, TIER_ENCODED, RISK_ENCODED, MARITAL_ENCODED, INCOME_ENCODED,
           MEMBERSHIP_DAYS, TOTAL_CRUISES, LIFETIME_SPEND,
           TOTAL_SESSIONS, TOTAL_SPINS, AVG_SESSION_DURATION, AVG_SPINS_PER_SESSION,
           SHIPS_PLAYED, GAMES_PLAYED, DENOMINATIONS_PLAYED, BET_STDDEV, WIN_RATE_PCT,
           PLAY_SPAN_DAYS, PREFERRED_DENOMINATION, AVG_BET_PER_SPIN, SPLIT
    FROM ML_TRAINING_DATA
""").to_pandas()

feature_cols = [
    "AGE", "GENDER_ENCODED", "TIER_ENCODED", "RISK_ENCODED", "MARITAL_ENCODED", "INCOME_ENCODED",
    "MEMBERSHIP_DAYS", "TOTAL_CRUISES", "LIFETIME_SPEND",
    "TOTAL_SESSIONS", "TOTAL_SPINS", "AVG_SESSION_DURATION", "AVG_SPINS_PER_SESSION",
    "SHIPS_PLAYED", "GAMES_PLAYED", "DENOMINATIONS_PLAYED", "BET_STDDEV", "WIN_RATE_PCT",
    "PLAY_SPAN_DAYS"
]

train = train_df[train_df["SPLIT"] == "TRAIN"]
test = train_df[train_df["SPLIT"] == "TEST"]

X_train = train[feature_cols].astype(float)
X_test = test[feature_cols].astype(float)

# ============================================================
# Model 1: Denomination Classifier
# ============================================================
print("\n--- Training Denomination Classifier (GradientBoosting) ---")
y_train_denom = train["PREFERRED_DENOMINATION"].astype(str)
y_test_denom = test["PREFERRED_DENOMINATION"].astype(str)

denom_clf = GradientBoostingClassifier(
    n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42
)
denom_clf.fit(X_train, y_train_denom)

denom_preds = denom_clf.predict(X_test)
denom_acc = accuracy_score(y_test_denom, denom_preds)
print(f"Denomination Classifier Accuracy: {denom_acc:.4f}")
print(classification_report(y_test_denom, denom_preds))

# ============================================================
# Model 2: Bet Amount Classifier
# ============================================================
print("\n--- Training Bet Amount Classifier (GradientBoosting) ---")

def bet_category(val):
    if val <= 2:
        return "Low"
    elif val <= 4:
        return "Medium"
    elif val <= 7:
        return "High"
    else:
        return "VeryHigh"

y_train_bet = train["AVG_BET_PER_SPIN"].apply(bet_category)
y_test_bet = test["AVG_BET_PER_SPIN"].apply(bet_category)

bet_clf = GradientBoostingClassifier(
    n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42
)
bet_clf.fit(X_train, y_train_bet)

bet_preds = bet_clf.predict(X_test)
bet_acc = accuracy_score(y_test_bet, bet_preds)
print(f"Bet Amount Classifier Accuracy: {bet_acc:.4f}")
print(classification_report(y_test_bet, bet_preds))

# ============================================================
# Register models in Snowflake Model Registry
# ============================================================
print("\n--- Registering models in Snowflake Model Registry ---")

reg = Registry(session=session, database_name="CARNIVAL_CASINO", schema_name="SLOT_ANALYTICS")
sample_input = X_train.head(10)

print("Registering DENOMINATION_CLASSIFIER_REG...")
mv_denom = reg.log_model(
    denom_clf,
    model_name="DENOMINATION_CLASSIFIER_REG",
    version_name="V1",
    sample_input_data=sample_input,
    conda_dependencies=["scikit-learn"],
    target_platforms=["WAREHOUSE"],
    metrics={"accuracy": denom_acc},
    comment="Gradient Boosting classifier predicting preferred slot denomination from member demographics and behavior"
)
print(f"  Registered: {mv_denom.model_name} version {mv_denom.version_name}")

print("Registering BET_AMOUNT_CLASSIFIER_REG...")
mv_bet = reg.log_model(
    bet_clf,
    model_name="BET_AMOUNT_CLASSIFIER_REG",
    version_name="V1",
    sample_input_data=sample_input,
    conda_dependencies=["scikit-learn"],
    target_platforms=["WAREHOUSE"],
    metrics={"accuracy": bet_acc},
    comment="Gradient Boosting classifier predicting bet category (Low/Medium/High/VeryHigh) from member demographics and behavior"
)
print(f"  Registered: {mv_bet.model_name} version {mv_bet.version_name}")

print("\n--- Verifying registration ---")
models = reg.show_models()
print(models)

print("\nDone! Both models registered in CARNIVAL_CASINO.SLOT_ANALYTICS")
