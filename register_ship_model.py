import os
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from snowpark_session import create_snowpark_session
from snowflake.ml.registry import Registry

session = create_snowpark_session()
session.use_database("CARNIVAL_CASINO")
session.use_schema("SLOT_ANALYTICS")

print("Connected. Loading ship daily metrics...")

train_df = session.sql("""
    SELECT SHIP_ENCODED, DAY_OF_WEEK, MONTH_NUM, SESSIONS, UNIQUE_PLAYERS,
           AVG_SPINS, AVG_BET_PER_SPIN, DAILY_WAGERED, AVG_SESSION_MINS,
           NIGHT_SESSIONS, EVENING_SESSIONS, PROGRESSIVE_SESSIONS,
           WIN_RATE_PCT, SPLIT
    FROM SHIP_WIN_RATE_TRAINING
""").to_pandas()

feature_cols = [
    "SHIP_ENCODED", "DAY_OF_WEEK", "MONTH_NUM", "SESSIONS", "UNIQUE_PLAYERS",
    "AVG_SPINS", "AVG_BET_PER_SPIN", "DAILY_WAGERED", "AVG_SESSION_MINS",
    "NIGHT_SESSIONS", "EVENING_SESSIONS", "PROGRESSIVE_SESSIONS"
]

train = train_df[train_df["SPLIT"] == "TRAIN"]
test = train_df[train_df["SPLIT"] == "TEST"]

X_train = train[feature_cols].astype(float)
X_test = test[feature_cols].astype(float)
y_train = train["WIN_RATE_PCT"].astype(float)
y_test = test["WIN_RATE_PCT"].astype(float)

print(f"\nTraining set: {len(train)} rows")
print(f"Test set: {len(test)} rows")

print("\n--- Training Ship Win Rate Predictor (GradientBoosting Regressor) ---")

model = GradientBoostingRegressor(
    n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42
)
model.fit(X_train, y_train)

preds = model.predict(X_test)
mae = mean_absolute_error(y_test, preds)
rmse = np.sqrt(mean_squared_error(y_test, preds))
r2 = r2_score(y_test, preds)

print(f"Mean Absolute Error: {mae:.4f}%")
print(f"RMSE: {rmse:.4f}%")
print(f"R² Score: {r2:.4f}")

print("\n--- Feature Importance ---")
importance = pd.DataFrame({
    'Feature': feature_cols,
    'Importance': model.feature_importances_
}).sort_values('Importance', ascending=False)
print(importance.to_string(index=False))

print("\n--- Registering model in Snowflake Model Registry ---")

reg = Registry(session=session, database_name="CARNIVAL_CASINO", schema_name="SLOT_ANALYTICS")
sample_input = X_train.head(10)

mv = reg.log_model(
    model,
    model_name="SHIP_WIN_RATE_PREDICTOR",
    version_name="V1",
    sample_input_data=sample_input,
    conda_dependencies=["scikit-learn"],
    target_platforms=["WAREHOUSE"],
    metrics={"mae": mae, "rmse": rmse, "r2": r2},
    comment="Gradient Boosting regressor predicting daily ship win rate based on ship, day, traffic, and session characteristics"
)
print(f"Registered: {mv.model_name} version {mv.version_name}")

print("\n--- Verifying registration ---")
models = reg.show_models()
print(models[['name', 'comment']])

print("\nDone! Ship Win Rate Predictor registered in CARNIVAL_CASINO.SLOT_ANALYTICS")
