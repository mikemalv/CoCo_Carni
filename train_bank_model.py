import os
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.multioutput import MultiOutputRegressor
from sklearn.model_selection import LeaveOneOut, cross_val_predict
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder
import pickle
from snowflake.snowpark import Session

connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME") or "demo"
session = Session.builder.config("connection_name", connection_name).create()
session.use_database("CARNIVAL_CASINO")
session.use_schema("SLOT_ANALYTICS")

print("Loading ship-level demand data...")
df = session.sql("""
    SELECT 
        SHIP_NAME, DENOMINATION, DEMAND_PCT,
        AVG_PASSENGER_AGE, PCT_HIGH_TIER, PCT_HIGH_INCOME,
        PCT_HIGH_RISK, AVG_CRUISES, AVG_LIFETIME_SPEND, UNIQUE_PLAYERS
    FROM CARNIVAL_CASINO.SLOT_ANALYTICS.SHIP_BANK_DEMAND
    ORDER BY SHIP_NAME, DENOMINATION
""").to_pandas()

print(f"Loaded {len(df)} rows across {df['SHIP_NAME'].nunique()} ships")

denominations = sorted(df['DENOMINATION'].unique())
denom_labels = {d: f'PCT_{str(d).replace(".", "_")}' for d in denominations}
print(f"Denominations: {denominations}")

pivot = df.pivot_table(index='SHIP_NAME', columns='DENOMINATION', values='DEMAND_PCT', aggfunc='first').reset_index()
pivot.columns = ['SHIP_NAME'] + [denom_labels[d] for d in denominations]

features_df = df.groupby('SHIP_NAME').first().reset_index()[[
    'SHIP_NAME', 'AVG_PASSENGER_AGE', 'PCT_HIGH_TIER', 'PCT_HIGH_INCOME',
    'PCT_HIGH_RISK', 'AVG_CRUISES', 'AVG_LIFETIME_SPEND', 'UNIQUE_PLAYERS'
]]

ship_le = LabelEncoder()
features_df['SHIP_ENCODED'] = ship_le.fit_transform(features_df['SHIP_NAME'])

train_df = features_df.merge(pivot, on='SHIP_NAME')

feature_cols = ['SHIP_ENCODED', 'AVG_PASSENGER_AGE', 'PCT_HIGH_TIER', 'PCT_HIGH_INCOME',
                'PCT_HIGH_RISK', 'AVG_CRUISES', 'AVG_LIFETIME_SPEND', 'UNIQUE_PLAYERS']
target_cols = [denom_labels[d] for d in denominations]

X = train_df[feature_cols].values
y = train_df[target_cols].values

print(f"\nFeatures shape: {X.shape}, Targets shape: {y.shape}")
print(f"Feature columns: {feature_cols}")
print(f"Target columns: {target_cols}")

print("\nTraining multi-output GBR with LOO cross-validation...")
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
print(f"LOO CV - MAE: {mae:.2f}%, R²: {r2:.4f}")

print("\nPer-denomination MAE:")
for i, col in enumerate(target_cols):
    col_mae = mean_absolute_error(y[:, i], y_pred_loo[:, i])
    print(f"  {col}: {col_mae:.2f}%")

print("\nTraining final model on all data...")
model.fit(X, y)

print("\nSample prediction for Carnival Breeze:")
breeze_idx = list(ship_le.classes_).index('Carnival Breeze')
breeze_features = X[train_df['SHIP_ENCODED'] == breeze_idx][0:1]
pred = model.predict(breeze_features)
pred = np.clip(pred, 0, 100)
pred = pred / pred.sum() * 100

print(f"  {'Denomination':<15} {'Predicted %':<12} {'Actual %':<12}")
for i, d in enumerate(denominations):
    actual = y[train_df['SHIP_ENCODED'].values == breeze_idx][0][i]
    print(f"  ${d:<14} {pred[0][i]:>8.1f}%    {actual:>8.1f}%")

def allocate_bank(demand_pcts, bank_size, denominations):
    raw = demand_pcts * bank_size / 100.0
    allocated = np.floor(raw).astype(int)
    remainder = raw - allocated
    shortfall = bank_size - allocated.sum()
    if shortfall > 0:
        indices = np.argsort(-remainder)
        for i in range(int(shortfall)):
            allocated[indices[i]] += 1
    return allocated

print("\nBank allocation example (24 machines on Carnival Breeze):")
allocation = allocate_bank(pred[0], 24, denominations)
for i, d in enumerate(denominations):
    if allocation[i] > 0:
        print(f"  ${d:<8} -> {allocation[i]} machines ({pred[0][i]:.1f}% demand)")

artifact = {
    'model': model,
    'ship_label_encoder': ship_le,
    'feature_cols': feature_cols,
    'target_cols': target_cols,
    'denominations': denominations,
    'denom_labels': denom_labels,
    'ship_profiles': train_df.to_dict('records'),
}

pkl_path = '/Users/mmalveira/CoCo_Carnival/bank_allocation_model.pkl'
with open(pkl_path, 'wb') as f:
    pickle.dump(artifact, f)
print(f"\nModel saved to {pkl_path}")

print("\nRegistering to Snowflake Model Registry...")
from snowflake.ml.registry import Registry

reg = Registry(session=session, database_name="CARNIVAL_CASINO", schema_name="SLOT_ANALYTICS")

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
    print(f"Registered: {mv.model_name} version {mv.version_name}")
    print(mv.show_functions())
except Exception as e:
    print(f"Registry error: {e}")
    print("Model saved locally — will use pickle for Streamlit inference")

print("\nDone!")
session.close()
