import os
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
import pickle
import snowflake.connector
from snowflake.ml.registry import Registry
from snowflake.snowpark import Session

connection_name = os.getenv("SNOWFLAKE_CONNECTION_NAME") or "demo"
session = Session.builder.config("connection_name", connection_name).create()
session.use_database("CARNIVAL_CASINO")
session.use_schema("SLOT_ANALYTICS")

print("Loading data from Snowflake...")
query = """
SELECT 
    p.SHIP_NAME,
    p.PLAY_DATE,
    p.DENOMINATION,
    p.GAME_TYPE,
    m.AGE,
    m.GENDER,
    m.MEMBERSHIP_TIER,
    m.TOTAL_CRUISES,
    m.RISK_APPETITE,
    m.INCOME_BRACKET
FROM CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_PLAY_HISTORY p
JOIN CARNIVAL_CASINO.SLOT_ANALYTICS.MEMBER_DEMOGRAPHICS m 
    ON p.MEMBER_ID = m.MEMBER_ID
"""
df = session.sql(query).to_pandas()
print(f"Loaded {len(df)} rows")

df['PLAY_MONTH'] = pd.to_datetime(df['PLAY_DATE']).dt.month
df['PLAY_DAY_OF_WEEK'] = pd.to_datetime(df['PLAY_DATE']).dt.dayofweek
df['IS_WEEKEND'] = df['PLAY_DAY_OF_WEEK'].isin([5, 6]).astype(int)

features = ['SHIP_NAME', 'PLAY_MONTH', 'PLAY_DAY_OF_WEEK', 'IS_WEEKEND', 
            'GAME_TYPE', 'AGE', 'GENDER', 'MEMBERSHIP_TIER', 
            'TOTAL_CRUISES', 'RISK_APPETITE', 'INCOME_BRACKET']

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

print("Training model...")
model = RandomForestClassifier(n_estimators=100, max_depth=15, min_samples_split=10, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f"Model accuracy: {accuracy:.2%}")

with open('/Users/mmalveira/CoCo_Carnival/denomination_classifier.pkl', 'wb') as f:
    pickle.dump({'model': model, 'label_encoders': label_encoders, 'features': features, 'categorical_cols': categorical_cols}, f)
print("Model saved to pickle file")

print("\nRegistering model to Snowflake Model Registry...")
reg = Registry(session=session, database_name="CARNIVAL_CASINO", schema_name="SLOT_ANALYTICS")

sample_input = X_train.head(10)

try:
    mv = reg.log_model(
        model,
        model_name="SLOT_DENOMINATION_MODEL",
        version_name="v1",
        sample_input_data=sample_input,
        conda_dependencies=["scikit-learn"],
        target_platforms=["WAREHOUSE", "SNOWPARK_CONTAINER_SERVICES"],
        comment="RandomForest classifier to predict optimal slot denomination based on patron demographics"
    )
    print(f"Model registered: {mv.model_name} version {mv.version_name}")
    print("\nAvailable functions:")
    print(mv.show_functions())
except Exception as e:
    print(f"Error: {e}")
    print("\nTrying SPCS-only target...")
    mv = reg.log_model(
        model,
        model_name="SLOT_DENOMINATION_MODEL",
        version_name="v1",
        sample_input_data=sample_input,
        pip_requirements=["scikit-learn"],
        target_platforms=["SNOWPARK_CONTAINER_SERVICES"],
        comment="RandomForest classifier to predict optimal slot denomination based on patron demographics"
    )
    print(f"Model registered (SPCS only): {mv.model_name} version {mv.version_name}")
