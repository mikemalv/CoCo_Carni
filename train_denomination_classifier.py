import sys
sys.path.insert(0, '/Users/mmalveira/CoCo_Carnival')
from snowpark_session import create_snowpark_session
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, accuracy_score, f1_score
import pickle

session = create_snowpark_session()

print("Loading data from Snowflake...")
query = """
SELECT 
    p.SHIP_NAME,
    p.PLAY_DATE,
    p.DENOMINATION,
    p.GAME_TYPE,
    p.TOTAL_WAGERED,
    m.AGE,
    m.GENDER,
    m.MEMBERSHIP_TIER,
    m.HOME_STATE,
    m.TOTAL_CRUISES,
    m.LIFETIME_SPEND,
    m.RISK_APPETITE,
    m.INCOME_BRACKET
FROM CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_PLAY_HISTORY p
JOIN CARNIVAL_CASINO.SLOT_ANALYTICS.MEMBER_DEMOGRAPHICS m 
    ON p.MEMBER_ID = m.MEMBER_ID
"""
df = session.sql(query).to_pandas()
print(f"Loaded {len(df):,} rows")

df['PLAY_MONTH'] = pd.to_datetime(df['PLAY_DATE']).dt.month
df['PLAY_DAY_OF_WEEK'] = pd.to_datetime(df['PLAY_DATE']).dt.dayofweek
df['IS_WEEKEND'] = df['PLAY_DAY_OF_WEEK'].isin([5, 6]).astype(int)

features = ['SHIP_NAME', 'PLAY_MONTH', 'PLAY_DAY_OF_WEEK', 'IS_WEEKEND', 
            'GAME_TYPE', 'AGE', 'GENDER', 'MEMBERSHIP_TIER', 
            'TOTAL_CRUISES', 'RISK_APPETITE', 'INCOME_BRACKET']

target = 'DENOMINATION'

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

print(f"\nTraining set: {len(X_train):,} samples")
print(f"Test set: {len(X_test):,} samples")
print(f"Denomination classes: {list(denom_le.classes_)}")

print("\nTraining Random Forest Classifier...")
model = RandomForestClassifier(
    n_estimators=100,
    max_depth=15,
    min_samples_split=10,
    random_state=42,
    n_jobs=-1
)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)

accuracy = accuracy_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred, average='weighted')

print(f"\n{'='*50}")
print("MODEL EVALUATION")
print(f"{'='*50}")
print(f"Accuracy: {accuracy:.4f}")
print(f"F1 Score (weighted): {f1:.4f}")
print(f"\nClassification Report:")
print(classification_report(y_test, y_pred, target_names=[f"${d}" for d in denom_le.classes_]))

print(f"\n{'='*50}")
print("FEATURE IMPORTANCE")
print(f"{'='*50}")
importance_df = pd.DataFrame({
    'feature': features,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)
for _, row in importance_df.iterrows():
    print(f"{row['feature']:25s} {row['importance']:.4f}")

model_path = '/Users/mmalveira/CoCo_Carnival/denomination_classifier.pkl'
model_data = {
    'model': model,
    'label_encoders': label_encoders,
    'features': features,
    'categorical_cols': categorical_cols,
    'target': target
}
with open(model_path, 'wb') as f:
    pickle.dump(model_data, f)

print(f"\n{'='*50}")
print(f"Model saved to: {model_path}")
print(f"{'='*50}")

session.close()
