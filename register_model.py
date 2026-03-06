import sys
sys.path.insert(0, '/Users/mmalveira/CoCo_Carnival')
from snowpark_session import create_snowpark_session
import pandas as pd
import pickle
from snowflake.ml.registry import Registry

session = create_snowpark_session()
session.use_database("CARNIVAL_CASINO")
session.use_schema("SLOT_ANALYTICS")

print("Loading model from pickle file...")
with open('/Users/mmalveira/CoCo_Carnival/denomination_classifier.pkl', 'rb') as f:
    model_data = pickle.load(f)

model = model_data['model']
label_encoders = model_data['label_encoders']
features = model_data['features']

sample_input = pd.DataFrame({
    'SHIP_NAME': [0, 1, 2],
    'PLAY_MONTH': [6, 12, 3],
    'PLAY_DAY_OF_WEEK': [0, 5, 2],
    'IS_WEEKEND': [0, 1, 0],
    'GAME_TYPE': [0, 1, 2],
    'AGE': [35, 55, 42],
    'GENDER': [0, 1, 0],
    'MEMBERSHIP_TIER': [0, 1, 2],
    'TOTAL_CRUISES': [5, 12, 3],
    'RISK_APPETITE': [1, 2, 0],
    'INCOME_BRACKET': [2, 4, 1]
})

print("Registering model to Snowflake Model Registry...")
reg = Registry(session=session, database_name="CARNIVAL_CASINO", schema_name="SLOT_ANALYTICS")

try:
    mv = reg.log_model(
        model,
        model_name="DENOMINATION_CLASSIFIER",
        version_name="v1",
        sample_input_data=sample_input,
        conda_dependencies=["scikit-learn"],
        target_platforms=["WAREHOUSE", "SNOWPARK_CONTAINER_SERVICES"],
        comment="RandomForest classifier to predict optimal slot denomination based on patron demographics and trip context"
    )
    print(f"Model registered: {mv.model_name} version {mv.version_name}")
except Exception as e:
    print(f"Warehouse registration failed: {e}")
    print("Trying SPCS-only target...")
    mv = reg.log_model(
        model,
        model_name="DENOMINATION_CLASSIFIER",
        version_name="v1",
        sample_input_data=sample_input,
        pip_requirements=["scikit-learn"],
        target_platforms=["SNOWPARK_CONTAINER_SERVICES"],
        comment="RandomForest classifier to predict optimal slot denomination based on patron demographics and trip context"
    )
    print(f"Model registered (SPCS only): {mv.model_name} version {mv.version_name}")

print("\nAvailable functions:")
print(mv.show_functions())
