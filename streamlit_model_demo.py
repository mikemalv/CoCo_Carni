import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Slot Denomination Predictor", page_icon="🎰", layout="wide")
st.title("🎰 Slot Denomination Predictor")
st.markdown("Predict optimal slot machine denominations based on patron demographics")

session = get_active_session()

SHIP_MAPPING = {"Carnival Breeze": 0, "Carnival Celebration": 1, "Carnival Horizon": 2, "Carnival Jubilee": 3, "Carnival Magic": 4, "Carnival Panorama": 5, "Carnival Vista": 6, "Mardi Gras": 7}
GAME_MAPPING = {"Classic Slots": 0, "Bonus Slots": 1, "Progressive Slots": 2, "Video Poker": 3}
GENDER_MAPPING = {"Female": 0, "Male": 1}
TIER_MAPPING = {"Basic": 0, "Bronze": 1, "Gold": 2, "Platinum": 3, "Silver": 4}
RISK_MAPPING = {"Conservative": 0, "High": 1, "Moderate": 2}
INCOME_MAPPING = {"Under $50K": 0, "$50K-$75K": 1, "$75K-$100K": 2, "$100K-$150K": 3, "Over $150K": 4}
DENOM_CLASSES = {0: "$0.01", 1: "$0.05", 2: "$0.25", 3: "$1.00", 4: "$5.00", 5: "$10.00", 6: "$20.00", 7: "$50.00", 8: "$100.00"}

col1, col2, col3 = st.columns(3)

with col1:
    ship_name = st.selectbox("Ship", list(SHIP_MAPPING.keys()))
    play_month = st.slider("Month", 1, 12, 6)
    play_day = st.slider("Day of Week (0=Mon)", 0, 6, 2)
    is_weekend = st.checkbox("Weekend")

with col2:
    game_type = st.selectbox("Game Type", list(GAME_MAPPING.keys()))
    age = st.number_input("Age", 21, 85, 45)
    gender = st.selectbox("Gender", list(GENDER_MAPPING.keys()))
    tier = st.selectbox("Membership Tier", list(TIER_MAPPING.keys()))

with col3:
    total_cruises = st.number_input("Total Cruises", 1, 50, 5)
    risk = st.selectbox("Risk Appetite", list(RISK_MAPPING.keys()))
    income = st.selectbox("Income Bracket", list(INCOME_MAPPING.keys()))

if st.button("Predict Denomination", type="primary"):
    query = f"""
    SELECT MODEL(CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_DENOMINATION_MODEL, V1)!PREDICT(
        {SHIP_MAPPING[ship_name]}, {play_month}, {play_day}, {1 if is_weekend else 0},
        {GAME_MAPPING[game_type]}, {age}, {GENDER_MAPPING[gender]}, {TIER_MAPPING[tier]},
        {total_cruises}, {RISK_MAPPING[risk]}, {INCOME_MAPPING[income]}
    ):output_feature_0 AS predicted_class
    """
    result = session.sql(query).collect()
    pred_class = int(result[0]["PREDICTED_CLASS"])
    
    st.success(f"### Recommended Denomination: {DENOM_CLASSES.get(pred_class, 'Unknown')}")
    
    proba_query = f"""
    SELECT MODEL(CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_DENOMINATION_MODEL, V1)!PREDICT_PROBA(
        {SHIP_MAPPING[ship_name]}, {play_month}, {play_day}, {1 if is_weekend else 0},
        {GAME_MAPPING[game_type]}, {age}, {GENDER_MAPPING[gender]}, {TIER_MAPPING[tier]},
        {total_cruises}, {RISK_MAPPING[risk]}, {INCOME_MAPPING[income]}
    ) AS probs
    """
    proba_result = session.sql(proba_query).collect()
    probs = proba_result[0]["PROBS"]
    
    st.subheader("Probability Distribution")
    prob_df = pd.DataFrame({"Denomination": list(DENOM_CLASSES.values()), "Probability": [probs[f"output_feature_{i}"] for i in range(9)]})
    st.bar_chart(prob_df.set_index("Denomination"))
