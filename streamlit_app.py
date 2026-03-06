import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
import os
import json

CARNIVAL_RED = "#B61B38"
CARNIVAL_BLUE = "#014E8F"
CARNIVAL_NAVY = "#002D62"
CARNIVAL_GOLD = "#F59E0B"
CARNIVAL_LIGHT_BLUE = "#4A90D9"

st.set_page_config(page_title="Carnival Casino Slot Analytics", layout="wide", page_icon="🎰")

DB = "CARNIVAL_CASINO"
SCHEMA = "SLOT_ANALYTICS"

def _create_session():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session(), True
    except Exception:
        pass
    conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    from snowflake.snowpark import Session
    sess = Session.builder.config("connection_name", conn_name).create()
    sess.sql(f"USE DATABASE {DB}").collect()
    sess.sql(f"USE SCHEMA {SCHEMA}").collect()
    return sess, False

def _get_session():
    if "snowpark_session" not in st.session_state:
        sess, is_sis = _create_session()
        st.session_state["snowpark_session"] = sess
        st.session_state["is_sis"] = is_sis
    return st.session_state["snowpark_session"]

def _reconnect():
    if "snowpark_session" in st.session_state:
        try:
            st.session_state["snowpark_session"].close()
        except Exception:
            pass
        del st.session_state["snowpark_session"]
    return _get_session()

st.markdown(f'''
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    .stApp {{
        font-family: 'Inter', sans-serif;
    }}
    .stApp header {{
        background: linear-gradient(135deg, {CARNIVAL_NAVY}, {CARNIVAL_BLUE}) !important;
        backdrop-filter: blur(10px);
    }}
    .stApp [data-testid="stHeader"] {{
        background: linear-gradient(135deg, {CARNIVAL_NAVY}, {CARNIVAL_BLUE}) !important;
    }}
    div[data-testid="stMetric"] {{
        background: linear-gradient(145deg, rgba(1,78,143,0.12), rgba(182,27,56,0.06));
        border-radius: 12px;
        padding: 18px 20px;
        border: 1px solid rgba(1,78,143,0.15);
        border-left: 4px solid {CARNIVAL_BLUE};
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }}
    div[data-testid="stMetric"]:hover {{
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(1,78,143,0.15);
    }}
    div[data-testid="stMetric"] label {{
        font-size: 0.8rem !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        opacity: 0.8;
    }}
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {{
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }}
    .stTabs [data-baseweb="tab-list"] {{
        gap: 4px;
        padding: 4px;
        background: rgba(1,78,143,0.06);
        border-radius: 12px;
    }}
    .stTabs [data-baseweb="tab"] {{
        color: {CARNIVAL_BLUE};
        border-radius: 8px;
        padding: 8px 16px;
        font-weight: 500;
        font-size: 0.9rem;
        transition: all 0.2s ease;
    }}
    .stTabs [data-baseweb="tab"]:hover {{
        background: rgba(1,78,143,0.08);
    }}
    .stTabs [data-baseweb="tab"][aria-selected="true"] {{
        background: linear-gradient(135deg, {CARNIVAL_BLUE}, {CARNIVAL_LIGHT_BLUE}) !important;
        color: white !important;
        font-weight: 600;
        box-shadow: 0 2px 8px rgba(1,78,143,0.3);
    }}
    .stTabs [data-baseweb="tab-highlight"] {{
        display: none;
    }}
    div[data-testid="stExpander"] {{
        border: 1px solid rgba(1,78,143,0.12);
        border-radius: 10px;
        overflow: hidden;
    }}
    div[data-testid="stExpander"]:hover {{
        border-color: rgba(1,78,143,0.25);
    }}
    .hero-container {{
        background: linear-gradient(135deg, {CARNIVAL_NAVY} 0%, {CARNIVAL_BLUE} 60%, {CARNIVAL_LIGHT_BLUE} 100%);
        border-radius: 16px;
        padding: 28px 36px;
        margin-bottom: 8px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        box-shadow: 0 4px 20px rgba(0,45,98,0.25);
        position: relative;
        overflow: hidden;
    }}
    .hero-container::before {{
        content: '';
        position: absolute;
        top: -50%;
        right: -20%;
        width: 400px;
        height: 400px;
        background: radial-gradient(circle, rgba(255,255,255,0.05) 0%, transparent 70%);
        pointer-events: none;
    }}
    .hero-title {{
        font-size: 2rem;
        font-weight: 700;
        color: white;
        margin: 0;
        letter-spacing: -0.02em;
    }}
    .hero-subtitle {{
        font-size: 0.85rem;
        color: rgba(255,255,255,0.7);
        font-weight: 500;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        margin: 4px 0 0 0;
    }}
    .hero-tagline {{
        font-size: 0.95rem;
        color: {CARNIVAL_GOLD};
        font-weight: 600;
        letter-spacing: 0.1em;
        margin: 8px 0 0 0;
    }}
    .hero-funnel {{
        width: 50px;
        height: 55px;
        position: relative;
        margin-right: 20px;
        flex-shrink: 0;
    }}
    .section-header {{
        font-size: 1.1rem;
        font-weight: 600;
        color: {CARNIVAL_BLUE};
        margin: 0 0 16px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(1,78,143,0.15);
        display: flex;
        align-items: center;
        gap: 8px;
    }}
    .card {{
        background: linear-gradient(145deg, rgba(1,78,143,0.06), rgba(182,27,56,0.03));
        border: 1px solid rgba(1,78,143,0.1);
        border-radius: 12px;
        padding: 20px;
    }}
    .model-card {{
        background: linear-gradient(145deg, rgba(1,78,143,0.08), rgba(0,45,98,0.04));
        border: 1px solid rgba(1,78,143,0.15);
        border-radius: 14px;
        padding: 24px;
        margin-bottom: 12px;
        transition: transform 0.2s ease;
    }}
    .model-card:hover {{
        transform: translateY(-1px);
        box-shadow: 0 4px 15px rgba(1,78,143,0.1);
    }}
    .model-name {{
        font-size: 1.05rem;
        font-weight: 600;
        color: {CARNIVAL_BLUE};
        margin: 0;
    }}
    .model-type {{
        font-size: 0.8rem;
        color: rgba(255,255,255,0.6);
        font-weight: 400;
    }}
    .stat-pill {{
        display: inline-block;
        background: rgba(1,78,143,0.15);
        border-radius: 20px;
        padding: 4px 14px;
        font-size: 0.78rem;
        font-weight: 500;
        color: {CARNIVAL_LIGHT_BLUE};
        margin-right: 8px;
    }}
    .prediction-result {{
        background: linear-gradient(135deg, {CARNIVAL_NAVY}, {CARNIVAL_BLUE});
        border-radius: 14px;
        padding: 24px 28px;
        text-align: center;
        box-shadow: 0 4px 20px rgba(0,45,98,0.3);
    }}
    .prediction-label {{
        font-size: 0.8rem;
        color: rgba(255,255,255,0.6);
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin: 0;
    }}
    .prediction-value {{
        font-size: 2.4rem;
        font-weight: 700;
        color: {CARNIVAL_GOLD};
        margin: 4px 0 0 0;
    }}
    .policy-card {{
        border-left: 3px solid {CARNIVAL_BLUE};
        padding-left: 16px;
        margin-bottom: 8px;
    }}
    .policy-cat-header {{
        font-size: 1rem;
        font-weight: 600;
        color: {CARNIVAL_BLUE};
        padding: 8px 16px;
        background: rgba(1,78,143,0.08);
        border-radius: 8px;
        margin: 16px 0 8px 0;
        display: flex;
        align-items: center;
        gap: 8px;
    }}
    div[data-testid="stDataFrame"] {{
        border: 1px solid rgba(1,78,143,0.1);
        border-radius: 10px;
        overflow: hidden;
    }}
    .stButton > button[kind="primary"] {{
        background: linear-gradient(135deg, {CARNIVAL_BLUE}, {CARNIVAL_LIGHT_BLUE}) !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        letter-spacing: 0.02em;
        transition: all 0.2s ease !important;
        box-shadow: 0 2px 8px rgba(1,78,143,0.25) !important;
    }}
    .stButton > button[kind="primary"]:hover {{
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 15px rgba(1,78,143,0.35) !important;
    }}
    .stSelectbox > div > div {{
        border-radius: 8px !important;
    }}
    .stNumberInput > div > div > input {{
        border-radius: 8px !important;
    }}
    div.stSlider > div > div > div > div {{
        background-color: {CARNIVAL_BLUE} !important;
    }}
</style>
''', unsafe_allow_html=True)

funnel_svg = '''<svg width="50" height="55" viewBox="0 0 50 55" fill="none" xmlns="http://www.w3.org/2000/svg">
  <path d="M5 10 L25 10 L35 45 L15 45 Z" fill="#B61B38" opacity="0.9"/>
  <path d="M25 10 L45 10 L35 45 L15 45 Z" fill="white" opacity="0.2"/>
  <path d="M15 8 L5 10 L25 10 Z" fill="#B61B38"/>
  <path d="M25 8 L25 10 L45 10 L35 8 Z" fill="white" opacity="0.15"/>
  <ellipse cx="25" cy="8" rx="17" ry="5" fill="white" opacity="0.9" stroke="rgba(255,255,255,0.3)" stroke-width="1"/>
</svg>'''
import base64
funnel_b64 = base64.b64encode(funnel_svg.encode()).decode()

st.markdown(f'''
<div class="hero-container">
    <div style="display:flex;align-items:center;">
        <img src="data:image/svg+xml;base64,{funnel_b64}" class="hero-funnel"/>
        <div>
            <p class="hero-subtitle">Carnival Cruise Line</p>
            <h1 class="hero-title">Casino Slot Analytics</h1>
            <p class="hero-tagline">FUN FOR ALL. ALL FOR FUN.</p>
        </div>
    </div>
    <div style="text-align:right;opacity:0.5;font-size:0.75rem;color:rgba(255,255,255,0.5);">
        Powered by Snowflake
    </div>
</div>
''', unsafe_allow_html=True)

@st.cache_data(ttl=300)
def run_query(sql):
    try:
        return _get_session().sql(sql).to_pandas()
    except Exception as e:
        if "expired" in str(e).lower() or "390114" in str(e):
            st.cache_data.clear()
            return _reconnect().sql(sql).to_pandas()
        raise

alt_theme = {
    "config": {
        "background": "transparent",
        "axis": {
            "labelColor": "rgba(255,255,255,0.65)",
            "titleColor": "rgba(255,255,255,0.8)",
            "gridColor": "rgba(255,255,255,0.06)",
            "domainColor": "rgba(255,255,255,0.15)",
            "tickColor": "rgba(255,255,255,0.1)",
            "labelFont": "Inter, sans-serif",
            "titleFont": "Inter, sans-serif",
            "labelFontSize": 11,
            "titleFontSize": 12,
            "titleFontWeight": 500,
        },
        "legend": {
            "labelColor": "rgba(255,255,255,0.7)",
            "titleColor": "rgba(255,255,255,0.8)",
            "labelFont": "Inter, sans-serif",
            "titleFont": "Inter, sans-serif",
        },
        "title": {
            "color": "rgba(255,255,255,0.85)",
            "font": "Inter, sans-serif",
        },
        "view": {"stroke": "transparent"},
    }
}

def themed_chart(chart):
    return chart.configure_view(stroke="transparent").configure_axis(
        labelColor="rgba(255,255,255,0.65)",
        titleColor="rgba(255,255,255,0.8)",
        gridColor="rgba(255,255,255,0.06)",
        domainColor="rgba(255,255,255,0.15)",
        labelFont="Inter, sans-serif",
        titleFont="Inter, sans-serif",
        labelFontSize=11,
        titleFontSize=12,
    ).configure_legend(
        labelColor="rgba(255,255,255,0.7)",
        titleColor="rgba(255,255,255,0.8)",
        labelFont="Inter, sans-serif",
    ).configure_view(stroke="transparent")

tab1, tab2, tab3, tab4 = st.tabs([
    "👥 Member Overview",
    "🎰 Slot Analytics",
    "🤖 ML Models",
    "📋 Policies & Info"
])

with tab1:
    members = run_query(f"SELECT COUNT(*) AS CNT FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS")
    plays = run_query(f"SELECT COUNT(*) AS CNT FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY")
    total_wagered = run_query(f"SELECT ROUND(SUM(TOTAL_WAGERED),0) AS TOTAL FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY")
    avg_sessions = run_query(f"SELECT ROUND(AVG(TOTAL_SESSIONS),1) AS AVG_S FROM {DB}.{SCHEMA}.MEMBER_SLOT_FEATURES")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Members", f"{members['CNT'][0]:,}")
    col2.metric("Play Sessions", f"{plays['CNT'][0]:,}")
    col3.metric("Total Wagered", f"${total_wagered['TOTAL'][0]:,.0f}")
    col4.metric("Avg Sessions / Member", f"{avg_sessions['AVG_S'][0]}")

    st.markdown("")
    c1, c2 = st.columns(2)

    with c1:
        st.markdown('##### 📊 Membership Tier Distribution')
        tier_data = run_query(f"""
            SELECT MEMBERSHIP_TIER, COUNT(*) AS MEMBERS
            FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS
            GROUP BY MEMBERSHIP_TIER ORDER BY MEMBERS DESC
        """)
        base = alt.Chart(tier_data).encode(
            theta=alt.Theta('MEMBERS:Q', stack=True),
            color=alt.Color('MEMBERSHIP_TIER:N', legend=alt.Legend(orient='bottom', title=None, labelFontSize=11),
                scale=alt.Scale(domain=['Platinum','Gold','Silver','Bronze','Basic'],
                               range=[CARNIVAL_BLUE, CARNIVAL_GOLD,'#9CA3AF','#CD7F32',CARNIVAL_RED]))
        )
        pie = base.mark_arc(innerRadius=60, outerRadius=120, cornerRadius=4, padAngle=0.02)
        text = base.mark_text(radius=140, size=11, fill="rgba(255,255,255,0.7)").encode(
            text=alt.Text('MEMBERS:Q', format=',')
        )
        chart = (pie + text).properties(height=300)
        chart = themed_chart(chart)
        st.altair_chart(chart, use_container_width=True)

    with c2:
        st.markdown('##### 👤 Age Distribution by Gender')
        age_data = run_query(f"""
            SELECT 
                FLOOR(AGE/10)*10 AS AGE_GROUP,
                GENDER,
                COUNT(*) AS CNT
            FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS
            GROUP BY AGE_GROUP, GENDER ORDER BY AGE_GROUP
        """)
        age_data['AGE_GROUP'] = age_data['AGE_GROUP'].astype(str) + 's'
        chart = alt.Chart(age_data).mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5, size=18).encode(
            x=alt.X('AGE_GROUP:N', title='Age Group', axis=alt.Axis(labelAngle=0)),
            y=alt.Y('CNT:Q', title='Members'),
            color=alt.Color('GENDER:N', legend=alt.Legend(orient='bottom', title=None),
                scale=alt.Scale(domain=['M','F'], range=[CARNIVAL_BLUE, CARNIVAL_RED])),
            xOffset='GENDER:N'
        ).properties(height=300)
        chart = themed_chart(chart)
        st.altair_chart(chart, use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        st.markdown('##### 💰 Income Bracket Distribution')
        income = run_query(f"""
            SELECT INCOME_BRACKET, COUNT(*) AS CNT FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS
            GROUP BY INCOME_BRACKET ORDER BY CNT DESC
        """)
        chart = alt.Chart(income).mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5).encode(
            x=alt.X('INCOME_BRACKET:N', sort='-y', title='Income Bracket', axis=alt.Axis(labelAngle=-30)),
            y=alt.Y('CNT:Q', title='Members'),
            color=alt.value(CARNIVAL_LIGHT_BLUE)
        ).properties(height=260)
        chart = themed_chart(chart)
        st.altair_chart(chart, use_container_width=True)

    with c4:
        st.markdown('##### 🗺️ Top Home States')
        state = run_query(f"""
            SELECT HOME_STATE, COUNT(*) AS MEMBERS FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS
            GROUP BY HOME_STATE ORDER BY MEMBERS DESC LIMIT 15
        """)
        chart = alt.Chart(state).mark_bar(cornerRadiusTopRight=5, cornerRadiusBottomRight=5).encode(
            y=alt.Y('HOME_STATE:N', sort='-x', title='State'),
            x=alt.X('MEMBERS:Q', title='Members'),
            color=alt.Color('MEMBERS:Q', scale=alt.Scale(scheme='blues'), legend=None)
        ).properties(height=260)
        chart = themed_chart(chart)
        st.altair_chart(chart, use_container_width=True)

with tab2:
    ship_filter = st.selectbox("Filter by ship", ["All Ships"] + run_query(f"""
        SELECT DISTINCT SHIP_NAME FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY ORDER BY SHIP_NAME
    """)['SHIP_NAME'].tolist(), key="ship_filter")

    where = f"WHERE SHIP_NAME = '{ship_filter}'" if ship_filter != "All Ships" else ""

    m1, m2, m3, m4 = st.columns(4)
    ship_stats = run_query(f"""
        SELECT 
            COUNT(DISTINCT MEMBER_ID) AS UNIQUE_PLAYERS,
            ROUND(AVG(BET_PER_SPIN),2) AS AVG_BET,
            ROUND(AVG(SESSION_DURATION_MINS),1) AS AVG_DURATION,
            ROUND(SUM(TOTAL_WON)/NULLIF(SUM(TOTAL_WAGERED),0)*100,1) AS WIN_RATE
        FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where}
    """)
    m1.metric("Unique Players", f"{ship_stats['UNIQUE_PLAYERS'][0]:,}")
    m2.metric("Avg Bet / Spin", f"${ship_stats['AVG_BET'][0]}")
    m3.metric("Avg Session (min)", f"{ship_stats['AVG_DURATION'][0]}")
    m4.metric("Win Rate", f"{ship_stats['WIN_RATE'][0]}%")

    st.markdown("")
    c1, c2 = st.columns(2)

    with c1:
        st.markdown('##### 💵 Revenue by Denomination')
        denom = run_query(f"""
            SELECT DENOMINATION, ROUND(SUM(TOTAL_WAGERED),0) AS TOTAL_WAGERED, COUNT(*) AS SESSIONS
            FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where}
            GROUP BY DENOMINATION ORDER BY DENOMINATION
        """)
        denom['DENOMINATION'] = '$' + denom['DENOMINATION'].astype(str)
        chart = alt.Chart(denom).mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5).encode(
            x=alt.X('DENOMINATION:N', title='Denomination', sort=None, axis=alt.Axis(labelAngle=-30)),
            y=alt.Y('TOTAL_WAGERED:Q', title='Total Wagered ($)', axis=alt.Axis(format='~s')),
            color=alt.Color('TOTAL_WAGERED:Q', scale=alt.Scale(scheme='blues'), legend=None),
            tooltip=[
                alt.Tooltip('DENOMINATION:N', title='Denom'),
                alt.Tooltip('TOTAL_WAGERED:Q', title='Wagered', format='$,.0f'),
                alt.Tooltip('SESSIONS:Q', title='Sessions', format=',')
            ]
        ).properties(height=320)
        chart = themed_chart(chart)
        st.altair_chart(chart, use_container_width=True)

    with c2:
        st.markdown('##### 🏆 Top Games by Sessions')
        games = run_query(f"""
            SELECT GAME_NAME, COUNT(*) AS SESSIONS
            FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where}
            GROUP BY GAME_NAME ORDER BY SESSIONS DESC LIMIT 10
        """)
        chart = alt.Chart(games).mark_bar(cornerRadiusTopRight=5, cornerRadiusBottomRight=5).encode(
            x=alt.X('SESSIONS:Q', title='Sessions', axis=alt.Axis(format='~s')),
            y=alt.Y('GAME_NAME:N', sort='-x', title='Game'),
            color=alt.Color('SESSIONS:Q', scale=alt.Scale(range=[CARNIVAL_RED, CARNIVAL_GOLD]), legend=None),
            tooltip=[
                alt.Tooltip('GAME_NAME:N', title='Game'),
                alt.Tooltip('SESSIONS:Q', title='Sessions', format=',')
            ]
        ).properties(height=320)
        chart = themed_chart(chart)
        st.altair_chart(chart, use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        st.markdown('##### 🕐 Play Patterns by Time of Day')
        tod = run_query(f"""
            SELECT TIME_OF_DAY, ROUND(AVG(BET_PER_SPIN),2) AS AVG_BET, COUNT(*) AS SESSIONS
            FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where}
            GROUP BY TIME_OF_DAY ORDER BY SESSIONS DESC
        """)
        chart = alt.Chart(tod).mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5).encode(
            x=alt.X('TIME_OF_DAY:N', title='Time of Day', sort='-y'),
            y=alt.Y('SESSIONS:Q', title='Sessions'),
            color=alt.value(CARNIVAL_LIGHT_BLUE),
            tooltip=[
                alt.Tooltip('TIME_OF_DAY:N', title='Time'),
                alt.Tooltip('SESSIONS:Q', title='Sessions', format=','),
                alt.Tooltip('AVG_BET:Q', title='Avg Bet', format='$.2f')
            ]
        ).properties(height=260)
        chart = themed_chart(chart)
        st.altair_chart(chart, use_container_width=True)

    with c4:
        st.markdown('##### 🎮 Game Type Distribution')
        gtype = run_query(f"""
            SELECT GAME_TYPE, COUNT(*) AS SESSIONS, ROUND(AVG(BET_PER_SPIN),2) AS AVG_BET
            FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where}
            GROUP BY GAME_TYPE ORDER BY SESSIONS DESC
        """)
        base = alt.Chart(gtype).encode(
            theta=alt.Theta('SESSIONS:Q', stack=True),
            color=alt.Color('GAME_TYPE:N', legend=alt.Legend(orient='bottom', title=None),
                scale=alt.Scale(range=[CARNIVAL_BLUE, CARNIVAL_RED, CARNIVAL_GOLD, CARNIVAL_LIGHT_BLUE]))
        )
        pie = base.mark_arc(innerRadius=55, outerRadius=110, cornerRadius=4, padAngle=0.02)
        text = base.mark_text(radius=130, size=11, fill="rgba(255,255,255,0.7)").encode(
            text=alt.Text('GAME_TYPE:N')
        )
        chart = (pie + text).properties(height=260)
        chart = themed_chart(chart)
        st.altair_chart(chart, use_container_width=True)

    st.markdown('##### 📈 Monthly Wagering Trend')
    monthly = run_query(f"""
        SELECT DATE_TRUNC('MONTH', PLAY_DATE)::DATE AS MONTH,
            ROUND(SUM(TOTAL_WAGERED),0) AS TOTAL_WAGERED,
            COUNT(DISTINCT MEMBER_ID) AS UNIQUE_PLAYERS
        FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where}
        GROUP BY MONTH ORDER BY MONTH
    """)
    area = alt.Chart(monthly).mark_area(
        line={"color": CARNIVAL_BLUE, "strokeWidth": 2},
        color=alt.Gradient(
            gradient='linear',
            stops=[
                alt.GradientStop(color=CARNIVAL_BLUE, offset=0),
                alt.GradientStop(color='transparent', offset=1)
            ],
            x1=1, x2=1, y1=1, y2=0
        ),
        interpolate='monotone',
        opacity=0.3
    ).encode(
        x=alt.X('MONTH:T', title='Month', axis=alt.Axis(format='%b %Y')),
        y=alt.Y('TOTAL_WAGERED:Q', title='Total Wagered ($)', axis=alt.Axis(format='~s')),
        tooltip=[
            alt.Tooltip('MONTH:T', title='Month', format='%B %Y'),
            alt.Tooltip('TOTAL_WAGERED:Q', title='Wagered', format='$,.0f'),
            alt.Tooltip('UNIQUE_PLAYERS:Q', title='Players', format=',')
        ]
    ).properties(height=280)
    points = alt.Chart(monthly).mark_circle(size=40, color=CARNIVAL_BLUE).encode(
        x='MONTH:T',
        y='TOTAL_WAGERED:Q'
    )
    chart = themed_chart(area + points)
    st.altair_chart(chart, use_container_width=True)

with tab3:
    st.markdown('##### 📊 Model Performance Summary')

    eval_summary = run_query(f"SELECT * FROM {DB}.{SCHEMA}.MODEL_EVALUATION_SUMMARY")
    for _, row in eval_summary.iterrows():
        improvement = round(row['MODEL_ACCURACY_PCT'] - row['BASELINE_ACCURACY_PCT'], 2)
        imp_color = "#22c55e" if improvement > 0 else CARNIVAL_RED

        st.markdown(f'''
        <div class="model-card">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                <div>
                    <p class="model-name">{row['MODEL_NAME']}</p>
                    <span class="model-type">{row['MODEL_TYPE']} &middot; Target: {row['TARGET']}</span>
                </div>
                <div style="text-align:right;">
                    <span style="font-size:1.6rem;font-weight:700;color:{CARNIVAL_GOLD};">{row['MODEL_ACCURACY_PCT']}%</span>
                    <span style="font-size:0.75rem;color:rgba(255,255,255,0.5);display:block;">accuracy</span>
                </div>
            </div>
            <div style="margin-top:8px;">
                <span class="stat-pill">Baseline: {row['BASELINE_ACCURACY_PCT']}%</span>
                <span class="stat-pill" style="color:{imp_color};">{improvement:+.2f}pp improvement</span>
            </div>
        </div>
        ''', unsafe_allow_html=True)
        st.caption(row['NOTES'])

    st.markdown('##### 🔲 Confusion Matrix')

    denom_preds = run_query(f"""
        SELECT 
            PREFERRED_DENOMINATION AS ACTUAL,
            PREDICTION:class::STRING AS PREDICTED,
            COUNT(*) AS CNT
        FROM {DB}.{SCHEMA}.DENOMINATION_PREDICTIONS
        GROUP BY ACTUAL, PREDICTED
        ORDER BY ACTUAL, PREDICTED
    """)
    heatmap = alt.Chart(denom_preds).mark_rect(cornerRadius=3).encode(
        x=alt.X('PREDICTED:N', title='Predicted', axis=alt.Axis(labelAngle=-45)),
        y=alt.Y('ACTUAL:N', title='Actual'),
        color=alt.Color('CNT:Q', scale=alt.Scale(scheme='blues'), legend=alt.Legend(title='Count')),
        tooltip=[
            alt.Tooltip('ACTUAL:N', title='Actual'),
            alt.Tooltip('PREDICTED:N', title='Predicted'),
            alt.Tooltip('CNT:Q', title='Count')
        ]
    ).properties(height=350, width='container')
    text_layer = alt.Chart(denom_preds).mark_text(fontSize=10, color='white').encode(
        x='PREDICTED:N',
        y='ACTUAL:N',
        text=alt.Text('CNT:Q', format='.0f'),
        opacity=alt.condition(alt.datum.CNT > 0, alt.value(1), alt.value(0))
    )
    chart = themed_chart(heatmap + text_layer)
    st.altair_chart(chart, use_container_width=True)

    st.markdown('##### 📉 Feature Importance')
    try:
        session = _get_session()
        try:
            fi = session.sql(f"CALL {DB}.{SCHEMA}.DENOMINATION_CLASSIFIER!SHOW_FEATURE_IMPORTANCE()").to_pandas()
        except Exception as e:
            if "expired" in str(e).lower() or "390114" in str(e):
                session = _reconnect()
                fi = session.sql(f"CALL {DB}.{SCHEMA}.DENOMINATION_CLASSIFIER!SHOW_FEATURE_IMPORTANCE()").to_pandas()
            else:
                raise
        fi.columns = [c.strip('"').upper() for c in fi.columns]
        if 'SCORE' not in fi.columns:
            st.warning(f"Feature importance returned unexpected columns: {list(fi.columns)}")
        else:
            fi['SCORE'] = fi['SCORE'].astype(float)
            fi = fi.sort_values('SCORE', ascending=False).head(15)
            chart = alt.Chart(fi).mark_bar(cornerRadiusTopRight=5, cornerRadiusBottomRight=5).encode(
                x=alt.X('SCORE:Q', title='Importance Score'),
                y=alt.Y('FEATURE:N', sort='-x', title='Feature'),
                color=alt.Color('SCORE:Q', scale=alt.Scale(range=[CARNIVAL_LIGHT_BLUE, CARNIVAL_BLUE]), legend=None)
            ).properties(height=380)
            chart = themed_chart(chart)
            st.altair_chart(chart, use_container_width=True)
    except Exception as e:
        st.warning(f"Could not load feature importance: {e}")

    st.markdown('##### 🤖 Live ML Predictions')
    st.caption("Select a member and run denomination preference + bet category predictions using Model Registry models.")

    members_df = run_query(f"""
        SELECT m.MEMBER_ID, m.GENDER, m.AGE, m.MEMBERSHIP_TIER, m.RISK_APPETITE, m.INCOME_BRACKET
        FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS m
        JOIN {DB}.{SCHEMA}.ML_TRAINING_DATA t ON m.MEMBER_ID = t.MEMBER_ID
        ORDER BY m.MEMBER_ID
    """)

    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        tier_filter = st.selectbox("Membership tier", ["All"] + sorted(members_df['MEMBERSHIP_TIER'].unique().tolist()))
    with fc2:
        risk_filter = st.selectbox("Risk appetite", ["All"] + sorted(members_df['RISK_APPETITE'].unique().tolist()))
    with fc3:
        income_filter = st.selectbox("Income bracket", ["All"] + sorted(members_df['INCOME_BRACKET'].unique().tolist()))

    filtered = members_df.copy()
    if tier_filter != "All":
        filtered = filtered[filtered['MEMBERSHIP_TIER'] == tier_filter]
    if risk_filter != "All":
        filtered = filtered[filtered['RISK_APPETITE'] == risk_filter]
    if income_filter != "All":
        filtered = filtered[filtered['INCOME_BRACKET'] == income_filter]

    st.caption(f"{len(filtered):,} members match filters")

    filtered['LABEL'] = filtered.apply(
        lambda r: f"{r['MEMBER_ID']} — {r['GENDER']}, Age {r['AGE']}, {r['MEMBERSHIP_TIER']}, {r['RISK_APPETITE']} risk, {r['INCOME_BRACKET']}",
        axis=1
    )
    selected_label = st.selectbox("Select a member", filtered['LABEL'].tolist())
    selected_member = int(selected_label.split(" — ")[0]) if selected_label else None

    if st.button("Run Predictions", type="primary"):
        pc1, pc2 = st.columns(2)
        with pc1:
            with st.spinner("Predicting denomination..."):
                try:
                    session = _get_session()
                    try:
                        denom_result = session.sql(
                            f"CALL {DB}.{SCHEMA}.PREDICT_DENOMINATION_V2({selected_member})"
                        ).collect()[0][0]
                    except Exception as e:
                        if "expired" in str(e).lower() or "390114" in str(e):
                            session = _reconnect()
                            denom_result = session.sql(
                                f"CALL {DB}.{SCHEMA}.PREDICT_DENOMINATION_V2({selected_member})"
                            ).collect()[0][0]
                        else:
                            raise
                    st.success(denom_result)
                except Exception as e:
                    st.error(f"Denomination prediction failed: {e}")
        with pc2:
            with st.spinner("Predicting bet category..."):
                try:
                    session = _get_session()
                    try:
                        bet_result = session.sql(
                            f"CALL {DB}.{SCHEMA}.PREDICT_BET_CATEGORY({selected_member})"
                        ).collect()[0][0]
                    except Exception as e:
                        if "expired" in str(e).lower() or "390114" in str(e):
                            session = _reconnect()
                            bet_result = session.sql(
                                f"CALL {DB}.{SCHEMA}.PREDICT_BET_CATEGORY({selected_member})"
                            ).collect()[0][0]
                        else:
                            raise
                    st.success(bet_result)
                except Exception as e:
                    st.error(f"Bet category prediction failed: {e}")

        with st.expander("View member profile", expanded=False):
            profile = run_query(f"""
                SELECT m.MEMBER_ID, m.AGE, m.GENDER, m.MEMBERSHIP_TIER, m.RISK_APPETITE,
                       m.INCOME_BRACKET, m.MARITAL_STATUS, m.TOTAL_CRUISES, m.LIFETIME_SPEND,
                       f.TOTAL_SESSIONS, f.TOTAL_SPINS, f.AVG_BET_PER_SPIN, 
                       f.PREFERRED_DENOMINATION AS ACTUAL_PREF_DENOM,
                       f.WIN_RATE_PCT
                FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS m
                JOIN {DB}.{SCHEMA}.MEMBER_SLOT_FEATURES f ON m.MEMBER_ID = f.MEMBER_ID
                WHERE m.MEMBER_ID = {selected_member}
            """)
            st.dataframe(profile, hide_index=True, use_container_width=True)

    st.markdown('##### 🎛️ Interactive Denomination Predictor')
    st.caption("Set patron attributes manually to predict optimal slot denomination via the SLOT_DENOMINATION_MODEL (RandomForest).")

    SHIP_NAMES = ['Carnival Breeze', 'Carnival Celebration', 'Carnival Horizon', 'Carnival Jubilee', 'Carnival Magic', 'Carnival Panorama', 'Carnival Vista', 'Mardi Gras']
    GAME_TYPES = ['Classic Slots', 'Bonus Slots', 'Progressive Slots', 'Video Poker']
    GENDERS = ['Female', 'Male']
    TIERS = ['Basic', 'Bronze', 'Gold', 'Platinum', 'Silver']
    RISK_LEVELS = ['Conservative', 'Moderate', 'High']
    INCOME_BRACKETS = ['Under $50K', '$50K-$75K', '$75K-$100K', '$100K-$150K', 'Over $150K']
    SHIP_MAP = {s: i for i, s in enumerate(SHIP_NAMES)}
    GAME_MAP = {g: i for i, g in enumerate(GAME_TYPES)}
    GENDER_MAP = {'Female': 0, 'Male': 1}
    TIER_MAP = {t: i for i, t in enumerate(TIERS)}
    RISK_MAP = {r: i for i, r in enumerate(RISK_LEVELS)}
    INCOME_MAP = {b: i for i, b in enumerate(INCOME_BRACKETS)}
    DENOM_CLASSES = {0: '$0.01', 1: '$0.05', 2: '$0.25', 3: '$1.00', 4: '$5.00', 5: '$10.00', 6: '$20.00', 7: '$50.00', 8: '$100.00'}

    with st.container(border=True):
        ic1, ic2, ic3, ic4 = st.columns(4)
        with ic1:
            inp_ship = st.selectbox("Ship", SHIP_NAMES, key="pred_ship")
            inp_game = st.selectbox("Game type", GAME_TYPES, key="pred_game")
            inp_month = st.slider("Month", 1, 12, 6, key="pred_month")
        with ic2:
            inp_age = st.number_input("Age", 21, 85, 45, key="pred_age")
            inp_gender = st.selectbox("Gender", GENDERS, key="pred_gender")
            inp_dow = st.slider("Day of week (0=Mon)", 0, 6, 2, key="pred_dow")
        with ic3:
            inp_tier = st.selectbox("Membership tier", TIERS, key="pred_tier")
            inp_cruises = st.number_input("Total cruises", 1, 50, 5, key="pred_cruises")
            inp_weekend = st.checkbox("Weekend", key="pred_weekend")
        with ic4:
            inp_risk = st.selectbox("Risk appetite", RISK_LEVELS, key="pred_risk")
            inp_income = st.selectbox("Income bracket", INCOME_BRACKETS, key="pred_income")

    if st.button("Predict Denomination", type="primary", key="manual_predict"):
        ship_enc = SHIP_MAP[inp_ship]
        game_enc = GAME_MAP[inp_game]
        gender_enc = GENDER_MAP[inp_gender]
        tier_enc = TIER_MAP[inp_tier]
        risk_enc = RISK_MAP[inp_risk]
        income_enc = INCOME_MAP[inp_income]
        weekend_val = 1 if inp_weekend else 0

        predict_sql = f"""
            SELECT MODEL(CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_DENOMINATION_MODEL, V1)!PREDICT(
                {ship_enc}, {inp_month}, {inp_dow}, {weekend_val},
                {game_enc}, {inp_age}, {gender_enc}, {tier_enc},
                {inp_cruises}, {risk_enc}, {income_enc}
            ):output_feature_0::INT AS PRED_CLASS
        """
        proba_sql = f"""
            SELECT MODEL(CARNIVAL_CASINO.SLOT_ANALYTICS.SLOT_DENOMINATION_MODEL, V1)!PREDICT_PROBA(
                {ship_enc}, {inp_month}, {inp_dow}, {weekend_val},
                {game_enc}, {inp_age}, {gender_enc}, {tier_enc},
                {inp_cruises}, {risk_enc}, {income_enc}
            ) AS PROBS
        """
        with st.spinner("Running model inference..."):
            try:
                session = _get_session()
                pred_row = session.sql(predict_sql).collect()[0]
                pred_class = int(pred_row['PRED_CLASS'])
                denom_label = DENOM_CLASSES.get(pred_class, 'Unknown')

                st.markdown(f'''
                <div class="prediction-result">
                    <p class="prediction-label">Recommended Denomination</p>
                    <p class="prediction-value">{denom_label}</p>
                </div>
                ''', unsafe_allow_html=True)
                st.markdown("")

                proba_row = session.sql(proba_sql).collect()[0]
                probs_obj = proba_row['PROBS']
                if isinstance(probs_obj, str):
                    probs_obj = json.loads(probs_obj)
                prob_data = pd.DataFrame({
                    'Denomination': list(DENOM_CLASSES.values()),
                    'Probability': [float(probs_obj.get(f'output_feature_{i}', 0)) for i in range(9)]
                })
                bars = alt.Chart(prob_data).mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5).encode(
                    x=alt.X('Denomination:N', sort=None, title='Denomination'),
                    y=alt.Y('Probability:Q', title='Probability', axis=alt.Axis(format='.0%')),
                    color=alt.condition(
                        alt.datum.Denomination == denom_label,
                        alt.value(CARNIVAL_GOLD),
                        alt.value(CARNIVAL_BLUE)
                    ),
                    tooltip=[
                        alt.Tooltip('Denomination:N'),
                        alt.Tooltip('Probability:Q', format='.2%')
                    ]
                ).properties(height=300)
                rule = alt.Chart(prob_data).mark_rule(color=CARNIVAL_RED, strokeWidth=1.5, strokeDash=[4,4]).encode(
                    y=alt.Y('mean(Probability):Q')
                )
                chart = themed_chart(bars + rule)
                st.altair_chart(chart, use_container_width=True)
            except Exception as e:
                st.error(f"Prediction failed: {e}")

    st.divider()
    st.markdown('##### 🎰 Bank Denomination Allocator')
    st.caption("Predict optimal denomination mix for a bank of wired machines on a specific ship, based on historical play patterns and passenger demographics.")

    BANK_SHIPS = run_query(f"SELECT DISTINCT SHIP_NAME FROM {DB}.{SCHEMA}.SHIP_BANK_DEMAND ORDER BY SHIP_NAME")
    BANK_DENOM_LABELS = ['$0.01', '$0.05', '$0.25', '$1.00', '$5.00', '$10.00', '$20.00', '$50.00', '$100.00']

    with st.container(border=True):
        bc1, bc2, bc3 = st.columns([2, 1, 1])
        with bc1:
            bank_ship = st.selectbox("Ship", BANK_SHIPS['SHIP_NAME'].tolist(), key="bank_ship")
        with bc2:
            bank_size = st.number_input("Machines in bank", min_value=4, max_value=100, value=24, step=2, key="bank_size")
        with bc3:
            st.markdown("<br>", unsafe_allow_html=True)
            bank_predict_btn = st.button("Allocate Bank", type="primary", key="bank_predict")

    if bank_predict_btn:
        try:
            demand = run_query(f"""
                SELECT DENOMINATION, DEMAND_PCT
                FROM {DB}.{SCHEMA}.SHIP_BANK_DEMAND
                WHERE SHIP_NAME = '{bank_ship}'
                ORDER BY DENOMINATION
            """)

            pcts = demand['DEMAND_PCT'].values.astype(float)
            pcts = np.clip(pcts, 0, 100)
            pcts = pcts / pcts.sum() * 100

            raw_alloc = pcts * bank_size / 100.0
            allocated = np.floor(raw_alloc).astype(int)
            remainder = raw_alloc - allocated
            shortfall = bank_size - allocated.sum()
            if shortfall > 0:
                indices = np.argsort(-remainder)
                for idx in range(int(shortfall)):
                    allocated[indices[idx]] += 1

            st.session_state['bank_result'] = {
                'ship': bank_ship, 'bank_size': bank_size,
                'pcts': pcts.tolist(), 'allocated': allocated.tolist()
            }
        except Exception as e:
            st.error(f"Bank allocation failed: {e}")

    if 'bank_result' in st.session_state:
        br = st.session_state['bank_result']
        pcts = np.array(br['pcts'])
        allocated = np.array(br['allocated'])

        st.markdown(f'''
        <div class="prediction-result">
            <p style="color:rgba(255,255,255,0.7);font-size:0.85rem;text-transform:uppercase;letter-spacing:0.1em;margin:0;">
                Bank Allocation — {br['ship']}
            </p>
            <p style="color:{CARNIVAL_GOLD};font-size:1.4rem;font-weight:700;margin:4px 0 0 0;">
                {br['bank_size']} Machines Configured
            </p>
        </div>
        ''', unsafe_allow_html=True)

        alloc_data = pd.DataFrame({
            'Denomination': BANK_DENOM_LABELS,
            'Machines': allocated,
            'Demand %': [round(p, 1) for p in pcts]
        })
        alloc_data = alloc_data[alloc_data['Machines'] > 0]

        col_chart, col_table = st.columns([3, 1])

        with col_chart:
            bars = alt.Chart(alloc_data).mark_bar(
                cornerRadiusTopLeft=6, cornerRadiusTopRight=6
            ).encode(
                x=alt.X('Denomination:N', sort=BANK_DENOM_LABELS, title='Denomination'),
                y=alt.Y('Machines:Q', title='Machines Allocated', axis=alt.Axis(tickMinStep=1)),
                color=alt.Color('Demand %:Q', scale=alt.Scale(
                    range=[CARNIVAL_BLUE, CARNIVAL_GOLD]
                ), legend=alt.Legend(title='Demand %')),
                tooltip=[
                    alt.Tooltip('Denomination:N'),
                    alt.Tooltip('Machines:Q', title='Machines'),
                    alt.Tooltip('Demand %:Q', format='.1f')
                ]
            ).properties(height=350)

            labels = alt.Chart(alloc_data).mark_text(
                dy=-10, color='white', fontSize=13, fontWeight='bold'
            ).encode(
                x=alt.X('Denomination:N', sort=BANK_DENOM_LABELS),
                y=alt.Y('Machines:Q'),
                text='Machines:Q'
            )

            chart = themed_chart(bars + labels)
            st.altair_chart(chart, use_container_width=True)

        with col_table:
            st.markdown("**Allocation Summary**")
            for _, row in alloc_data.iterrows():
                st.markdown(f'''
                <div style="display:flex;justify-content:space-between;padding:6px 10px;
                            border-bottom:1px solid rgba(255,255,255,0.08);">
                    <span style="font-weight:500;">{row['Denomination']}</span>
                    <span><span class="stat-pill">{int(row['Machines'])} machines</span></span>
                </div>
                ''', unsafe_allow_html=True)
            st.markdown(f'''
            <div style="display:flex;justify-content:space-between;padding:10px;
                        background:rgba(245,158,11,0.1);border-radius:8px;margin-top:8px;">
                <span style="font-weight:700;">Total</span>
                <span style="font-weight:700;color:{CARNIVAL_GOLD};">{int(alloc_data['Machines'].sum())} machines</span>
            </div>
            ''', unsafe_allow_html=True)

with tab4:
    st.markdown('##### 📋 Casino Policies & Information')
    policies = run_query(f"SELECT * FROM {DB}.{SCHEMA}.CASINO_POLICIES ORDER BY CATEGORY, POLICY_ID")
    categories = policies['CATEGORY'].unique()

    for cat in categories:
        st.markdown(f"#### {cat}")
        cat_policies = policies[policies['CATEGORY'] == cat]
        for _, pol in cat_policies.iterrows():
            with st.expander(pol['TITLE']):
                st.markdown(pol['CONTENT'])
                st.caption(f"Last updated: {pol['LAST_UPDATED']}")
