import reflex as rx
import os
import snowflake.connector


DB = "CARNIVAL_CASINO"
SCHEMA = "SLOT_ANALYTICS"
CARNIVAL_RED = "#B61B38"
CARNIVAL_BLUE = "#014E8F"
GRADIENT_START = "#014E8F"
GRADIENT_END = "#B61B38"


def _get_conn():
    conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "demo")
    return snowflake.connector.connect(connection_name=conn_name)


def _run(sql: str) -> list[dict]:
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


class AppState(rx.State):
    active_tab: str = "members"

    total_members: str = "..."
    total_sessions: str = "..."
    total_wagered: str = "..."
    avg_sessions: str = "..."
    tiers_data: list[dict] = []
    age_data: list[dict] = []
    income_data: list[dict] = []
    states_data: list[dict] = []

    ships: list[str] = []
    ship_options: list[str] = ["All Ships"]
    selected_ship: str = "All Ships"
    unique_players: str = "..."
    avg_bet: str = "..."
    avg_duration: str = "..."
    win_rate: str = "..."
    denom_data: list[dict] = []
    games_data: list[dict] = []
    tod_data: list[dict] = []
    gtype_data: list[dict] = []
    monthly_data: list[dict] = []

    models_data: list[dict] = []
    confusion_data: list[dict] = []
    features_data: list[dict] = []

    policies_data: list[dict] = []

    loading: bool = True

    @rx.event
    def load_all(self):
        self.loading = True
        self._load_members()
        self._load_slots()
        self._load_ml()
        self._load_policies()
        self.loading = False

    @rx.event
    def set_tab(self, tab: str):
        self.active_tab = tab

    @rx.event
    def set_ship(self, ship: str):
        self.selected_ship = ship
        self._load_slots()

    def _load_members(self):
        kpis = _run(f"""
            SELECT 'total_members' AS METRIC, COUNT(*) AS VALUE FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS
            UNION ALL SELECT 'total_sessions', COUNT(*) FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY
            UNION ALL SELECT 'total_wagered', ROUND(SUM(TOTAL_WAGERED),0) FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY
            UNION ALL SELECT 'avg_sessions', ROUND(AVG(TOTAL_SESSIONS),1) FROM {DB}.{SCHEMA}.MEMBER_SLOT_FEATURES
        """)
        kpi_map = {r["METRIC"]: r["VALUE"] for r in kpis}
        self.total_members = f"{int(kpi_map.get('total_members', 0)):,}"
        self.total_sessions = f"{int(kpi_map.get('total_sessions', 0)):,}"
        self.total_wagered = f"${int(kpi_map.get('total_wagered', 0)):,}"
        self.avg_sessions = str(kpi_map.get("avg_sessions", 0))

        tiers_raw = _run(f"""
            SELECT MEMBERSHIP_TIER as name, COUNT(*) AS value
            FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS GROUP BY MEMBERSHIP_TIER ORDER BY value DESC
        """)
        self.tiers_data = [{"name": r["NAME"], "value": int(r["VALUE"])} for r in tiers_raw]
        ages_raw = _run(f"""
            SELECT CONCAT(FLOOR(AGE/10)*10, 's') AS AGE_GROUP, GENDER, COUNT(*) AS CNT
            FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS GROUP BY 1, GENDER ORDER BY 1
        """)
        groups = sorted(set(r["AGE_GROUP"] for r in ages_raw))
        self.age_data = []
        for g in groups:
            m = next((r["CNT"] for r in ages_raw if r["AGE_GROUP"] == g and r["GENDER"] == "M"), 0)
            f = next((r["CNT"] for r in ages_raw if r["AGE_GROUP"] == g and r["GENDER"] == "F"), 0)
            self.age_data.append({"name": g, "Male": int(m), "Female": int(f)})

        self.income_data = [
            {"name": r["INCOME_BRACKET"], "value": int(r["CNT"])}
            for r in _run(f"""
                SELECT INCOME_BRACKET, COUNT(*) AS CNT FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS
                GROUP BY INCOME_BRACKET ORDER BY CNT DESC
            """)
        ]
        self.states_data = [
            {"name": r["HOME_STATE"], "value": int(r["MEMBERS"])}
            for r in _run(f"""
                SELECT HOME_STATE, COUNT(*) AS MEMBERS FROM {DB}.{SCHEMA}.MEMBER_DEMOGRAPHICS
                GROUP BY HOME_STATE ORDER BY MEMBERS DESC LIMIT 10
            """)
        ]

    def _load_slots(self):
        if not self.ships:
            self.ships = [
                r["SHIP_NAME"]
                for r in _run(f"SELECT DISTINCT SHIP_NAME FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY ORDER BY SHIP_NAME")
            ]
            self.ship_options = ["All Ships"] + self.ships
        where = f"WHERE SHIP_NAME = '{self.selected_ship}'" if self.selected_ship != "All Ships" else ""
        stats = _run(f"""
            SELECT COUNT(DISTINCT MEMBER_ID) AS UNIQUE_PLAYERS, ROUND(AVG(BET_PER_SPIN),2) AS AVG_BET,
                ROUND(AVG(SESSION_DURATION_MINS),1) AS AVG_DURATION,
                ROUND(SUM(TOTAL_WON)/NULLIF(SUM(TOTAL_WAGERED),0)*100,1) AS WIN_RATE
            FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where}
        """)
        if stats:
            s = stats[0]
            self.unique_players = f"{int(s['UNIQUE_PLAYERS']):,}"
            self.avg_bet = f"${s['AVG_BET']}"
            self.avg_duration = str(s["AVG_DURATION"])
            self.win_rate = f"{s['WIN_RATE']}%"
        self.denom_data = [
            {"name": f"${r['DENOMINATION']}", "value": int(r["TOTAL_WAGERED"])}
            for r in _run(f"""
                SELECT DENOMINATION, ROUND(SUM(TOTAL_WAGERED),0) AS TOTAL_WAGERED
                FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where} GROUP BY DENOMINATION ORDER BY DENOMINATION
            """)
        ]
        self.games_data = [
            {"name": r["GAME_NAME"], "value": int(r["SESSIONS"])}
            for r in _run(f"""
                SELECT GAME_NAME, COUNT(*) AS SESSIONS FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where}
                GROUP BY GAME_NAME ORDER BY SESSIONS DESC LIMIT 10
            """)
        ]
        self.tod_data = _run(f"""
            SELECT TIME_OF_DAY, ROUND(AVG(BET_PER_SPIN),2) AS AVG_BET, COUNT(*) AS SESSIONS
            FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where} GROUP BY TIME_OF_DAY ORDER BY SESSIONS DESC
        """)
        self.gtype_data = _run(f"""
            SELECT GAME_TYPE, COUNT(*) AS SESSIONS, ROUND(AVG(BET_PER_SPIN),2) AS AVG_BET
            FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where} GROUP BY GAME_TYPE ORDER BY SESSIONS DESC
        """)
        self.monthly_data = [
            {"name": r["MONTH"], "value": int(r["TOTAL_WAGERED"])}
            for r in _run(f"""
                SELECT TO_VARCHAR(DATE_TRUNC('MONTH', PLAY_DATE), 'YYYY-MM') AS MONTH,
                    ROUND(SUM(TOTAL_WAGERED),0) AS TOTAL_WAGERED
                FROM {DB}.{SCHEMA}.SLOT_PLAY_HISTORY {where} GROUP BY 1 ORDER BY 1
            """)
        ]

    def _load_ml(self):
        self.models_data = _run(f"SELECT * FROM {DB}.{SCHEMA}.MODEL_EVALUATION_SUMMARY")
        raw_conf = _run(f"""
            SELECT PREFERRED_DENOMINATION AS ACTUAL, PREDICTION:class::STRING AS PREDICTED, COUNT(*) AS CNT
            FROM {DB}.{SCHEMA}.DENOMINATION_PREDICTIONS GROUP BY ACTUAL, PREDICTED ORDER BY ACTUAL, PREDICTED
        """)
        self.confusion_data = [
            {"ACTUAL": str(r["ACTUAL"]), "PREDICTED": str(r["PREDICTED"]), "CNT": int(r["CNT"])}
            for r in raw_conf
        ]
        try:
            raw_fi = _run(f"CALL {DB}.{SCHEMA}.DENOMINATION_CLASSIFIER!SHOW_FEATURE_IMPORTANCE()")
            cleaned = []
            for r in raw_fi:
                feat = r.get("FEATURE") or r.get('"FEATURE"', "")
                score = r.get("SCORE") or r.get('"SCORE"', 0)
                cleaned.append({"name": str(feat).strip('"'), "value": round(float(str(score).strip('"')), 4)})
            cleaned.sort(key=lambda x: x["value"], reverse=True)
            self.features_data = cleaned[:15]
        except Exception:
            self.features_data = []

    def _load_policies(self):
        self.policies_data = _run(f"SELECT * FROM {DB}.{SCHEMA}.CASINO_POLICIES ORDER BY CATEGORY, POLICY_ID")


CARD_STYLE = {
    "background": "rgba(30, 32, 40, 0.8)",
    "backdrop_filter": "blur(16px)",
    "border": "1px solid rgba(255, 255, 255, 0.08)",
    "border_radius": "16px",
    "box_shadow": "0 4px 24px rgba(0, 0, 0, 0.3), 0 1px 3px rgba(0, 0, 0, 0.2)",
    "transition": "all 0.2s ease",
    "_hover": {"box_shadow": "0 8px 32px rgba(0, 0, 0, 0.4), 0 2px 8px rgba(0, 0, 0, 0.3)", "border": "1px solid rgba(255, 255, 255, 0.12)"},
}

STAT_COLORS = {
    "blue": {"bg": "linear-gradient(135deg, rgba(1, 78, 143, 0.3), rgba(1, 78, 143, 0.15))", "icon_bg": "#3B82F6", "text": "#60A5FA"},
    "red": {"bg": "linear-gradient(135deg, rgba(182, 27, 56, 0.3), rgba(182, 27, 56, 0.15))", "icon_bg": "#EF4444", "text": "#F87171"},
    "emerald": {"bg": "linear-gradient(135deg, rgba(5, 150, 105, 0.3), rgba(5, 150, 105, 0.15))", "icon_bg": "#10B981", "text": "#34D399"},
    "amber": {"bg": "linear-gradient(135deg, rgba(217, 119, 6, 0.3), rgba(217, 119, 6, 0.15))", "icon_bg": "#F59E0B", "text": "#FBBF24"},
}


def stat_card(icon: str, label: str, value: rx.Var, color: str = "blue") -> rx.Component:
    palette = STAT_COLORS.get(color, STAT_COLORS["blue"])
    return rx.box(
        rx.vstack(
            rx.hstack(
                rx.box(
                    rx.icon(icon, size=18, color="white"),
                    padding="10px",
                    border_radius="12px",
                    background=palette["icon_bg"],
                ),
                rx.spacer(),
                spacing="2",
                align="center",
                width="100%",
            ),
            rx.text(label, size="2", weight="medium", color="rgba(255, 255, 255, 0.6)"),
            rx.heading(value, size="7", weight="bold", color=palette["text"], letter_spacing="-0.02em"),
            spacing="2",
            padding="20px",
            width="100%",
        ),
        background=palette["bg"],
        border_radius="16px",
        border="1px solid rgba(255, 255, 255, 0.1)",
        box_shadow="0 4px 16px rgba(0, 0, 0, 0.2)",
        transition="all 0.2s ease",
        _hover={"transform": "translateY(-2px)", "box_shadow": "0 12px 32px rgba(0, 0, 0, 0.3)", "border": "1px solid rgba(255, 255, 255, 0.15)"},
        width="100%",
    )


def chart_card(title: str, chart: rx.Component) -> rx.Component:
    return rx.box(
        rx.vstack(
            rx.heading(title, size="4", weight="bold", color="rgba(255, 255, 255, 0.9)"),
            chart,
            spacing="4",
            padding="24px",
            width="100%",
        ),
        **CARD_STYLE,
        width="100%",
    )


def section_heading(title: str, subtitle: str = "") -> rx.Component:
    items = [rx.heading(title, size="6", weight="bold", color="rgba(255, 255, 255, 0.95)", letter_spacing="-0.02em")]
    if subtitle:
        items.append(rx.text(subtitle, size="2", color="rgba(255, 255, 255, 0.5)"))
    return rx.vstack(*items, spacing="1")


def members_tab() -> rx.Component:
    return rx.vstack(
        section_heading("Member Overview", "Key metrics and demographics for casino members"),
        rx.grid(
            stat_card("users", "Total Members", AppState.total_members, "blue"),
            stat_card("play", "Total Play Sessions", AppState.total_sessions, "emerald"),
            stat_card("dollar-sign", "Total Wagered", AppState.total_wagered, "red"),
            stat_card("activity", "Avg Sessions/Member", AppState.avg_sessions, "amber"),
            columns=rx.breakpoints(initial="1", sm="2", lg="4"),
            spacing="4",
            width="100%",
        ),
        rx.grid(
            chart_card(
                "Membership Tier Distribution",
                rx.recharts.bar_chart(
                    rx.recharts.bar(data_key="value", fill="#60A5FA", radius=[8, 8, 0, 0]),
                    rx.recharts.x_axis(data_key="name", tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.y_axis(tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.graphing_tooltip(content_style={"backgroundColor": "rgba(30,32,40,0.95)", "border": "1px solid rgba(255,255,255,0.1)", "borderRadius": "8px", "color": "#fff"}),
                    rx.recharts.cartesian_grid(stroke_dasharray="3 3", vertical=False, stroke="rgba(255,255,255,0.08)"),
                    data=AppState.tiers_data,
                    width="100%",
                    height=300,
                ),
            ),
            chart_card(
                "Age Distribution by Gender",
                rx.recharts.bar_chart(
                    rx.recharts.bar(data_key="Male", fill="#60A5FA", radius=[8, 8, 0, 0]),
                    rx.recharts.bar(data_key="Female", fill="#F87171", radius=[8, 8, 0, 0]),
                    rx.recharts.x_axis(data_key="name", tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.y_axis(tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.legend(wrapperStyle={"color": "rgba(255,255,255,0.8)"}),
                    rx.recharts.graphing_tooltip(content_style={"backgroundColor": "rgba(30,32,40,0.95)", "border": "1px solid rgba(255,255,255,0.1)", "borderRadius": "8px", "color": "#fff"}),
                    rx.recharts.cartesian_grid(stroke_dasharray="3 3", vertical=False, stroke="rgba(255,255,255,0.08)"),
                    data=AppState.age_data,
                    width="100%",
                    height=300,
                ),
            ),
            chart_card(
                "Income Bracket Distribution",
                rx.recharts.bar_chart(
                    rx.recharts.bar(data_key="value", fill="#60A5FA", radius=[8, 8, 0, 0]),
                    rx.recharts.x_axis(data_key="name", tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.y_axis(tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.graphing_tooltip(content_style={"backgroundColor": "rgba(30,32,40,0.95)", "border": "1px solid rgba(255,255,255,0.1)", "borderRadius": "8px", "color": "#fff"}),
                    rx.recharts.cartesian_grid(stroke_dasharray="3 3", vertical=False, stroke="rgba(255,255,255,0.08)"),
                    data=AppState.income_data,
                    width="100%",
                    height=280,
                ),
            ),
            chart_card(
                "Top 10 Home States",
                rx.recharts.bar_chart(
                    rx.recharts.bar(data_key="value", fill="#F87171", radius=[8, 8, 0, 0]),
                    rx.recharts.x_axis(data_key="name", tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.y_axis(tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.graphing_tooltip(content_style={"backgroundColor": "rgba(30,32,40,0.95)", "border": "1px solid rgba(255,255,255,0.1)", "borderRadius": "8px", "color": "#fff"}),
                    rx.recharts.cartesian_grid(stroke_dasharray="3 3", vertical=False, stroke="rgba(255,255,255,0.08)"),
                    data=AppState.states_data,
                    width="100%",
                    height=280,
                ),
            ),
            columns=rx.breakpoints(initial="1", lg="2"),
            spacing="4",
            width="100%",
        ),
        spacing="6",
        width="100%",
        padding_top="8px",
    )


def show_tod_row(row: dict) -> rx.Component:
    return rx.table.row(
        rx.table.cell(rx.badge(row["TIME_OF_DAY"], variant="soft", size="1")),
        rx.table.cell(rx.text(f"${row['AVG_BET']}", align="right", weight="medium")),
        rx.table.cell(rx.text(f"{row['SESSIONS']}", align="right", color=rx.color("slate", 9))),
    )


def show_gtype_row(row: dict) -> rx.Component:
    return rx.table.row(
        rx.table.cell(rx.badge(row["GAME_TYPE"], variant="soft", size="1")),
        rx.table.cell(rx.text(f"{row['SESSIONS']}", align="right", weight="medium")),
        rx.table.cell(rx.text(f"${row['AVG_BET']}", align="right", color=rx.color("slate", 9))),
    )


def slots_tab() -> rx.Component:
    return rx.vstack(
        rx.hstack(
            section_heading("Slot Play Analysis", "Performance metrics filtered by ship"),
            rx.spacer(),
            rx.box(
                rx.select(
                    AppState.ship_options,
                    value=AppState.selected_ship,
                    on_change=AppState.set_ship,
                    width="220px",
                    size="3",
                ),
                padding="4px",
                border_radius="12px",
                background="rgba(30,32,40,0.8)",
                border="1px solid rgba(255,255,255,0.1)",
            ),
            align="end",
            width="100%",
            flex_wrap="wrap",
            gap="4",
        ),
        rx.grid(
            stat_card("users", "Unique Players", AppState.unique_players, "blue"),
            stat_card("coins", "Avg Bet/Spin", AppState.avg_bet, "emerald"),
            stat_card("clock", "Avg Session (min)", AppState.avg_duration, "amber"),
            stat_card("percent", "Win Rate", AppState.win_rate, "red"),
            columns=rx.breakpoints(initial="1", sm="2", lg="4"),
            spacing="4",
            width="100%",
        ),
        rx.grid(
            chart_card(
                "Revenue by Denomination",
                rx.recharts.bar_chart(
                    rx.recharts.bar(data_key="value", fill="#60A5FA", radius=[8, 8, 0, 0]),
                    rx.recharts.x_axis(data_key="name", tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.y_axis(tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.graphing_tooltip(content_style={"backgroundColor": "rgba(30,32,40,0.95)", "border": "1px solid rgba(255,255,255,0.1)", "borderRadius": "8px", "color": "#fff"}),
                    rx.recharts.cartesian_grid(stroke_dasharray="3 3", vertical=False, stroke="rgba(255,255,255,0.08)"),
                    data=AppState.denom_data,
                    width="100%",
                    height=300,
                ),
            ),
            chart_card(
                "Top Games by Sessions",
                rx.recharts.bar_chart(
                    rx.recharts.bar(data_key="value", fill="#F87171", radius=[0, 8, 8, 0]),
                    rx.recharts.x_axis(type_="number", tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.y_axis(data_key="name", type_="category", width=130, tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.graphing_tooltip(content_style={"backgroundColor": "rgba(30,32,40,0.95)", "border": "1px solid rgba(255,255,255,0.1)", "borderRadius": "8px", "color": "#fff"}),
                    rx.recharts.cartesian_grid(stroke_dasharray="3 3", horizontal=False, stroke="rgba(255,255,255,0.08)"),
                    data=AppState.games_data,
                    layout="vertical",
                    width="100%",
                    height=300,
                ),
            ),
            rx.box(
                rx.vstack(
                    rx.heading("Play Patterns by Time of Day", size="4", weight="bold", color="rgba(255,255,255,0.9)"),
                    rx.table.root(
                        rx.table.header(
                            rx.table.row(
                                rx.table.column_header_cell("Time of Day"),
                                rx.table.column_header_cell("Avg Bet"),
                                rx.table.column_header_cell("Sessions"),
                            ),
                        ),
                        rx.table.body(
                            rx.foreach(AppState.tod_data, show_tod_row),
                        ),
                        variant="surface",
                        size="2",
                        width="100%",
                    ),
                    spacing="4",
                    padding="24px",
                    width="100%",
                ),
                **CARD_STYLE,
                width="100%",
            ),
            rx.box(
                rx.vstack(
                    rx.heading("Game Type Distribution", size="4", weight="bold", color="rgba(255,255,255,0.9)"),
                    rx.table.root(
                        rx.table.header(
                            rx.table.row(
                                rx.table.column_header_cell("Game Type"),
                                rx.table.column_header_cell("Sessions"),
                                rx.table.column_header_cell("Avg Bet"),
                            ),
                        ),
                        rx.table.body(
                            rx.foreach(AppState.gtype_data, show_gtype_row),
                        ),
                        variant="surface",
                        size="2",
                        width="100%",
                    ),
                    spacing="4",
                    padding="24px",
                    width="100%",
                ),
                **CARD_STYLE,
                width="100%",
            ),
            columns=rx.breakpoints(initial="1", lg="2"),
            spacing="4",
            width="100%",
        ),
        chart_card(
            "Monthly Wagering Trend",
            rx.recharts.area_chart(
                rx.recharts.area(
                    data_key="value",
                    stroke="#60A5FA",
                    fill="#60A5FA",
                    fill_opacity=0.2,
                    stroke_width=2,
                    type_="monotone",
                ),
                rx.recharts.x_axis(data_key="name", tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                rx.recharts.y_axis(tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                rx.recharts.graphing_tooltip(content_style={"backgroundColor": "rgba(30,32,40,0.95)", "border": "1px solid rgba(255,255,255,0.1)", "borderRadius": "8px", "color": "#fff"}),
                rx.recharts.cartesian_grid(stroke_dasharray="3 3", vertical=False, stroke="rgba(255,255,255,0.08)"),
                data=AppState.monthly_data,
                width="100%",
                height=280,
            ),
        ),
        spacing="6",
        width="100%",
        padding_top="8px",
    )


def show_model(model: dict) -> rx.Component:
    return rx.box(
        rx.vstack(
            rx.hstack(
                rx.box(
                    rx.icon("brain-circuit", size=20, color="white"),
                    padding="10px",
                    border_radius="12px",
                    background=f"linear-gradient(135deg, {CARNIVAL_BLUE}, {CARNIVAL_RED})",
                ),
                rx.vstack(
                    rx.text(model["MODEL_NAME"], weight="bold", size="4", color="rgba(255,255,255,0.95)"),
                    rx.text(model["MODEL_TYPE"], size="2", color="rgba(255,255,255,0.5)"),
                    spacing="0",
                ),
                rx.spacer(),
                rx.badge(rx.text(f"Target: {model['TARGET']}"), variant="surface", size="2", radius="large"),
                align="center",
                width="100%",
            ),
            rx.grid(
                rx.box(
                    rx.vstack(
                        rx.text("Model Accuracy", size="2", color="rgba(255,255,255,0.5)"),
                        rx.heading(rx.text(f"{model['MODEL_ACCURACY_PCT']}%"), size="6", color="#34D399"),
                        spacing="1",
                    ),
                    padding="16px",
                    border_radius="12px",
                    background="linear-gradient(135deg, rgba(5,150,105,0.2), rgba(5,150,105,0.1))",
                    border="1px solid rgba(52,211,153,0.2)",
                ),
                rx.box(
                    rx.vstack(
                        rx.text("Baseline Accuracy", size="2", color="rgba(255,255,255,0.5)"),
                        rx.heading(rx.text(f"{model['BASELINE_ACCURACY_PCT']}%"), size="6", color="rgba(255,255,255,0.7)"),
                        spacing="1",
                    ),
                    padding="16px",
                    border_radius="12px",
                    background="rgba(255,255,255,0.05)",
                    border="1px solid rgba(255,255,255,0.08)",
                ),
                columns="2",
                spacing="3",
                width="100%",
            ),
            rx.text(model["NOTES"], size="2", color="rgba(255,255,255,0.5)", line_height="1.6"),
            spacing="4",
            padding="24px",
            width="100%",
        ),
        **CARD_STYLE,
        width="100%",
    )


def show_confusion_row(row: dict) -> rx.Component:
    return rx.table.row(
        rx.table.cell(rx.badge(f"${row['ACTUAL']}", variant="soft", color_scheme="blue")),
        rx.table.cell(rx.badge(f"${row['PREDICTED']}", variant="soft", color_scheme="crimson")),
        rx.table.cell(rx.text(str(row["CNT"]), align="right", weight="bold")),
    )


def ml_tab() -> rx.Component:
    return rx.vstack(
        section_heading("ML Model Performance", "Evaluation metrics and feature analysis for predictive models"),
        rx.foreach(AppState.models_data, show_model),
        rx.grid(
            rx.box(
                rx.vstack(
                    rx.heading("Confusion Matrix", size="4", weight="bold", color="rgba(255,255,255,0.9)"),
                    rx.text("Denomination Prediction Results", size="2", color="rgba(255,255,255,0.5)"),
                    rx.table.root(
                        rx.table.header(
                            rx.table.row(
                                rx.table.column_header_cell("Actual"),
                                rx.table.column_header_cell("Predicted"),
                                rx.table.column_header_cell("Count"),
                            ),
                        ),
                        rx.table.body(
                            rx.foreach(AppState.confusion_data, show_confusion_row),
                        ),
                        variant="surface",
                        size="2",
                        width="100%",
                    ),
                    spacing="3",
                    padding="24px",
                    width="100%",
                ),
                **CARD_STYLE,
                width="100%",
            ),
            chart_card(
                "Feature Importance",
                rx.recharts.bar_chart(
                    rx.recharts.bar(data_key="value", fill="#FBBF24", radius=[0, 8, 8, 0]),
                    rx.recharts.x_axis(type_="number", tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.y_axis(data_key="name", type_="category", width=160, tick_line=False, axis_line=False, stroke="rgba(255,255,255,0.6)"),
                    rx.recharts.graphing_tooltip(content_style={"backgroundColor": "rgba(30,32,40,0.95)", "border": "1px solid rgba(255,255,255,0.1)", "borderRadius": "8px", "color": "#fff"}),
                    rx.recharts.cartesian_grid(stroke_dasharray="3 3", horizontal=False, stroke="rgba(255,255,255,0.08)"),
                    data=AppState.features_data,
                    layout="vertical",
                    width="100%",
                    height=420,
                ),
            ),
            columns=rx.breakpoints(initial="1", lg="2"),
            spacing="4",
            width="100%",
        ),
        spacing="6",
        width="100%",
        padding_top="8px",
    )


def show_policy(policy: dict) -> rx.Component:
    return rx.accordion.item(
        header=rx.hstack(
            rx.text(policy["TITLE"], weight="medium", size="3"),
            rx.spacer(),
            rx.badge(
                rx.text(f"Updated: {policy['LAST_UPDATED']}"),
                variant="surface",
                size="1",
                radius="large",
                color_scheme="blue",
            ),
            width="100%",
            align="center",
        ),
        content=rx.box(
            rx.text(policy["CONTENT"], size="2", color="rgba(255,255,255,0.6)", white_space="pre-wrap", line_height="1.7"),
            padding="8px",
        ),
        value=str(policy["POLICY_ID"]),
    )


def policies_tab() -> rx.Component:
    return rx.vstack(
        section_heading("Casino Policies & Information", "Guidelines, rules, and compliance documentation"),
        rx.box(
            rx.accordion.root(
                rx.foreach(AppState.policies_data, show_policy),
                type="multiple",
                variant="surface",
                width="100%",
            ),
            **CARD_STYLE,
            width="100%",
            overflow="hidden",
        ),
        spacing="6",
        width="100%",
        padding_top="8px",
    )


def header() -> rx.Component:
    return rx.box(
        rx.hstack(
            rx.hstack(
                rx.box(
                    rx.icon("ship", size=24, color="white"),
                    padding="10px",
                    border_radius="12px",
                    background=f"linear-gradient(135deg, {CARNIVAL_BLUE}, {CARNIVAL_RED})",
                ),
                rx.vstack(
                    rx.heading("Carnival", size="6", weight="bold", color="white", style={"font_style": "italic"}),
                    rx.text("CASINO SLOT ANALYTICS", size="1", weight="bold", color="rgba(255,255,255,0.7)", letter_spacing="0.15em"),
                    spacing="0",
                ),
                spacing="3",
                align="center",
            ),
            rx.spacer(),
            rx.badge("Live", variant="solid", color_scheme="green", size="1", radius="full"),
            padding_x="32px",
            padding_y="20px",
            align="center",
            width="100%",
        ),
        background=f"linear-gradient(135deg, {CARNIVAL_BLUE} 0%, #01396B 50%, {CARNIVAL_RED} 100%)",
        width="100%",
        position="sticky",
        top="0",
        z_index="50",
        box_shadow="0 4px 24px rgba(0, 0, 0, 0.12)",
    )


def index() -> rx.Component:
    return rx.box(
        header(),
        rx.box(
            rx.tabs.root(
                rx.box(
                    rx.tabs.list(
                        rx.tabs.trigger(
                            rx.hstack(rx.icon("users", size=16), rx.text("Member Overview"), spacing="2", align="center"),
                            value="members",
                        ),
                        rx.tabs.trigger(
                            rx.hstack(rx.icon("dice-5", size=16), rx.text("Slot Analytics"), spacing="2", align="center"),
                            value="slots",
                        ),
                        rx.tabs.trigger(
                            rx.hstack(rx.icon("brain-circuit", size=16), rx.text("ML Models"), spacing="2", align="center"),
                            value="ml",
                        ),
                        rx.tabs.trigger(
                            rx.hstack(rx.icon("file-text", size=16), rx.text("Policies"), spacing="2", align="center"),
                            value="policies",
                        ),
                        size="2",
                    ),
                    background="rgba(20, 22, 30, 0.9)",
                    backdrop_filter="blur(16px)",
                    border_bottom="1px solid rgba(255, 255, 255, 0.08)",
                    padding_x="32px",
                    padding_y="8px",
                    position="sticky",
                    top="72px",
                    z_index="40",
                ),
                rx.box(
                    rx.tabs.content(members_tab(), value="members"),
                    rx.tabs.content(slots_tab(), value="slots"),
                    rx.tabs.content(ml_tab(), value="ml"),
                    rx.tabs.content(policies_tab(), value="policies"),
                    width="100%",
                    max_width="1400px",
                    margin_x="auto",
                    padding_x="32px",
                    padding_y="24px",
                ),
                default_value="members",
                orientation="horizontal",
                width="100%",
            ),
            width="100%",
        ),
        background="linear-gradient(180deg, #0f1015 0%, #1a1c24 50%, #0f1015 100%)",
        min_height="100vh",
        width="100%",
    )


app = rx.App(
    theme=rx.theme(
        appearance="dark",
        accent_color="blue",
        radius="large",
    ),
    style={
        "font_family": "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
    },
)
app.add_page(index, on_load=AppState.load_all, title="Carnival Casino Slot Analytics")
