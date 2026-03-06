import { NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

const DB = "CARNIVAL_CASINO";
const SCHEMA = "SLOT_ANALYTICS";

export async function GET() {
  try {
    const [kpis, tiers, ages, income, states] = await Promise.all([
      query<{ METRIC: string; VALUE: number }>(`
        SELECT 'total_members' AS METRIC, COUNT(*) AS VALUE FROM ${DB}.${SCHEMA}.MEMBER_DEMOGRAPHICS
        UNION ALL
        SELECT 'total_sessions', COUNT(*) FROM ${DB}.${SCHEMA}.SLOT_PLAY_HISTORY
        UNION ALL
        SELECT 'total_wagered', ROUND(SUM(TOTAL_WAGERED),0) FROM ${DB}.${SCHEMA}.SLOT_PLAY_HISTORY
        UNION ALL
        SELECT 'avg_sessions', ROUND(AVG(TOTAL_SESSIONS),1) FROM ${DB}.${SCHEMA}.MEMBER_SLOT_FEATURES
      `),
      query<{ MEMBERSHIP_TIER: string; MEMBERS: number }>(`
        SELECT MEMBERSHIP_TIER, COUNT(*) AS MEMBERS
        FROM ${DB}.${SCHEMA}.MEMBER_DEMOGRAPHICS
        GROUP BY MEMBERSHIP_TIER ORDER BY MEMBERS DESC
      `),
      query<{ AGE_GROUP: string; GENDER: string; CNT: number }>(`
        SELECT 
          CONCAT(FLOOR(AGE/10)*10, 's') AS AGE_GROUP,
          GENDER,
          COUNT(*) AS CNT
        FROM ${DB}.${SCHEMA}.MEMBER_DEMOGRAPHICS
        GROUP BY 1, GENDER ORDER BY 1
      `),
      query<{ INCOME_BRACKET: string; CNT: number }>(`
        SELECT INCOME_BRACKET, COUNT(*) AS CNT FROM ${DB}.${SCHEMA}.MEMBER_DEMOGRAPHICS
        GROUP BY INCOME_BRACKET ORDER BY CNT DESC
      `),
      query<{ HOME_STATE: string; MEMBERS: number }>(`
        SELECT HOME_STATE, COUNT(*) AS MEMBERS FROM ${DB}.${SCHEMA}.MEMBER_DEMOGRAPHICS
        GROUP BY HOME_STATE ORDER BY MEMBERS DESC
      `)
    ]);

    const kpiMap = Object.fromEntries(kpis.map(k => [k.METRIC, k.VALUE]));

    return NextResponse.json({
      kpis: kpiMap,
      tiers,
      ages,
      income,
      states
    });
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json({ error: "Failed to fetch data" }, { status: 500 });
  }
}
