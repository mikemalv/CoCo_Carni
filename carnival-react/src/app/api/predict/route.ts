import { NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

const DB = "CARNIVAL_CASINO";
const SCHEMA = "SLOT_ANALYTICS";

export async function POST(request: Request) {
  try {
    const { memberId } = await request.json();

    const [denomResult, betResult, profile] = await Promise.all([
      query<Record<string, string>>(`CALL ${DB}.${SCHEMA}.PREDICT_DENOMINATION(${memberId})`),
      query<Record<string, string>>(`CALL ${DB}.${SCHEMA}.PREDICT_BET_CATEGORY(${memberId})`),
      query<Record<string, string | number>>(`
        SELECT m.MEMBER_ID, m.AGE, m.GENDER, m.MEMBERSHIP_TIER, m.RISK_APPETITE,
               m.INCOME_BRACKET, m.MARITAL_STATUS, m.TOTAL_CRUISES, m.LIFETIME_SPEND,
               f.TOTAL_SESSIONS, f.TOTAL_SPINS, f.AVG_BET_PER_SPIN, 
               f.PREFERRED_DENOMINATION AS ACTUAL_PREF_DENOM,
               f.WIN_RATE_PCT
        FROM ${DB}.${SCHEMA}.MEMBER_DEMOGRAPHICS m
        JOIN ${DB}.${SCHEMA}.MEMBER_SLOT_FEATURES f ON m.MEMBER_ID = f.MEMBER_ID
        WHERE m.MEMBER_ID = ${memberId}
      `)
    ]);

    return NextResponse.json({
      denomination: Object.values(denomResult[0])[0],
      betCategory: Object.values(betResult[0])[0],
      profile: profile[0]
    });
  } catch (error) {
    console.error("Prediction error:", error);
    return NextResponse.json({ error: "Prediction failed" }, { status: 500 });
  }
}
