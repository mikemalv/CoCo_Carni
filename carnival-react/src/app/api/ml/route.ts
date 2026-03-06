import { NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

const DB = "CARNIVAL_CASINO";
const SCHEMA = "SLOT_ANALYTICS";

export async function GET() {
  try {
    const [models, confusion, members] = await Promise.all([
      query<{
        MODEL_NAME: string;
        MODEL_TYPE: string;
        TARGET: string;
        MODEL_ACCURACY_PCT: number;
        BASELINE_ACCURACY_PCT: number;
        NOTES: string;
      }>(`SELECT * FROM ${DB}.${SCHEMA}.MODEL_EVALUATION_SUMMARY`),
      query<{ ACTUAL: string; PREDICTED: string; CNT: number }>(`
        SELECT 
          PREFERRED_DENOMINATION AS ACTUAL,
          PREDICTION:class::STRING AS PREDICTED,
          COUNT(*) AS CNT
        FROM ${DB}.${SCHEMA}.DENOMINATION_PREDICTIONS
        GROUP BY ACTUAL, PREDICTED
        ORDER BY ACTUAL, PREDICTED
      `),
      query<{
        MEMBER_ID: number;
        GENDER: string;
        AGE: number;
        MEMBERSHIP_TIER: string;
        RISK_APPETITE: string;
        INCOME_BRACKET: string;
      }>(`
        SELECT m.MEMBER_ID, m.GENDER, m.AGE, m.MEMBERSHIP_TIER, m.RISK_APPETITE, m.INCOME_BRACKET
        FROM ${DB}.${SCHEMA}.MEMBER_DEMOGRAPHICS m
        JOIN ${DB}.${SCHEMA}.ML_TRAINING_DATA t ON m.MEMBER_ID = t.MEMBER_ID
        ORDER BY m.MEMBER_ID
      `)
    ]);

    let features: { FEATURE: string; SCORE: number }[] = [];
    try {
      const rawFeatures = await query<{ RANK: number; FEATURE: string; SCORE: number; FEATURE_TYPE: string }>(
        `CALL ${DB}.${SCHEMA}.DENOMINATION_CLASSIFIER!SHOW_FEATURE_IMPORTANCE()`
      );
      features = rawFeatures
        .map((f) => ({ FEATURE: String(f.FEATURE), SCORE: Number(f.SCORE) }))
        .sort((a, b) => b.SCORE - a.SCORE)
        .slice(0, 15);
    } catch (e) {
      console.log("Could not load feature importance:", e);
    }

    return NextResponse.json({
      models,
      confusion,
      features,
      members
    });
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json({ error: "Failed to fetch ML data" }, { status: 500 });
  }
}
