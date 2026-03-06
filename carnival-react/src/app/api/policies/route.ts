import { NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

const DB = "CARNIVAL_CASINO";
const SCHEMA = "SLOT_ANALYTICS";

export async function GET() {
  try {
    const policies = await query<{
      POLICY_ID: number;
      CATEGORY: string;
      TITLE: string;
      CONTENT: string;
      LAST_UPDATED: string;
    }>(`SELECT * FROM ${DB}.${SCHEMA}.CASINO_POLICIES ORDER BY CATEGORY, POLICY_ID`);

    return NextResponse.json(policies);
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json({ error: "Failed to fetch policies" }, { status: 500 });
  }
}
