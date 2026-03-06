import { NextResponse } from "next/server";
import { query } from "@/lib/snowflake";

const DB = "CARNIVAL_CASINO";
const SCHEMA = "SLOT_ANALYTICS";

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const ship = searchParams.get("ship");
    const where = ship && ship !== "All Ships" ? `WHERE SHIP_NAME = '${ship}'` : "";

    const [ships, stats, denominations, games, timeOfDay, gameTypes, monthly] = await Promise.all([
      query<{ SHIP_NAME: string }>(`
        SELECT DISTINCT SHIP_NAME FROM ${DB}.${SCHEMA}.SLOT_PLAY_HISTORY ORDER BY SHIP_NAME
      `),
      query<{ UNIQUE_PLAYERS: number; AVG_BET: number; AVG_DURATION: number; WIN_RATE: number }>(`
        SELECT 
          COUNT(DISTINCT MEMBER_ID) AS UNIQUE_PLAYERS,
          ROUND(AVG(BET_PER_SPIN),2) AS AVG_BET,
          ROUND(AVG(SESSION_DURATION_MINS),1) AS AVG_DURATION,
          ROUND(SUM(TOTAL_WON)/NULLIF(SUM(TOTAL_WAGERED),0)*100,1) AS WIN_RATE
        FROM ${DB}.${SCHEMA}.SLOT_PLAY_HISTORY ${where}
      `),
      query<{ DENOMINATION: number; TOTAL_WAGERED: number; SESSIONS: number }>(`
        SELECT DENOMINATION, ROUND(SUM(TOTAL_WAGERED),0) AS TOTAL_WAGERED, COUNT(*) AS SESSIONS
        FROM ${DB}.${SCHEMA}.SLOT_PLAY_HISTORY ${where}
        GROUP BY DENOMINATION ORDER BY DENOMINATION
      `),
      query<{ GAME_NAME: string; SESSIONS: number }>(`
        SELECT GAME_NAME, COUNT(*) AS SESSIONS
        FROM ${DB}.${SCHEMA}.SLOT_PLAY_HISTORY ${where}
        GROUP BY GAME_NAME ORDER BY SESSIONS DESC LIMIT 10
      `),
      query<{ TIME_OF_DAY: string; AVG_BET: number; SESSIONS: number }>(`
        SELECT TIME_OF_DAY, ROUND(AVG(BET_PER_SPIN),2) AS AVG_BET, COUNT(*) AS SESSIONS
        FROM ${DB}.${SCHEMA}.SLOT_PLAY_HISTORY ${where}
        GROUP BY TIME_OF_DAY ORDER BY SESSIONS DESC
      `),
      query<{ GAME_TYPE: string; SESSIONS: number; AVG_BET: number }>(`
        SELECT GAME_TYPE, COUNT(*) AS SESSIONS, ROUND(AVG(BET_PER_SPIN),2) AS AVG_BET
        FROM ${DB}.${SCHEMA}.SLOT_PLAY_HISTORY ${where}
        GROUP BY GAME_TYPE ORDER BY SESSIONS DESC
      `),
      query<{ MONTH: string; TOTAL_WAGERED: number; UNIQUE_PLAYERS: number }>(`
        SELECT TO_VARCHAR(DATE_TRUNC('MONTH', PLAY_DATE), 'YYYY-MM') AS MONTH,
          ROUND(SUM(TOTAL_WAGERED),0) AS TOTAL_WAGERED,
          COUNT(DISTINCT MEMBER_ID) AS UNIQUE_PLAYERS
        FROM ${DB}.${SCHEMA}.SLOT_PLAY_HISTORY ${where}
        GROUP BY 1 ORDER BY 1
      `)
    ]);

    return NextResponse.json({
      ships: ships.map(s => s.SHIP_NAME),
      stats: stats[0],
      denominations,
      games,
      timeOfDay,
      gameTypes,
      monthly
    });
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json({ error: "Failed to fetch slot data" }, { status: 500 });
  }
}
