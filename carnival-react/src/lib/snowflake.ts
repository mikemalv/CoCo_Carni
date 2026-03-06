import { execSync } from "child_process";

const SNOW_CLI = process.env.SNOW_CLI_PATH || "snow";
const CONNECTION = process.env.SNOWFLAKE_CONNECTION || "demo";

export async function query<T>(sql: string): Promise<T[]> {
  try {
    const result = execSync(
      `${SNOW_CLI} sql -c ${CONNECTION} -q "${sql.replace(/"/g, '\\"')}" --format json`,
      {
        encoding: "utf-8",
        timeout: 60000,
        maxBuffer: 50 * 1024 * 1024,
      }
    );
    return JSON.parse(result) as T[];
  } catch (err) {
    console.error("Query error:", (err as Error).message?.slice(0, 200));
    throw err;
  }
}
