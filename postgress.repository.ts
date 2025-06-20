import { Pool } from "pg";
import pgPool from "../config/db";
import { storeGeography } from "../interface/storeGeography";
import format from "pg-format";

const CHUNK_SIZE = 2000; // Tune based on available RAM and CPU
const PARALLEL_LIMIT = 4; // Number of parallel operations

function chunkArray<T>(arr: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    result.push(arr.slice(i, i + size));
  }
  return result;
}

export class PostgresRepository {
  constructor(private pool: Pool = pgPool) {}

  private async upsertChunk(
    chunk: storeGeography[],
    chunkIndex: number
  ): Promise<void> {
    const client = await this.pool.connect();
    const tempTableName = `temp_store_geo_${
      process.pid
    }_${Date.now()}_${chunkIndex}`;
    const columns = [
      "STORE_NBR",
      "RETAIL_SQ_FT",
      "HR24_IND",
      "CITY_NAME",
      "STATE_CD",
      "ZIP_CD",
      "PHARMACY_IND",
      "ACTIVE_STORE_IND",
      "CLINIC_LINK_STORE_NBR",
      "MIN_CLINIC_IND",
      "PRIMARY_DC_ID",
      "WEB_SALE_PICKUP_LOCATION_IND",
      "DISTRICT_NBR",
      "STORE_OPEN_DT",
      "STORE_CLOSE_DT",
      "AREA_NBR",
      "REGION_NBR",
      "STREET_TXT",
    ];

    try {
      await client.query("BEGIN");

      // 1. Create temp table
      await client.query(`
        CREATE TEMP TABLE ${tempTableName} AS 
        SELECT * FROM plappl."POG_STR_GEO" LIMIT 0
      `);

      // 2. Bulk insert into temp table
      const values = chunk.map((row) => [
        row.STR_NBR,
        row.RETAIL_SQ_FT,
        row.HR24_IND,
        row.CITY_NAME,
        row.STATE_CD,
        row.ZIP_CD,
        row.PHARMACY_IND,
        row.ACTIVE_STORE_IND,
        row.CLINIC_LINK_STORE_NBR,
        row.MIN_CLINIC_IND,
        row.PRIMARY_DC_ID,
        row.WEB_SALE_PICKUP_LOCATION_IND,
        row.DISTRICT_NBR,
        row.STORE_OPEN_DT,
        row.STORE_CLOSE_DT,
        row.AREA_NBR,
        row.REGION_NBR,
        row.STREET_TXT,
      ]);

      const insertSql = format(
        `INSERT INTO ${tempTableName} (%I) VALUES %L`,
        columns,
        values
      );
      await client.query(insertSql);

      // 3. Delete from main table
      await client.query(
        format(`
        DELETE FROM plappl."POG_STR_GEO" t
        USING ${tempTableName} temp
        WHERE t."STORE_NBR" = temp."STORE_NBR"
      `)
      );

      // 4. Insert into main table
      await client.query(
        format(
          `
        INSERT INTO plappl."POG_STR_GEO" (%I)
        SELECT %I FROM ${tempTableName}
      `,
          columns,
          columns
        )
      );

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(`❌ Chunk ${chunkIndex} failed:`, err);
      throw err;
    } finally {
      client.release();
    }
  }

  public async upsertStoreGeographyBatch(
    rows: storeGeography[]
  ): Promise<void> {
    const chunks = chunkArray(rows, CHUNK_SIZE);

    // Batch with parallel limit
    const runBatches = async () => {
      for (let i = 0; i < chunks.length; i += PARALLEL_LIMIT) {
        const batch = chunks.slice(i, i + PARALLEL_LIMIT);
        await Promise.all(
          batch.map((chunk, idx) => this.upsertChunk(chunk, i + idx))
        );
      }
    };

    const start = Date.now();
    await runBatches();
    console.log(
      `✅ Total ${rows.length} rows upserted in ${Date.now() - start} ms`
    );
  }
}
