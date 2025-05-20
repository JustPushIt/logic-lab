import { Pool } from "pg";
import pgPool from "../config/db";
import { storeGeography } from "../interface/storeGeography";
import format from "pg-format";

const CHUNK_SIZE = 2000;         // Tune based on row size and memory
const PARALLEL_LIMIT = 4;        // Number of parallel batches to run

function chunkArray<T>(arr: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    result.push(arr.slice(i, i + size));
  }
  return result;
}

export class PostgresRepository {
  constructor(private pool: Pool = pgPool) {}

  public async upsertStoreGeographyBatch(rows: storeGeography[]): Promise<void> {
    const chunks = chunkArray(rows, CHUNK_SIZE);

    const runParallelBatches = async () => {
      for (let i = 0; i < chunks.length; i += PARALLEL_LIMIT) {
        const batch = chunks.slice(i, i + PARALLEL_LIMIT);
        await Promise.all(
          batch.map((chunk, idx) =>
            this.bulkReplaceChunk(chunk, i + idx)
          )
        );
      }
    };

    const start = Date.now();
    try {
      await runParallelBatches();
      console.log(`✅ Upserted ${rows.length} rows in ${Date.now() - start} ms`);
    } catch (err) {
      console.error("❌ Error in parallel upsert:", err);
      throw err;
    }
  }

  private async bulkReplaceChunk(chunk: storeGeography[], chunkIndex: number): Promise<void> {
    const client = await this.pool.connect();
    const columns = [
      "STORE_NBR", "RETAIL_SQ_FT", "HR24_IND", "CITY_NAME", "STATE_CD",
      "ZIP_CD", "PHARMACY_IND", "ACTIVE_STORE_IND", "CLINIC_LINK_STORE_NBR",
      "MIN_CLINIC_IND", "PRIMARY_DC_ID", "WEB_SALE_PICKUP_LOCATION_IND",
      "DISTRICT_NBR", "STORE_OPEN_DT", "STORE_CLOSE_DT", "AREA_NBR",
      "REGION_NBR", "STREET_TXT"
    ];

    const values = chunk.map(row => [
      row.STR_NBR, row.RETAIL_SQ_FT, row.HR24_IND, row.CITY_NAME, row.STATE_CD,
      row.ZIP_CD, row.PHARMACY_IND, row.ACTIVE_STORE_IND, row.CLINIC_LINK_STORE_NBR,
      row.MIN_CLINIC_IND, row.PRIMARY_DC_ID, row.WEB_SALE_PICKUP_LOCATION_IND,
      row.DISTRICT_NBR, row.STORE_OPEN_DT, row.STORE_CLOSE_DT, row.AREA_NBR,
      row.REGION_NBR, row.STREET_TXT
    ]);

    try {
      await client.query("BEGIN");

      // 1. Delete old rows with matching STORE_NBR
      const storeNbrs = chunk.map(row => row.STR_NBR);
      const deleteSql = format(
        `DELETE FROM plappl."POG_STR_GEO" WHERE "STORE_NBR" IN (%L)`,
        storeNbrs
      );
      await client.query(deleteSql);

      // 2. Insert new rows
      const insertSql = format(
        `INSERT INTO plappl."POG_STR_GEO" (%I) VALUES %L`,
        columns,
        values
      );
      await client.query(insertSql);

      await client.query("COMMIT");
      console.log(`✅ Chunk ${chunkIndex} committed (${chunk.length} rows)`);
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(`❌ Chunk ${chunkIndex} failed`, err);
      throw err;
    } finally {
      client.release();
    }
  }
}
