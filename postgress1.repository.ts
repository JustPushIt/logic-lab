public async upsertStoreGeographyBatch(rows: storeGeography[]): Promise<void> {
  const client = await this.pool.connect();
  const tempTableName = 'temp_store_geography';

  const columns = [
    "STORE_NBR", "RETAIL_SQ_FT", "HR24_IND", "CITY_NAME", "STATE_CD",
    "ZIP_CD", "PHARMACY_IND", "ACTIVE_STORE_IND", "CLINIC_LINK_STORE_NBR",
    "MIN_CLINIC_IND", "PRIMARY_DC_ID", "WEB_SALE_PICKUP_LOCATION_IND",
    "DISTRICT_NBR", "STORE_OPEN_DT", "STORE_CLOSE_DT", "AREA_NBR",
    "REGION_NBR", "STREET_TXT"
  ];

  try {
    await client.query("BEGIN");

    // 1. Create temporary table
    await client.query(`
      CREATE TEMP TABLE ${tempTableName} AS 
      SELECT * FROM plappl."POG_STR_GEO" LIMIT 0
    `);

    // 2. Bulk insert into temp table
    const values = rows.map(row => [
      row.STR_NBR, row.RETAIL_SQ_FT, row.HR24_IND, row.CITY_NAME, row.STATE_CD,
      row.ZIP_CD, row.PHARMACY_IND, row.ACTIVE_STORE_IND, row.CLINIC_LINK_STORE_NBR,
      row.MIN_CLINIC_IND, row.PRIMARY_DC_ID, row.WEB_SALE_PICKUP_LOCATION_IND,
      row.DISTRICT_NBR, row.STORE_OPEN_DT, row.STORE_CLOSE_DT, row.AREA_NBR,
      row.REGION_NBR, row.STREET_TXT
    ]);

    const insertSql = format(`
      INSERT INTO ${tempTableName} (%I) VALUES %L
    `, columns, values);

    await client.query(insertSql);

    // 3. Delete matching records from target table
    await client.query(`
      DELETE FROM plappl."POG_STR_GEO" t
      USING ${tempTableName} temp
      WHERE t."STORE_NBR" = temp."STORE_NBR"
    `);

    // 4. Insert all new data
    await client.query(`
      INSERT INTO plappl."POG_STR_GEO" (%I)
      SELECT %I FROM ${tempTableName}
    `, columns, columns);

    await client.query("COMMIT");
    console.log(`✅ Upserted ${rows.length} rows via temp table.`);
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("❌ Error during temp table upsert:", error);
    throw error;
  } finally {
    client.release();
  }
}
