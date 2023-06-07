const MultiMap = require("./multi-map");

/**
 * TODO: #5563 Fetch relations and show values in fronted
 */
module.exports = class ShowDb {
  init(logger) {
    Object.assign(this, {
      logger,
      conf: logger.conf.showCache,
      tables: new MultiMap(),
      cache: new MultiMap()
    });
  }

  checkDb() {
    const {conf, cache} = this;
    const now = Date.now();
    const dbOutdated = this.loops % conf.maxLoops == 0
      || this.lastFlush > now + conf.life * 1000

    if (dbOutdated) {
      cache.clear();
      this.loops = 0;
      this.lastFlush = now;
    }
    this.loops++;
  }

  async loadSchema() {
    const {logger, tables} = this;
    const {db, schemaMap} = logger;
    tables.clear();

    // Fetch relations with other tables

    for (const [schema, table, tableInfo] of schemaMap) {
      const [relations] = await db.query(
        `SELECT
            COLUMN_NAME \`col\`,
            REFERENCED_TABLE_SCHEMA \`schema\`,
            REFERENCED_TABLE_NAME \`table\`
          FROM information_schema.KEY_COLUMN_USAGE
          WHERE TABLE_NAME = ?
            AND TABLE_SCHEMA = ?
            AND COLUMN_NAME IN (?)
            AND REFERENCED_TABLE_NAME IS NOT NULL`,
        [
          table,
          schema,
          Array.from(tableInfo.columns.keys())
        ]
      );
  
      tableInfo.relations = new Map();
      for (const {col, schema, table} of relations) {
        if (col == tableInfo.relation) continue;
        tableInfo.relations.set(col, {schema, table});
        tables.setIfEmpty(schema, table, {});
      }
    }

    const relatedList = Array.from(tables.keys());

    // Fetch primary key of related tables

    const [res] = await db.query(
      `SELECT
          TABLE_SCHEMA \`schema\`,
          TABLE_NAME \`table\`,
          COLUMN_NAME \`idName\`,
          COUNT(*) nPks
        FROM information_schema.\`COLUMNS\`
        WHERE (TABLE_SCHEMA, TABLE_NAME) IN (?)
          AND COLUMN_KEY = 'PRI'
        GROUP BY TABLE_NAME, TABLE_SCHEMA
          HAVING nPks = 1`,
      [relatedList]
    );
    for (const {schema, table, idName} of res)
      tables.get(schema, table).idName = idName;

    // Fetch show field of related tables

    const showFields = logger.modelLoader.conf.showFields;
    const [result] = await db.query(
      `SELECT
          TABLE_SCHEMA \`schema\`,
          TABLE_NAME \`table\`,
          COLUMN_NAME \`col\`
        FROM information_schema.\`COLUMNS\`
        WHERE (TABLE_SCHEMA, TABLE_NAME) IN (?)
          AND COLUMN_NAME IN (?)
          AND COLUMN_KEY <> 'PRI'`,
      [relatedList, showFields]
    );

    for (const {schema, table, col} of result) {
      const tableInfo = tables.get(schema, table);
      let save;
      if (tableInfo.showField) {
        const newIndex = showFields.indexOf(col);
        const oldIndex = showFields.indexOf(tableInfo.showField);
        save = newIndex < oldIndex;
      } else
        save = true;
      if (save) tableInfo.showField = col;
    }

    // Clean tables and relations without required information

    for (const [schema, table] of relatedList) {
      const tableInfo = tables.get(schema, table);
      const {idName, showField} = tableInfo;
      if (!idName || !showField || idName == showField) {
        tables.delete(schema, table);
        continue;
      }

      const sqlShowField = db.escapeId(showField);
      const sqlIdName = db.escapeId(idName);
      const sqlTable = `${db.escapeId(schema)}.${db.escapeId(table)}`;

      tableInfo.selectStmt =
        `SELECT ${sqlIdName} \`id\`, ${sqlShowField} \`val\`
          FROM ${sqlTable}
          WHERE ${sqlIdName} IN (?)`;
    }

    for (const tableInfo of schemaMap.values())
    for (const [col, relation] of tableInfo.relations) {
      if (!tables.has(relation.schema, relation.table))
        tableInfo.relations.delete(col);
    }
  }

  async getValues(db, ops) {
    const {tables, cache} = this;
    const fetchMap = new MultiMap();

    this.checkDb();

    // Fetch relations ids

    for (const op of ops) {
      const {
        relations,
        showRelation
      } = op.tableInfo;
  
      for (const change of op.changes) {
        let rows;
        if (op.action == 'update')
          rows = [change.newI, change.oldI];
        else
          rows = [change.instance];

        if (showRelation)
          rows.push({[showRelation]: change.row[showRelation]});

        for (const row of rows)
        for (const col in row) {
          const relation = relations.get(col);
          if (!relation) continue;
          const {schema, table} = relation;
          const id = row[col];

          let ids = cache.get(schema, table);
          if (ids && ids.has(id)) continue;

          ids = fetchMap.get(schema, table);
          if (!ids) fetchMap.set(schema, table, ids = new Set());
          ids.add(id);
        }
      }
    }

    // Query show values to database

    for (const [schema, table, fetchIds] of fetchMap) {
      const tableInfo = tables.get(schema, table);
      const [res] = await db.query(
        tableInfo.selectStmt,
        [Array.from(fetchIds.keys())]
      );

      let ids = cache.get(schema, table);
      if (!ids) cache.set(schema, table, ids = new Map());

      for (const row of res)
        ids.set(row.id, row.val);
    }

    // Fill rows with show values

    for (const op of ops) {
      const {
        relations,
        showRelation,
        showField
      } = op.tableInfo;
  
      for (const change of op.changes) {
        let rows;
        if (op.action == 'update')
          rows = [change.newI, change.oldI];
        else
          rows = [change.instance];

        for (const row of rows)
        for (const col in row) {
          const relation = relations.get(col);
          if (!relation) continue;
          const showValue = getValue(relation, row, col);
          if (showValue) row[col +'$'] = showValue;
        }

        const {row} = change;
        if (showRelation) {
          const relation = relations.get(showRelation);
          change.modelValue = getValue(relation, row, showRelation);
        } else if (showField)
          change.modelValue = row[showField];
      }
    }

    function getValue(relation, row, col) {
      const {schema, table} = relation;
      const ids = cache.get(schema, table);
      return ids && ids.get(row[col])
    }
  }
}
