const path = require('path');
const {loadConfig, toUpperCamelCase} = require('./util');
const MultiMap = require('./multi-map');

module.exports = class ModelLoader {
  init(conf) {
    const configDir = path.join(__dirname, '..');
    const logsConf = this.logsConf = loadConfig(configDir, 'logs');
    const schemaMap = new MultiMap();
    const logMap = new Map();
    const showTables = new MultiMap();
  
    for (const logName in logsConf.logs) {
      const logConf = logsConf.logs[logName];
      const schema = logConf.schema || conf.srcDb.database;
      const logInfo = {
        name: logName,
        conf: logConf,
        schema,
        table: parseTable(logConf.logTable, schema),
        mainTable: parseTable(logConf.mainTable, schema)
      };
      logMap.set(logName, logInfo);
  
      const mainTable = addTable(logConf.mainTable, logInfo);
      mainTable.isMain = true;
  
      if (logConf.tables)
      for (const tableConf of logConf.tables){
        const table = addTable(tableConf, logInfo);
        if (table !== mainTable) {
          Object.assign(table, {
            main: mainTable,
            isMain: false
          });
        }
      }
    }
  
    function addTable(tableConf, logInfo) {
      if (typeof tableConf == 'string')
        tableConf = {name: tableConf};
      const table = parseTable(tableConf.name, logInfo.schema);
  
      let tableInfo = schemaMap.get(table.schema, table.name);
      if (!tableInfo) {
        tableInfo = {
          conf: tableConf,
          log: logInfo
        };
        schemaMap.set(table.schema, table.name, tableInfo);
      }
  
      let modelName = tableConf.modelName;
      if (!modelName) {
        modelName = logsConf.upperCaseTable
          ? toUpperCamelCase(table.name)
          : table.name;
      }
  
      const {
        showField,
        relation,
        idName
      } = tableConf;
  
      Object.assign(tableInfo, {
        conf: tableConf,
        exclude: new Set(tableConf.exclude),
        modelName,
        showField,
        relation,
        idName,
        userField: tableConf.userField || logsConf.userField
      });
  
      return tableInfo;
    }
  
    return {schemaMap, logMap, showTables};
  }

  async loadSchema(db, schemaMap, showTables) {
    const {logsConf} = this;
  
    for (const [schema, table, tableInfo] of schemaMap) {
      const tableConf = tableInfo.conf;
  
      // Fetch columns & types
  
      Object.assign (tableInfo, {
        castTypes: new Map(),
        columns: new Map()
      });
  
      if (tableConf.types)
      for (const col in tableConf.types)
        tableInfo.castTypes.set(col, tableConf.types[col]);
  
      const [dbCols] = await db.query(
        `SELECT
            COLUMN_NAME \`col\`,
            DATA_TYPE \`type\`,
            COLUMN_DEFAULT \`def\`
          FROM information_schema.\`COLUMNS\`
          WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?`,
        [table, schema]
      );
  
      for (const {col, type, def} of dbCols) {
        if (!tableInfo.exclude.has(col) && col != tableInfo.userField)
          tableInfo.columns.set(col, {type, def});
  
        const castType = logsConf.castTypes[type];
        if (castType && !tableInfo.castTypes.has(col))
          tableInfo.castTypes.set(col, castType);
      }
  
      // Fetch primary key
  
      if (!tableConf.idName) {
        const [dbPks] = await db.query(
          `SELECT COLUMN_NAME idName
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE CONSTRAINT_NAME = 'PRIMARY'
              AND TABLE_NAME = ?
              AND TABLE_SCHEMA = ?`,
          [table, schema]
        );
  
        if (!dbPks.length)
          throw new Error(`Primary not found for table: ${schema}.${table}`);
        if (dbPks.length > 1)
          throw new Error(`Only one column primary is supported: ${schema}.${table}`);
  
        for (const {idName} of dbPks)
          tableInfo.idName = idName;
      }
  
      // Get show field
  
      if (!tableConf.showField) {
        for (const showField of logsConf.showFields) {
          if (tableInfo.columns.has(showField)) {
            tableInfo.showField = showField;
            break;
          }
        }
      }
    }

    // Fetch relation to main table

    for (const [schema, table, tableInfo] of schemaMap) {
      if (!tableInfo.conf.relation && !tableInfo.isMain) {
        const mainTable = tableInfo.log.mainTable;
        const mainInfo = schemaMap.get(mainTable.schema, mainTable.name);
  
        const [mainRelations] = await db.query(
          `SELECT COLUMN_NAME relation
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = ?
              AND TABLE_SCHEMA = ?
              AND REFERENCED_TABLE_NAME = ?
              AND REFERENCED_TABLE_SCHEMA = ?
              AND REFERENCED_COLUMN_NAME = ?`,
          [
            table,
            schema,
            mainTable.name,
            mainTable.schema,
            mainInfo.idName
          ]
        );
  
        if (!mainRelations.length)
          throw new Error(`No relation to main table found for table: ${schema}.${table}`);
        if (mainRelations.length > 1)
          throw new Error(`Found more multiple relations to main table: ${schema}.${table}`);
  
        for (const {relation} of mainRelations)
          tableInfo.relation = relation;
      }
    }
  
    // Fetch relations with other tables
    // TODO: #5563 Fetch relations and show values in fronted
  
    showTables.clear();

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
        showTables.setIfEmpty(schema, table, {});
      }
    }

    const relatedList = Array.from(showTables.keys());

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
      showTables.get(schema, table).idName = idName;

    // Fetch show field of related tables

    const showFields = logsConf.showFields;
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
      const tableInfo = showTables.get(schema, table);
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
      const tableInfo = showTables.get(schema, table);
      const {idName, showField} = tableInfo;
      if (!idName || !showField || idName == showField) {
        showTables.delete(schema, table);
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
      if (!showTables.has(relation.schema, relation.table))
        tableInfo.relations.delete(col);
    }
  }
}

function parseTable(tableString, defaultSchema) {
  let name, schema;
  const split = tableString.split('.');
  if (split.length == 1) {
    name = split[0];
    schema = defaultSchema;
  } else {
    [schema, name] = split;
  }
  return {name, schema};
}
