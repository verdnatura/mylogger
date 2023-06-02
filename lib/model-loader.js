const path = require('path');
const {loadConfig, toUpperCamelCase} = require('./util');

module.exports = class ModelLoader {
  init(conf) {
    const configDir = path.join(__dirname, '..');
    const logsConf = this.logsConf = loadConfig(configDir, 'logs');
    const schemaMap = new Map();
    const logMap = new Map();
  
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
  
      let tableMap = schemaMap.get(table.schema);
      if (!tableMap) {
        tableMap = new Map();
        schemaMap.set(table.schema, tableMap);
      }
  
      let tableInfo = tableMap.get(table.name);
      if (!tableInfo) {
        tableInfo = {
          conf: tableConf,
          log: logInfo
        };
        tableMap.set(table.name, tableInfo);
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
  
    return {schemaMap, logMap};
  }

  async loadSchema(schemaMap, db) {
    const {logsConf} = this;
  
    for (const [schema, tableMap] of schemaMap)
    for (const [table, tableInfo] of tableMap) {
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

    for (const [schema, tableMap] of schemaMap)
    for (const [table, tableInfo] of tableMap) {
  
      if (!tableInfo.conf.relation && !tableInfo.isMain) {
        const mainTable = tableInfo.log.mainTable;
        const mainTableInfo = schemaMap
          .get(mainTable.schema)
          .get(mainTable.name);
  
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
            mainTableInfo.idName
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
  
    // Fetch relations and show values of related tables
    // TODO: #5563 Not used yet 
  
    const relatedList = [];
    const relatedMap = new Map();

    for (const [schema, tableMap] of schemaMap)
    for (const [table, tableInfo] of tableMap) {
      const [relations] = await db.query(
        `SELECT
            COLUMN_NAME \`col\`,
            REFERENCED_TABLE_SCHEMA \`schema\`,
            REFERENCED_TABLE_NAME \`table\`,
            REFERENCED_COLUMN_NAME \`column\`
          FROM information_schema.KEY_COLUMN_USAGE
          WHERE TABLE_NAME = ?
            AND TABLE_SCHEMA = ?
            AND REFERENCED_TABLE_NAME IS NOT NULL`,
        [table, schema]
      );
  
      tableInfo.relations = new Map();
      for (const {col, schema, table, column} of relations) {
        tableInfo.relations.set(col, {schema, table, column});
        relatedList.push([table, schema]);
  
        let tables = relatedMap.get(schema);
        if (!tables) relatedMap.set(schema, tables = new Set());
        if (!tables.has(table)) {
          tables.add(table);
          relatedList.push([table, schema]);
        }
      }
    }
  
    const showFields = logsConf.showFields;
    const [result] = await db.query(
      `SELECT
          TABLE_NAME \`table\`,
          TABLE_SCHEMA \`schema\`,
          COLUMN_NAME \`col\`
        FROM information_schema.\`COLUMNS\`
        WHERE (TABLE_NAME, TABLE_SCHEMA) IN (?)
          AND COLUMN_NAME IN (?)`,
      [relatedList, showFields]
    );
  
    const showTables = new Map();
  
    for (const {table, schema, col} of result) {
      let tables = showTables.get(schema);
      if (!tables) showTables.set(schema, tables = new Map())
      const showField = tables.get(table);
  
      let save;
      if (showField) {
        const newIndex = showFields.indexOf(col);
        const oldIndex = showFields.indexOf(showField);
        save = newIndex < oldIndex;
      } else
        save = true;
  
      if (save) tables.set(table, col);
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
