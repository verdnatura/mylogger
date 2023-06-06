const path = require('path');
const {loadConfig, toUpperCamelCase} = require('./util');
const MultiMap = require('./multi-map');

module.exports = class ModelLoader {
  init(logger) {
    const configDir = path.join(__dirname, '..');
    const conf = loadConfig(configDir, 'logs');
    const schemaMap = new MultiMap();
    const logMap = new Map();

    Object.assign(this, {
      logger,
      conf
    });
    Object.assign(logger, {
      schemaMap,
      logMap
    });
  
    for (const logName in conf.logs) {
      const logConf = conf.logs[logName];
      const schema = logConf.schema || logger.conf.srcDb.database;
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
        modelName = conf.upperCaseTable
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
        userField: tableConf.userField || conf.userField
      });
  
      return tableInfo;
    }
  }

  async loadSchema() {
    const {db, schemaMap} = this.logger;
    const {conf} = this;
  
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
  
        const castType = conf.castTypes[type];
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
  
      if (!tableInfo.isMain) {
        const {showField} = tableInfo;
        if (!showField) {
          for (const field of conf.showFields) {
            if (tableInfo.columns.has(field)) {
              tableInfo.showField = field;
              break;
            }
          }
        } else {
          const match = showField.match(/(^.*)\$$/);
          if (match) tableInfo.showRelation = match[1];
        }
      } else
        tableInfo.showField = null;
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
