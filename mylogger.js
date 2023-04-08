require('require-yaml');
require('colors');
const fs = require('fs');
const path = require('path');
const ZongJi = require('./zongji');
const mysql = require('mysql2/promise');

const allEvents = [
  'writerows',
  'updaterows',
  'deleterows'
];

const actions = {
  writerows: 'insert',
  updaterows: 'update',
  deleterows: 'delete'
};

module.exports = class MyLogger {
  constructor() {
    this.running = false;
    this.filename = null;
    this.position = null;
    this.schemaMap = new Map();
    this.logMap = new Map();
  }

  async start() {
    const defaultConfig = require('./config.yml');
    const conf = this.conf = Object.assign({}, defaultConfig);
    const localPath = path.join(__dirname, 'config.local.yml');
    if (fs.existsSync(localPath)) {
      const localConfig = require(localPath);
      Object.assign(conf, localConfig);
    }

    const defaultSchema = conf.db.database;
    function parseTable(tableString) {
      let name, schema;
      const split = tableString.split('.');
      if (split.length == 1) {
        name = split[0];
        schema = defaultSchema;
      } else {
        [name, schema] = split;
      }
      return {name, schema};
    }

    const schemaMap = this.schemaMap;
    function addTable(tableConf, logInfo) {
      if (typeof tableConf == 'string')
      tableConf = {name: tableConf};
      const table = parseTable(tableConf.name);

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

      Object.assign(tableInfo, {
        conf: tableConf,
        exclude: new Set(tableConf.exclude),
        castTypes: new Map(),
        columns: new Map(),
        showField: tableConf.showField,
        relation: tableConf.relation
      });

      if (tableConf.types)
      for (const col in tableConf.types)
        tableInfo.castTypes.set(col, tableConf.types[col]);

      return tableInfo;
    }

    for (const logName in conf.logs) {
      const logConf = conf.logs[logName];
      const logInfo = {
        conf: logConf,
        table: parseTable(logConf.logTable),
        mainTable: parseTable(logConf.mainTable)
      };
      this.logMap.set(logName, logInfo);

      const mainTable = addTable(logInfo.mainTable, logInfo);
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

    const includeSchema = {};
    for (const [schemaName, tableMap] of this.schemaMap)
      includeSchema[schemaName] = Array.from(tableMap.keys());
  
    this.opts = {
      includeEvents: [
        'rotate',
        'tablemap',
        'writerows',
        'updaterows',
        'deleterows'
      ],
      includeSchema
    };

    if (conf.testMode)
      console.log('Test mode enabled, just logging queries to console.');

    console.log('Starting process.');
    await this.init();
    console.log('Process started.');
  }

  async stop() {
    console.log('Stopping process.');
    await this.end();
    console.log('Process stopped.');
  }

  async init() {
    const {conf} = this;
    this.debug('MyLogger', 'Initializing.');
    this.onErrorListener = err => this.onError(err);

    // DB connection

    const db = this.db = await mysql.createConnection(conf.db);
    db.on('error', this.onErrorListener);

    for (const logInfo of this.logMap.values()) {
      const table = logInfo.table;
      const sqlTable = `${db.escapeId(table.schema)}.${db.escapeId(table.name)}`
      logInfo.addStmt = await db.prepare(
        `INSERT INTO ${sqlTable}
          SET originFk = ?,
            userFk = ?,
            action = ?,
            creationDate = ?,
            changedModel = ?,
            oldInstance = ?,
            newInstance = ?,
            changedModelId = ?,
            changedModelValue = ?`
      );
      logInfo.fetchStmt = await db.prepare(
        `SELECT id FROM ${sqlTable}
          WHERE changedModel = ?
            AND changedModelId = ?
            AND action = 'delete'`
      );
      logInfo.updateStmt = await db.prepare(
        `UPDATE ${sqlTable}
          SET originFk = ?,
            creationDate = ?,
            oldInstance = ?,
            changedModelValue = ?
          WHERE id = ?`
      );
    }

    for (const [schema, tableMap] of this.schemaMap)
    for (const [table, tableInfo] of tableMap) {

      // Fetch columns & types

      const [dbCols] = await db.query(
        `SELECT COLUMN_NAME \`col\`, DATA_TYPE \`type\`, COLUMN_DEFAULT \`def\`
          FROM information_schema.\`COLUMNS\`
          WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?`,
        [table, schema]
      );

      for (const {col, type, def} of dbCols) {
        if (!tableInfo.exclude.has(col))
          tableInfo.columns.set(col, {type, def});

        const castType = conf.castTypes[type];
        if (castType && !tableInfo.castTypes.has(col))
          tableInfo.castTypes.set(col, castType);
      }

      // Fetch primary key

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

      // Get show field

      if (!tableInfo.showField) {
        for (const showField of conf.showFields) {
          if (tableInfo.columns.has(showField)) {
            tableInfo.showField = showField;
            break;
          }
        }
      }
    }

    for (const [schema, tableMap] of this.schemaMap)
    for (const [table, tableInfo] of tableMap) {

      // Fetch relation

      if (!tableInfo.relation && !tableInfo.isMain) {
        const mainTable = tableInfo.log.mainTable;
        const mainTableInfo = this.schemaMap
          .get(mainTable.schema)
          .get(mainTable.name);

        const [relations] = await db.query(
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

        console.debug(
          table,
          schema,
          mainTable.name,
          mainTable.schema,
          mainTableInfo.idName
        );

        if (!relations.length)
          throw new Error(`No relation to main table found for table: ${schema}.${table}`);
        if (relations.length > 1)
          throw new Error(`Found more multiple relations to main table: ${schema}.${table}`);
  
        for (const {relation} of relations)
          tableInfo.relation = relation;
      }
    }

    // Zongji

    const zongji = new ZongJi(conf.db);
    this.zongji = zongji;

    this.onBinlogListener = evt => this.onBinlog(evt);
    zongji.on('binlog', this.onBinlogListener);

    const [res] = await db.query(
      'SELECT `logName`, `position` FROM `util`.`binlogQueue` WHERE code = ?',
      [conf.code]
    );
    if (res.length) {
      const [row] = res;
      this.filename = row.logName;
      this.position = row.position;
      Object.assign(this.opts, {
        filename: this.filename,
        position: this.position
      });
    } else
      this.opts.startAtEnd = true;

    this.debug('Zongji', 'Starting.');
    await new Promise((resolve, reject) => {
      const onReady = () => {
        zongji.off('error', onError);
        resolve();
      };
      const onError = err => {
        this.zongji = null;
        zongji.off('ready', onReady);
        zongji.off('binlog', this.onBinlogListener);
        reject(err);
      }

      zongji.once('ready', onReady);
      zongji.once('error',  onError);
      zongji.start(this.opts);
    });
    this.debug('Zongji', 'Started.');

    this.zongji.on('error', this.onErrorListener);

    this.flushInterval = setInterval(
      () => this.flushQueue(), conf.flushInterval * 1000);
    this.pingInterval = setInterval(
      () => this.connectionPing(), conf.pingInterval * 1000);

    // Summary

    this.running = true;
    this.debug('MyLogger', 'Initialized.');
  }

  async end(silent) {
    const zongji = this.zongji;
    if (!zongji) return;

    this.debug('MyLogger', 'Ending.');

    // Zongji

    clearInterval(this.flushInterval);
    clearInterval(this.pingInterval);
    zongji.off('binlog', this.onBinlogListener);
    zongji.off('error', this.onErrorListener);
    this.zongji = null;
    this.running = false;

    this.debug('Zongji', 'Stopping.');
    // FIXME: Cannot call Zongji.stop(), it doesn't wait to end connection
    zongji.connection.destroy(() => {
      console.log('zongji.connection.destroy');
    });
    await new Promise(resolve => {
      zongji.ctrlConnection.query('KILL ?', [zongji.connection.threadId],
      err => {
        if (err && err.code !== 'ER_NO_SUCH_THREAD' && !silent)
          console.error(err);
        resolve();
      });
    });
    zongji.ctrlConnection.destroy(() => {
      console.log('zongji.ctrlConnection.destroy');
    });
    zongji.emit('stopped');
    this.debug('Zongji', 'Stopped.');

    // DB connection

    this.db.off('error', this.onErrorListener);
    // FIXME: mysql2/promise bug, db.end() ends process
    this.db.on('error', () => {});
    try {
      await this.db.end();
    } catch (err) {
      if (!silent)
        console.error(err);
    }

    // Summary

    this.debug('MyLogger', 'Ended.');
  }

  async tryRestart() {
    try {
      await this.init();
      console.log('Process restarted.');
    } catch(err) {
      setTimeout(() => this.tryRestart(), 30);
    }
  }

  async onError(err) {
    console.log(`Error: ${err.code}: ${err.message}`);
    try {
      await this.end(true);
    } catch(e) {}

    switch (err.code) {
      case 'PROTOCOL_CONNECTION_LOST':
      case 'ECONNRESET':
      console.log('Trying to restart process.');
      await this.tryRestart();
      break;
    default:
      process.exit();
    }
  }

  onBinlog(evt) {
    //evt.dump();
    const eventName = evt.getEventName();
    let position = evt.nextPosition;

    switch (eventName) {
    case 'rotate':
      this.filename = evt.binlogName;
      position = evt.position;
      console.log(`[${eventName}] filename: ${this.filename}`, `position: ${this.position}, nextPosition: ${evt.nextPosition}`);
      break;
    case 'writerows':
    case 'deleterows':
    case 'updaterows':
      this.onRowEvent(evt, eventName);
      break;
    }

    this.position = position;
    this.flushed = false;
  }

  async onRowEvent(evt, eventName) {
    const table = evt.tableMap[evt.tableId];
    const tableName = table.tableName;
    const tableMap = this.schemaMap.get(table.parentSchema);
    if (!tableMap) return;

    const tableInfo = tableMap.get(tableName);
    if (!tableInfo) return;

    const action = actions[eventName];
    const columns = tableInfo.columns;
    let changes = [];

    function castValue(col, value) {
      switch(tableInfo.castTypes.get(col)) {
        case 'boolean':
          return !!value;
        default:
          return value;
        
      }
    }

    if (action == 'update') {
      for (const row of evt.rows) {
        let nColsChanged = 0;
        const before = row.before;
        const after = row.after;
        const oldI = {};
        const newI = {};

        for (const col in before) {
          if (columns.has(col)
          && after[col] !== undefined
          && !equals(after[col], before[col])) {
            if (before[col] !== null)
              oldI[col] = castValue(col, before[col]);
            newI[col] = castValue(col, after[col]);
            nColsChanged++;
          }
        }
        if (nColsChanged)
          changes.push({row: after, oldI, newI});
      }
    } else {
      const cols = columns.keys();

      for (const row of evt.rows) {
        const instance = {};

        for (const col of cols) {
          if (row[col] !== null)
            instance[col] = castValue(col, row[col]);
        }

        changes.push({row, instance});
      }
    }

    if (!changes.length) return;

    if (this.debug) {
      console.debug('Log:'.blue,
        `${tableName}(${changes}) [${eventName}]`);
    }

    const logInfo = tableInfo.log;
    const isDelete = action == 'delete';

    for (const change of changes) {
      let newI, oldI;
      const row = change.row;

      switch(action) {
        case 'update':
          newI = change.newI;
          oldI = change.oldI;
          break;
        case 'insert':
          newI = change.instance;
          break;
        case 'delete':
          oldI = change.instance;
          break;
      }

      const modelId = row[tableInfo.idName];
      const modelValue = tableInfo.showField
        ? row[tableInfo.showField] || null
        : null;
      const created = new Date(evt.timestamp);
      const oldInstance = oldI ? JSON.stringify(oldI) : null;
      const originFk = !tableInfo.isMain
        ? row[tableInfo.relation]
        : modelId;

      let deleteRow;

      if (isDelete) {
        [[deleteRow]] = await logInfo.fetchStmt.execute([
          tableName, modelId
        ]);
        if (deleteRow)
          await logInfo.updateStmt.execute([
            originFk,
            created,
            oldInstance,
            modelValue,
            deleteRow.id
          ]);
      }
      if (!isDelete || !deleteRow) {
        await logInfo.addStmt.execute([
          originFk,
          row.editorFk || null,
          action,
          created,
          tableName,
          oldInstance,
          newI ? JSON.stringify(newI) : null,
          modelId,
          modelValue
        ]);
      }
    }
  }

  async flushQueue() {
    if (this.flushed) return;
    const position = this.nextPosition;

    if (position) {
      const filename = this.nextFilename;
      this.debug('Flush', `filename: ${filename}, position: ${position}`);
    
      const replaceQuery =
        'REPLACE INTO `util`.`binlogQueue` SET `code` = ?, `logName` = ?, `position` = ?';
      if (!this.conf.testMode)
        await this.db.query(replaceQuery, [this.conf.code, filename, position]);

      this.flushed = true;
    }

    this.nextFilename = this.filename;
    this.nextPosition = this.position;
  }

  async connectionPing() {
    this.debug('Ping', 'Sending ping to database.');

    // FIXME: Should Zongji.connection be pinged?
    await new Promise((resolve, reject) => {
      this.zongji.ctrlConnection.ping(err => {
        if (err) return reject(err);
        resolve();
      });
    })
    await this.db.ping();
  }

  debug(namespace, message) {
    if (this.conf.debug)
      console.debug(`${namespace}:`.blue, message.yellow);
  }
}

function equals(a, b) {
  if (a === b)
    return true;
  const type = typeof a;
  if (a == null || b == null || type !== typeof b)
    return false;
  if (type === 'object' && a.constructor === b.constructor) {
    if (a instanceof Date)
      return a.getTime() === b.getTime();
  }
  return false;
}
