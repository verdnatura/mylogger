require('require-yaml');
require('colors');
const fs = require('fs');
const path = require('path');
const ZongJi = require('./zongji');
const mysql = require('mysql2/promise');

const catchEvents = new Set([
  'writerows',
  'updaterows',
  'deleterows'
]);

const actions = {
  writerows: 'insert',
  updaterows: 'update',
  deleterows: 'delete'
};

module.exports = class MyLogger {
  constructor() {
    this.running = false;
    this.binlogName = null;
    this.binlogPosition = null;
    this.schemaMap = new Map();
    this.logMap = new Map();
    this.isFlushed = true;
    this.queue = [];
  }

  async start() {
    const defaultConfig = require('./config.yml');
    const conf = this.conf = Object.assign({}, defaultConfig);
    const localPath = path.join(__dirname, 'config.local.yml');
    if (fs.existsSync(localPath)) {
      const localConfig = require(localPath);
      Object.assign(conf, localConfig);
    }

    const defaultSchema = conf.srcDb.database;
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

    const db = this.db = await mysql.createConnection(conf.dstDb);
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
        if (!tableInfo.exclude.has(col) && col != 'editorFk')
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

        if (!relations.length)
          throw new Error(`No relation to main table found for table: ${schema}.${table}`);
        if (relations.length > 1)
          throw new Error(`Found more multiple relations to main table: ${schema}.${table}`);
  
        for (const {relation} of relations)
          tableInfo.relation = relation;
      }
    }

    // Zongji

    const zongji = new ZongJi(conf.srcDb);
    this.zongji = zongji;

    this.onBinlogListener = evt => this.onBinlog(evt);
    zongji.on('binlog', this.onBinlogListener);

    const [res] = await db.query(
      'SELECT `logName`, `position` FROM `util`.`binlogQueue` WHERE code = ?',
      [conf.code]
    );
    if (res.length) {
      const [row] = res;
      this.binlogName = row.logName;
      this.binlogPosition = row.position;
      Object.assign(this.opts, {
        filename: row.logName,
        position: row.position
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
    clearInterval(this.flushTimeout);
    await this.flushQueue();

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

  async onBinlog(evt) {
    //evt.dump();
    try {
      let shouldFlush;
      const eventName = evt.getEventName();

      if (eventName == 'rotate') {
        if (evt.binlogName !== this.binlogName) {
          shouldFlush = true;
          this.binlogName = evt.binlogName;
          this.binlogPosition = evt.position;
          console.log(
            `[${eventName}] filename: ${this.binlogName}`,
            `position: ${this.binlogPosition}`
          );
        }
      } else {
        shouldFlush = true;
        this.binlogPosition = evt.nextPosition;
        if (catchEvents.has(eventName))
          this.onRowEvent(evt, eventName);
      }

      if (shouldFlush) this.isFlushed = false;
    } catch(err) {
      this.handleError(err);
    }
  }

  onRowEvent(evt, eventName) {
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

    this.queue.push({
      tableInfo,
      action,
      evt,
      changes,
      tableName,
      binlogName: this.binlogName
    });
    if (!this.flushTimeout)
      this.flushTimeout = setTimeout(
        () => this.flushQueue(),
        this.conf.queueFlushDelay
      );
  }

  async flushQueue() {
    if (this.isFlushed || this.isFlushing) return;
    this.isFlushing = true;
    const {conf, db} = this;

    try {
      if (this.queue.length) {
        do {
          let appliedOps;
          try {
            await db.query('START TRANSACTION');
            let op;
            appliedOps = [];

            for (let i = 0; i < conf.maxBulkLog && this.queue.length; i++) {
              op = this.queue.shift();
              appliedOps.push(op);
              await this.applyOp(op);
            }

            await this.savePosition(op.binlogName, op.evt.nextPosition)
            await db.query('COMMIT');
          } catch(err) {
            this.queue = appliedOps.concat(this.queue);
            await db.query('ROLLBACK');
            throw err;
          }
        } while (this.queue.length);
      } else {
        await this.savePosition(this.binlogName, this.binlogPosition);
      }
    } catch(err) {
      this.handleError(err);
    } finally {
      this.flushTimeout = null;
      this.isFlushing = false;
    }
  }

  async savePosition(binlogName, binlogPosition) {
    this.debug('Flush', `filename: ${binlogName}, position: ${binlogPosition}`);
        
    const replaceQuery =
      'REPLACE INTO `util`.`binlogQueue` SET `code` = ?, `logName` = ?, `position` = ?';
    if (!this.conf.testMode)
      await this.db.query(replaceQuery, [this.conf.code, binlogName, binlogPosition]);

    this.isFlushed = this.binlogName == binlogName
      && this.binlogPosition == binlogPosition;
  }

  handleError(err) {
    console.error('Super error:', err);
  }

  async applyOp(op) {
    const {
      tableInfo,
      action,
      evt,
      changes,
      tableName
    } = op;

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