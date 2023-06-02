require('require-yaml');
require('colors');
const ZongJi = require('./zongji');
const mysql = require('mysql2/promise');
const {loadConfig} = require('./lib/util');
const ModelLoader = require('./lib/model-loader');

module.exports = class MyLogger {
  constructor() {
    this.running = false;
    this.isOk = null;
    this.binlogName = null;
    this.binlogPosition = null;
    this.isFlushed = true;
    this.queue = [];
    this.modelLoader = new ModelLoader();
  }

  async start() {
    const conf = this.conf = loadConfig(__dirname, 'config');

    const {logMap, schemaMap} = this.modelLoader.init(conf);
    Object.assign(this, {logMap, schemaMap});

    const includeSchema = {};
    for (const [schemaName, tableMap] of this.schemaMap)
      includeSchema[schemaName] = Array.from(tableMap.keys());
  
    this.zongjiOpts = {
      includeEvents: [
        'rotate',
        'tablemap',
        'writerows',
        'updaterows',
        'deleterows'
      ],
      includeSchema,
      serverId: conf.serverId
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
            AND action = 'delete'
            AND (originFk IS NULL OR originFk = ?)
          LIMIT 1`
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

    await this.modelLoader.loadSchema(this.schemaMap, db);

    // Zongji

    this.onBinlogListener = evt => this.onBinlog(evt);

    const [res] = await db.query(
      'SELECT `logName`, `position` FROM `util`.`binlogQueue` WHERE code = ?',
      [conf.code]
    );
    if (res.length) {
      const [row] = res;
      this.binlogName = row.logName;
      this.binlogPosition = row.position;
    }

    await this.zongjiStart();

    this.flushInterval = setInterval(
      () => this.flushQueue(), conf.flushInterval * 1000);
    this.pingInterval = setInterval(
      () => this.connectionPing(), conf.pingInterval * 1000);

    // Summary

    this.running = true;
    this.isOk = true;
    this.debug('MyLogger', 'Initialized.');
  }

  async end(silent) {
    if (!this.running) return;
    this.running = false;
    this.debug('MyLogger', 'Ending.');
  
    this.db.off('error', this.onErrorListener);

    clearInterval(this.flushInterval);
    clearInterval(this.pingInterval);
    clearInterval(this.flushTimeout);

    function logError(err) {
      if (!silent) console.error(err);
    }

    try {
      await this.flushQueue();
    } catch (err) {
      logError(err);
    }

    // Zongji

    await this.zongjiStop();
    this.zongji = null;

    // DB connection

    // FIXME: mysql2/promise bug, db.end() ends process
    this.db.on('error', () => {});
    try {
      this.db.end();
    } catch (err) {
      logError(err);
    }

    // Summary

    this.debug('MyLogger', 'Ended.');
  }

  async zongjiStart() {
    await this.zongjiStop();
    const zongji = new ZongJi(this.conf.srcDb);
    const zongjiOpts = this.zongjiOpts;

    if (this.binlogName) {
      this.debug('Zongji',
        `Starting: ${this.binlogName}, position: ${this.binlogPosition}`
      );
      Object.assign(zongjiOpts, {
        filename: this.binlogName,
        position: this.binlogPosition
      });
    } else {
      this.debug('Zongji', 'Starting at end.');
      zongjiOpts.startAtEnd = true;
    }

    zongji.on('binlog', this.onBinlogListener);

    await new Promise((resolve, reject) => {
      const onReady = () => {
        zongji.off('error', onError);
        resolve();
      };
      const onError = err => {
        zongji.off('ready', onReady);
        zongji.off('binlog', this.onBinlogListener);
        reject(err);
      }

      zongji.once('ready', onReady);
      zongji.once('error',  onError);
      zongji.start(zongjiOpts);
    });
    zongji.on('error', this.onErrorListener);
    this.zongji = zongji;
    this.debug('Zongji', 'Started.');
  }

  async zongjiStop() {
    if (!this.zongji) return;
    this.debug('Zongji',
      `Stopping: ${this.binlogName}, position: ${this.binlogPosition}`
    );
    const zongji = this.zongji;
    this.zongji = null;

    zongji.off('binlog', this.onBinlogListener);
    zongji.off('error', this.onErrorListener);

    // FIXME: Cannot call Zongji.stop(), it doesn't wait to end connection
    zongji.connection.destroy(() => {
      console.log('zongji.connection.destroy');
    });
    await new Promise(resolve => {
      zongji.ctrlConnection.query('KILL ?', [zongji.connection.threadId],
      err => {
        if (err && err.code !== 'ER_NO_SUCH_THREAD')
          logError(err);
        resolve();
      });
    });
    zongji.ctrlConnection.destroy(() => {
      console.log('zongji.ctrlConnection.destroy');
    });
    zongji.emit('stopped');
    this.debug('Zongji', 'Stopped.');
  }

  async tryRestart() {
    try {
      await this.init();
      console.log('Process restarted.');
    } catch(err) {
      setTimeout(() => this.tryRestart(), this.conf.restartTimeout * 1000);
    }
  }

  async onError(err) {
    if (!this.isOk) return;
    this.isOk = false;
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

  handleError(err) {
    console.error(err);
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

      if (this.queue.length > this.conf.maxQueueEvents) {
        this.debug('MyLogger', 'Queue full, stopping Zongji.');
        await this.zongjiStop();
      }
    } catch(err) {
      this.handleError(err);
    }
  }

  onRowEvent(evt, eventName) {
    const table = evt.tableMap[evt.tableId];
    const tableName = table.tableName;
    const tableInfo = this.schemaMap
      .get(table.parentSchema)?.get(tableName);
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

    function equals(a, b) {
      if (a === b)
        return true;
      const type = typeof a;
      if (a == null || b == null || type !== typeof b)
        return false;
      if (type === 'object' && a.constructor === b.constructor) {
        if (a instanceof Date) {
          // FIXME: zongji creates invalid dates for NULL DATE
          // Error is somewhere here: zongji/lib/rows_event.js:129
          let aTime = a.getTime();
          if (isNaN(aTime)) aTime = null;
          let bTime = b.getTime();
          if (isNaN(bTime)) bTime = null;
    
          return aTime === bTime;
        }
      }
      return false;
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

    this.queue.push({
      tableInfo,
      action,
      evt,
      changes,
      binlogName: this.binlogName
    });

    if (!this.flushTimeout)
      this.flushTimeout = setTimeout(
        () => this.flushQueue(),
        this.conf.queueFlushDelay
      );

    if (this.conf.debug)
      console.debug('Evt:'.blue,
        `[${action}]`[actionColor[action]],
        `${tableName}: ${changes.length} changes, queue: ${this.queue.length} elements`
      );
  }

  async flushQueue() {
    if (this.isFlushed || this.isFlushing || !this.isOk) return;
    this.isFlushing = true;
    const {conf, db, queue} = this;
    let op;

    try {
      if (queue.length) {
        do {
          const ops = [];
          let txStarted;
          try {
            await db.query('START TRANSACTION');
            txStarted = true;

            for (let i = 0; i < conf.maxBulkLog && queue.length; i++) {
              op = queue.shift();
              ops.push(op);
              await this.applyOp(op);
            }

            this.debug('Queue', `applied: ${ops.length}, remaining: ${queue.length}`);
            await this.savePosition(op.binlogName, op.evt.nextPosition)
            await db.query('COMMIT');
          } catch(err) {
            queue.unshift(...ops);
            if (txStarted)
              try {
                await db.query('ROLLBACK');
              } catch (err) {}
            throw err;
          }
        } while (queue.length);

        if (!this.zongji) {
          this.debug('MyLogger', 'Queue flushed, restarting Zongji.');
          await this.zongjiStart();
        }
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

  async applyOp(op) {
    const {conf} = this;
    const {
      tableInfo,
      action,
      evt,
      changes
    } = op;

    const logInfo = tableInfo.log;
    const isDelete = action == 'delete';
    const isUpdate = action == 'update';
    const isMain = tableInfo.isMain;
    const relation = tableInfo.relation;

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

      const created = new Date(evt.timestamp);
      const modelName = tableInfo.modelName;
      const modelId = row[tableInfo.idName];
      const modelValue = tableInfo.showField && !isMain
        ? row[tableInfo.showField] || null
        : null;
      const oldInstance = oldI ? JSON.stringify(oldI) : null;
      const originFk = !isMain ? row[relation] : modelId;
      const originChanged = isUpdate && !isMain
        && newI[relation] !== undefined;

      let deleteRow;
      if (conf.debug)
        console.debug('Log:'.blue,
          `[${action}]`[actionColor[action]],
          `${logInfo.name}: ${originFk}, ${modelName}: ${modelId}`
        );

      try {
        if (isDelete) {
          [[deleteRow]] = await logInfo.fetchStmt.execute([
            modelName, modelId, originFk
          ]);
          if (!conf.testMode && deleteRow)
            await logInfo.updateStmt.execute([
              originFk,
              created,
              oldInstance,
              modelValue,
              deleteRow.id
            ]);
        }
        if (!conf.testMode && (!isDelete || !deleteRow)) {
          async function log(originFk) {
            if (originFk == null) return;
            await logInfo.addStmt.execute([
              originFk,
              row[tableInfo.userField] || null,
              action,
              created,
              modelName,
              oldInstance,
              newI ? JSON.stringify(newI) : null,
              modelId,
              modelValue
            ]);
          }

          await log(originFk);
          if (originChanged)
            await log(oldI[relation]);
        }
      } catch (err) {
        if (err.code == 'ER_NO_REFERENCED_ROW_2') {
          this.debug('Log', `Ignored because of constraint failed.`);
        } else
          throw err;
      }
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

  async connectionPing() {
    if (!this.isOk) return;
    try {
      this.debug('Ping', 'Sending ping to database.');

      if (this.zongji) {
        // FIXME: Should Zongji.connection be pinged?
        await new Promise((resolve, reject) => {
          this.zongji.ctrlConnection.ping(err => {
            if (err) return reject(err);
            resolve();
          });
        })
      }

      await this.db.ping();
    } catch(err) {
      this.handleError(err);
    }
  }

  debug(namespace, message) {
    if (this.conf.debug)
      console.debug(`${namespace}:`.blue, message.yellow);
  }
}

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

const actionColor = {
  insert: 'green',
  update: 'yellow',
  delete: 'red'
};
