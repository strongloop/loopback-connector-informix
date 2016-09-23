'use strict';

/*!
 * Informix connector for LoopBack
 */
var g = require('./globalize');
var SqlConnector = require('loopback-connector').SqlConnector;
var ParameterizedSQL = SqlConnector.ParameterizedSQL;
var Driver = require('ibm_db');
var util = require('util');
var debug = require('debug')('loopback:connector:informix');
var async = require('async');
var Transaction = require('loopback-connector').Transaction;

/**
 * Initialize the Informix connector for the given data source
 *
 * @param {DataSource} ds The data source instance
 * @param {Function} [cb] The cb function
 */
exports.initialize = function(ds, cb) {
  ds.connector = new Informix(ds.settings);
  ds.connector.dataSource = ds;
  if (cb) ds.connector.connect(cb);
};

/**
 * The constructor for the Informix LoopBack connector
 *
 * @param {Object} settings The settings object
 * @constructor
 */
function Informix(settings) {
  debug('Informix constructor settings: %j', settings);
  SqlConnector.call(this, 'informix', settings);
  this.driver = (settings.driver || 'INFORMIX 3.31 64 BIT');
  this.debug = (settings.debug || debug.enabled);
  this.useLimitOffset = settings.useLimitOffset || false;
  this.client = new Driver.Pool();
  this.dbname = (settings.database || settings.db || 'testdb');
  this.hostname = (settings.hostname || settings.host);
  this.username = (settings.username || settings.user);
  this.password = settings.password;
  this.portnumber = settings.port;
  this.protocol = (settings.protocol || 'onsoctcp');
  this.servername = (settings.servername || 'test_server');

  this.connStr =
    'DRIVER=' + this.driver +
    ';HOSTNAME=' + this.hostname +
    // ';SERVER=' + this.servername +
    ';PORT=' + this.portnumber +
    ';PROTOCOL=' + this.protocol +
    ';DATABASE=' + this.dbname +
    ';UID=' + this.username +
    ';PWD=' + this.password;
}

util.inherits(Informix, SqlConnector);

Informix.prototype.tableEscaped = function(model) {
  var escapedName = this.escapeName(this.table(model));
  return escapedName;
};

/**
 * Connect to Informix
 *
 * {Function} [cb] The callback after the connect
 */
Informix.prototype.connect = function(cb) {
  var self = this;

  if (self.hostname === undefined ||
      self.portnumber === undefined ||
      self.username === undefined ||
      self.password === undefined ||
      self.protocol === undefined) {
    g.log('Invalid connection string: %s', self.connStr);
    return (cb && cb());
  }

  self.dataSource.connecting = true;
  self.client.open(this.connStr, function(err, con) {
    debug('Informix.prototype.connect (%s) err=%j con=%j',
          self.connStr, err, con);
    if (err) {
      self.dataSource.connected = false;
      self.dataSource.connecting = false;
      self.dataSource.emit('error', err);
    } else {
      self.connection = con;
      self.dataSource.connected = true;
      self.dataSource.connecting = false;
      self.dataSource.emit('connected');
    }
    if (cb) cb(err, con);
  });
};

/**
 * Execute the sql statement
 *
 */
Informix.prototype.executeSQL = function(sql, params, options, callback) {
  debug('Informix.prototype.executeSQL (enter)',
        sql, params, options);
  var self = this;
  var conn = self.connection;

  if (options.transaction) {
    conn = options.transaction.connection;
  }

  conn.query(sql, params, function(err, data, more) {
    debug('Informix.prototype.executeSQL (exit)' +
          ' sql=%j params=%j err=%j data=%j more=%j',
          sql, params, err, data, more);

    if (!err) {
      if (more) {
        process.nextTick(function() {
          return callback(err, data);
        });
      }
    }

    if (callback) callback(err, data);
  });
};

/**
 * Escape an identifier such as the column name
 * Informix requires double quotes for case-sensitivity
 *
 * @param {string} name A database identifier
 * @returns {string} The escaped database identifier
 */
Informix.prototype.escapeName = function(name) {
  debug('Informix.prototype.escapeName name=%j', name);
  if (!name) return name;
  name.replace(/["]/g, '""');
  return '\"' + name + '\"';
  // return name;
};

function dateToInformix(val) {
  var dateStr = val.getFullYear() + '-' +
    fillZeros(val.getMonth() + 1) + '-' +
    fillZeros(val.getDate()) + ' ' +
    fillZeros(val.getHours()) + ':' +
    fillZeros(val.getMinutes()) + ':' +
    fillZeros(val.getSeconds()) + '.' +
    fillZeros(val.getMilliseconds());
  return dateStr;

  function fillZeros(v) {
    return v < 10 ? '0' + v : v;
  }
}

function dateFromInformix(val) {
  // 2016-04-05 12:46:48.310000
  var pattern = /(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{6})/;
  var dt = new Date(val.replace(pattern, '$1-$2-$3T$4.$5.$6'));
  return dt;
}

/**
 * Convert property name/value to an escaped DB column value
 *
 * @param {Object} prop Property descriptor
 * @param {*} val Property value
 * @returns {*} The escaped value of DB column
 */
Informix.prototype.toColumnValue = function(prop, val) {
  debug('Informix.prototype.toColumnValue prop=%j val=%j', prop, val);
  if (val === null) {
    if (prop.autoIncrement || prop.id) {
      return new ParameterizedSQL('?', [0]);
    }
    return null;
  }
  if (!prop) {
    return val;
  }
  switch (prop.type.name) {
    default:
    case 'Array':
    case 'Number':
    case 'String':
      return val;
    case 'Boolean':
      return Number(val);
    case 'GeoPoint':
    case 'Point':
    case 'List':
    case 'Object':
    case 'ModelConstructor':
      return JSON.stringify(val);
    case 'JSON':
      return String(val);
    case 'Date':
      return dateToInformix(val);
  }
};

/*!
 * Convert the data from database column to model property
 *
 * @param {object} Model property descriptor
 * @param {*) val Column value
 * @returns {*} Model property value
 */
Informix.prototype.fromColumnValue = function(prop, val) {
  debug('Informix.prototype.fromColumnValue %j %j', prop, val);
  if (!val || val === null || !prop) {
    return val;
  }
  switch (prop.type.name) {
    case 'Number':
      return Number(val);
    case 'String':
      return String(val);
    case 'Date':
      return new Date(val);
    case 'Boolean':
      return Boolean(val);
    case 'GeoPoint':
    case 'Point':
    case 'List':
    case 'Array':
    case 'Object':
    case 'JSON':
      return JSON.parse(val);
    default:
      return val;
  }
};

/**
 * Get the place holder in SQL for identifiers, such as ??
 *
 * @param {string} key Optional key, such as 1 or id
 */
Informix.prototype.getPlaceholderForIdentifier = function(key) {
  throw new Error(g.f('Placeholder for identifiers is not supported: %s',
    key));
};

/**
 * Get the place holder in SQL for values, such as :1 or ?
 *
 * @param {string} key Optional key, such as 1 or id
 * @returns {string} The place holder
 */
Informix.prototype.getPlaceholderForValue = function(key) {
  debug('Informix.prototype.getPlaceholderForValue key=%j', key);
  return '(?)';
};

/**
 * Build the `DELETE FROM` SQL statement
 *
 * @param {string} model The model name
 * @param {Object} where The where object
 * @param {Object} options Options object
 * @param {Function} callback function
 */
Informix.prototype.destroyAll = Informix.prototype.deleteAll = function(
  model, where, options, callback) {
  var self = this;
  var tableName = self.tableEscaped(model);
  var id = self.idName(model);
  var deleteStmt = self.buildDelete(model, where, options);
  var selectStmt = new ParameterizedSQL('SELECT COUNT(' + id + ')');
  var connection, currentTx, err;

  process.nextTick(function() {
    if (options.transaction) {
      connection = options.transaction.connection;
    } else {
      try {
        connection = Driver.openSync(self.connStr);
        currentTx = connection.beginTransactionSync();
      } catch (e) {
        return callback(Error(e));
      }
    }

    selectStmt.merge('as cnt FROM ' + tableName);
    selectStmt.merge(self.buildWhere(model, where));

    try {
      var selectData = connection.querySync(selectStmt.sql, selectStmt.params);

      var deleteData = connection.querySync(deleteStmt, null);

      if (currentTx) {
        connection.commitTransactionSync();
        connection.closeSync();
      }
    } catch (e) {
      if (currentTx) {
        connection.closeSync();
      }
      return callback(e);
    }

    callback(err, {count: selectData[0]['cnt']});
  });
};

/**
 * Build the clause for default values if the fields is empty
 *
 * @param {string} model The model name
 * @returns {string} default values statement
 */
Informix.prototype.buildInsertDefaultValues = function(model) {
  var self = this;
  var def = this.getModelDefinition(model);
  var num = Object.keys(def.properties).length;
  var result = [];
  var resultStr = '';
  Object.keys(def.properties).forEach(function(prop) {
    var p = self.getPropertyDefinition(model, prop);
    if (p.id && p.generated) {
      result.push(0);
    } else if (self.isNullable(p)) {
      result.push('null');
    }
  });

  resultStr = result.join(',');
  return 'VALUES(' + resultStr + ')';
};

/**
 * Create the table for the given model
 *
 * @param {string} model The model name
 * @param {Object} [options] options
 * @param {Function} [cb] The callback function
 */
Informix.prototype.createTable = function(model, options, cb) {
  debug('Informix.prototype.createTable ', model, options);
  cb();
};

Informix.prototype.buildIndex = function(model, property) {
  debug('Informix.prototype.buildIndex %j %j', model, property);
};

Informix.prototype.buildIndexes = function(model) {
  debug('Informix.prototype.buildIndexes %j', model);
};

/**
 * Create the data model in Informix
 *
 * @param {string} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options Options object
 * @param {Function} [callback] The callback function
 */
Informix.prototype.create = function(model, data, options, callback) {
  var self = this;
  var insertStmt = self.buildInsert(model, data, options);
  var id = self.idName(model);
  var tableName = self.tableEscaped(model);
  var selectStmt = 'SELECT MAX(' + id + ') as ' + id + ' FROM ' + tableName;
  var connection, currentTx;

  process.nextTick(function() {
    // If a transaction hasn't already been started, then start a local one now.
    // We will have to deal with cleaning this up in the event some error
    // occurs in the code below.
    if (options.transaction) {
      connection = options.transaction.connection;
    } else {
      try {
        connection = Driver.openSync(self.connStr);
        currentTx = connection.beginTransactionSync();
      } catch (e) {
        if (connection) {
          connection.closeSync();
        }
        return callback(Error(e));
      }
    }

    try {
      var insertData = connection.querySync(insertStmt.sql, insertStmt.params);
    } catch (e) {
      if (currentTx) {
        options.transaction = undefined;
        connection.close();
      }

      return callback(Error(e));
    }

    try {
      var selectData = connection.querySync(selectStmt, null);

      if (currentTx) {
        connection.commitTransactionSync();
        connection.closeSync();
      }
    } catch (e) {
      if (currentTx) {
        options.transaction = undefined;
        connection.closeSync();
      }

      return callback(Error(e));
    }

    callback(null, selectData[0][id]);
  });
};

/**
 * Update all instances that match the where clause with the given data
 *
 * @param {string} model The model name
 * @param {Object} where The where object
 * @param {Object} data The property/value object representing changes
 * to be made
 * @param {Object} options The options object
 * @param {Function} callback The callback function
 */
Informix.prototype.update = function(
  model, where, data, options, callback) {
  var self = this;
  var tableName = self.tableEscaped(model);
  var id = self.idName(model);
  var updateStmt = self.buildUpdate(model, where, data, options);
  var selectStmt = new ParameterizedSQL('SELECT COUNT(' + id + ')');
  var connection, currentTx, err;

  process.nextTick(function() {
    if (options.transaction) {
      connection = options.transaction.connection;
    } else {
      try {
        connection = Driver.openSync(self.connStr);
        currentTx = connection.beginTransactionSync();
      } catch (e) {
        return callback(Error(e));
      }
    }

    selectStmt.merge('as cnt FROM ' + tableName);
    selectStmt.merge(self.buildWhere(model, where));

    try {
      var selectData = connection.querySync(selectStmt.sql,
                                            selectStmt.params);
      var updateData = connection.querySync(updateStmt.sql,
                                            updateStmt.params);

      if (currentTx) {
        connection.commitTransactionSync();
        connection.closeSync();
      }
    } catch (e) {
      if (currentTx) {
        connection.closeSync();
      }
      return callback(e);
    }

    callback(err, {count: selectData[0]['cnt']});
  });
};

/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {string} model The model name
 * @param {Object} data The model instance data
 * @param {Function} [callback] The callback function
 */
Informix.prototype.updateOrCreate = Informix.prototype.save =
  function(model, data, options, callback) {
    var self = this;
    var id = self.idName(model);
    var tableName = self.tableEscaped(model);
    var insertColumns = [];
    var idData = {};
    var isInsert = false;
    var meta = {};
    var connection, currentTx;

    process.nextTick(function() {
      if (options.transaction) {
        connection = options.transaction.connection;
      } else {
        try {
          connection = Driver.openSync(self.connStr);
          currentTx = connection.beginTransactionSync();
        } catch (e) {
          return callback(Error(e));
        }
      }

      idData.id = data.id;

      try {
        var selectStmt = new ParameterizedSQL('SELECT COUNT(*) AS cnt FROM ');
        selectStmt.merge(tableName);
        selectStmt.merge(self.buildWhere(data));

        var selectInfo = connection.querySync(selectStmt.sql,
          selectStmt.params);

        var stmt;
        if (selectInfo[0]['cnt'] > 0) {
          stmt = self.buildUpdate(model, idData, data);
        } else {
          stmt = self.buildInsert(model, data);
          isInsert = true;
        }

        connection.querySync(stmt.sql, stmt.params);

        if (isInsert) {
          stmt = 'SELECT MAX(' + id + ') as id FROM ' + tableName;
          var rows = connection.querySync(stmt);
          data.id = rows[0][id];
        } else {
          meta.isNewInstance = false;
        }

        if (currentTx) {
          connection.commitTransactionSync();
          connection.closeSync();
        }
      } catch (e) {
        if (currentTx) {
          connection.closeSync();
        }
        return callback(Error(g.f('Failed')));
      }

      callback(null, data, meta);
    });
  };

/**
 * Count all model instances by the where filter
 *
 * @param {string} model The model name
 * @param {Object} where The where object
 * @param {Object} options The options object
 * @param {Function} cb The callback function
 */
Informix.prototype.count = function(model, where, options, cb) {
  var self = this;
  if (typeof where === 'function') {
    // Backward compatibility for 1.x style signature:
    // count(model, cb, where)
    var tmp = options;
    cb = where;
    where = tmp;
  }

  // The only reason for this function is that the base loopback-connector
  // uses the escaped "cnt" alias which Informix doesn't like.  The proper
  // fix for this is to change loopback-connector to call this.escapeName
  // to properly escape for each connector.
  var stmt = new ParameterizedSQL('SELECT COUNT(*) as cnt FROM ' +
    self.tableEscaped(model));
  stmt = stmt.merge(self.buildWhere(model, where));
  stmt = self.parameterize(stmt);
  self.execute(stmt.sql, stmt.params,
    function(err, res) {
      if (err) {
        return cb(err);
      }
      var c = (res && res[0] && res[0].cnt) || 0;
      // Some drivers return count as a string to contain bigint
      // See https://github.com/brianc/node-postgres/pull/427
      cb(err, Number(c));
    });
};

function buildLimit(limit, offset) {
  if (isNaN(limit)) { limit = 0; }
  if (isNaN(offset)) { offset = 0; }
  if (!limit && !offset) {
    return '';
  }
  if (limit && !offset) {
    return 'FIRST ' + limit + ' ';
  }
  if (offset && !limit) {
    return 'SKIP ' + offset;
  }
  return 'SKIP ' + offset + ' LIMIT ' + limit + ' ';
}

Informix.prototype.applyPagination = function(model, stmt, filter) {
  debug('Informix.prototype.applyPagination');
  var limitClause = buildLimit(filter.limit, filter.offset || filter.skip);
  return (stmt + limitClause);
  // return limitClause;
};

Informix.prototype.getCountForAffectedRows = function(model, info) {
  var affectedRows = info && typeof info.affectedRows === 'number' ?
      info.affectedRows : undefined;
  return affectedRows;
};

/**
 * Build a SQL SELECT statement
 *
 * @param {string} model Model name
 * @param {Object} filter Filter object
 * @param {Object} options Options object
 * @returns {ParameterizedSQL} Statement object {sql: ..., params: [...]}
 */
Informix.prototype.buildSelect = function(model, filter, options) {
  if (!filter.order) {
    var idNames = this.idNames(model);
    if (idNames && idNames.length) {
      filter.order = idNames;
    }
  }

  var stmt = 'SELECT ';
  if (filter.limit || filter.skip || filter.offset) {
    stmt = this.applyPagination(model, stmt, filter);
  }

  var selectStmt = new ParameterizedSQL(stmt +
    this.buildColumnNames(model, filter) +
    ' FROM ' + this.tableEscaped(model)
  );

  if (filter) {
    if (filter.where) {
      var whereStmt = this.buildWhere(model, filter.where);
      selectStmt.merge(whereStmt);
    }

    if (filter.order) {
      selectStmt.merge(this.buildOrderBy(model, filter.order));
    }
  }
  return this.parameterize(selectStmt);
};

/**
 * Drop the table for the given model from the database
 *
 * @param {string} model The model name
 * @param {Function} [cb] The callback function
 */
Informix.prototype.dropTable = function(model, cb) {
  var self = this;
  var sql = 'DROP TABLE IF EXISTS ' + self.tableEscaped(model);
  self.execute(sql, cb);
};

Informix.prototype.createTable = function(model, cb) {
  var self = this;
  var tableName = self.tableEscaped(model);
  // var tableSchema = self.schema;
  var columnDefinitions = self.buildColumnDefinitions(model);
  var tasks = [];

  if (self.supportColumnStore && self.supportColumnStore === true) {
    return cb(new Error(g.f('Column organized tables are not ' +
                        'currently supported')));
  } else {
    tasks.push(function(callback) {
      var sql = 'CREATE TABLE ' + /* tableSchema + '.' + */ tableName +
          ' (' + columnDefinitions + ');';
      self.execute(sql, callback);
    });
  }

  var indexes = self.buildIndexes(model);
  indexes.forEach(function(i) {
    tasks.push(function(callback) {
      self.execute(i, callback);
    });
  });

  async.series(tasks, cb);
};

Informix.prototype.buildColumnDefinitions = function(model) {
  var self = this;
  var sql = [];
  var definition = this.getModelDefinition(model);
  var pks = this.idNames(model).map(function(i) {
    return self.columnEscaped(model, i);
  });
  Object.keys(definition.properties).forEach(function(prop) {
    var colName = self.columnEscaped(model, prop);
    sql.push(colName + ' ' + self.buildColumnDefinition(model, prop));
  });

  return sql.join(',');
};

Informix.prototype.buildIndex = function(model, property) {
  var self = this;
  var prop = self.getPropertyDefinition(model, property);
  var i = prop && prop.index;
  if (!i) {
    return '';
  }

  var stmt = 'CREATE ';
  var kind = '';
  if (i.kind) {
    kind = i.kind;
  }
  var columnName = self.columnEscaped(model, property);
  if (typeof i === 'object' && i.unique && i.unique === true) {
    kind = 'UNIQUE';
  }
  return (stmt + kind + ' INDEX ' + columnName +
          ' ON ' /* + self.schema + '.' */ + self.tableEscaped(model) +
          ' (' + columnName + ');');
};

Informix.prototype.buildIndexes = function(model) {
  var indexClauses = [];
  var definition = this.getModelDefinition(model);
  var indexes = definition.settings.indexes || {};
  // Build model level indexes
  for (var index in indexes) {
    var i = indexes[index];
    var stmt = 'CREATE ';
    var kind = '';
    if (i.kind) {
      kind = i.kind;
    }
    var indexedColumns = [];
    var indexName = this.escapeName(index);
    if (Array.isArray(i.keys)) {
      indexedColumns = i.keys.map(function(key) {
        return this.columnEscaped(model, key);
      });
    }

    var columns = (i.columns.split(/,\s*/)).join('\",\"');
    if (indexedColumns.length > 0) {
      columns = indexedColumns.join('\",\"');
    }

    indexClauses.push(stmt + kind + ' INDEX ' + indexName +
      ' ON ' /* + this.schema + '.' */ + this.tableEscaped(model) +
      ' (\"' + columns + '\");');
  }

  return indexClauses;
};

Informix.prototype.buildColumnDefinition = function(model, prop) {
  // var p = this.getModelDefinition(model).properties[prop];
  var p = this.getPropertyDefinition(model, prop);
  if (p.id && p.generated) {
    return 'SERIAL NOT NULL PRIMARY KEY';
  }
  var line = this.columnDataType(model, prop) + ' ' +
        ((this.isNullable(p)) ? '' : 'NOT NULL');
  return line;
};

Informix.prototype.columnDataType = function(model, property) {
  var prop = this.getPropertyDefinition(model, property);
  if (!prop) {
    return null;
  }
  return this.buildColumnType(prop);
};

Informix.prototype.buildColumnType = function buildColumnType(propDefinition) {
  var self = this;
  var dt = '';
  var p = propDefinition;
  var type = p.type.name;

  switch (type) {
    default:
    case 'Any':
    case 'Text':
    case 'String':
    case 'Object':
      dt = self.convertTextType(p, 'LVARCHAR');
      break;
    case 'JSON':
      dt = 'JSON';
      break;
    case 'Number':
      dt = self.convertNumberType(p, 'INTEGER');
      break;
    case 'Date':
      dt = 'DATETIME YEAR TO FRACTION';
      break;
    case 'Boolean':
      dt = 'SMALLINT';
      break;
    case 'Point':
    case 'GeoPoint':
      dt = 'POINT';
      break;
    case 'Enum':
      dt = 'ENUM(' + p.type._string + ')';
      dt = stringOptions(p, dt);
      break;
  }
  debug('Informix.prototype.buildColumnType %j %j', p.type.name, dt);
  return dt;
};

Informix.prototype.convertTextType = function convertTextType(p, defaultType) {
  var self = this;
  var dt = defaultType;
  var len = p.length ||
    ((p.type !== String) ? 4096 : p.id ? 255 : 255);

  if (p[self.name]) {
    if (p[self.name].dataLength) {
      len = p[self.name].dataLength;
    }
  }

  if (p[self.name] && p[self.name].dataType) {
    dt = String(p[self.name].dataType);
  } else if (p.dataType) {
    dt = String(p.dataType);
  }

  dt += '(' + len + ')';

  stringOptions(p, dt);

  return dt;
};

Informix.prototype.convertNumberType = function convertNumberType(p, defType) {
  var self = this;
  var dt = defType;
  var precision = p.precision;
  var scale = p.scale;

  if (p[self.name] && p[self.name].dataType) {
    dt = String(p[self.name].dataType);
    precision = p[self.name].dataPrecision;
    scale = p[self.name].dataScale;
  } else if (p.dataType) {
    dt = String(p.dataType);
  } else {
    return dt;
  }

  switch (dt) {
    case 'DECIMAL':
      dt = 'DECIMAL';
      if (precision && scale) {
        dt += '(' + precision + ',' + scale + ')';
      } else if (scale > 0) {
        throw new Error(g.f('Scale without Precision does not make sense'));
      }
      break;
    default:
      break;
  }

  return dt;
};

function stringOptions(p, columnType) {
  if (p.charset) {
    columnType += ' CHARACTER SET ' + p.charset;
  }
  if (p.collation) {
    columnType += ' COLLATE ' + p.collation;
  }
  return columnType;
}

require('./migration')(Informix);
require('./discovery')(Informix);
require('./transaction')(Informix);
