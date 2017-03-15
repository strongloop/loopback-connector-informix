// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-informix
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

/*!
 * Informix connector for LoopBack
 */
var g = require('./globalize');
var SqlConnector = require('loopback-connector').SqlConnector;
var ParameterizedSQL = SqlConnector.ParameterizedSQL;
var IBMDB = require('loopback-ibmdb').IBMDB;
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

  cb();
};

/**
 * The constructor for the Informix LoopBack connector
 *
 * @param {Object} settings The settings object
 * @constructor
 */
function Informix(settings) {
  debug('Informix constructor settings: %j', settings);
  IBMDB.call(this, 'Informix', settings);
};

util.inherits(Informix, IBMDB);

Informix.prototype.setConnectionProperties = function(name, settings) {
  var self = this;
  self.dbname = (settings.database || settings.db || 'testdb');
  self.dsn = settings.dsn;
  self.hostname = (settings.hostname || settings.host);
  self.username = (settings.username || settings.user);
  self.password = settings.password;
  self.portnumber = settings.port;
  self.protocol = (settings.protocol || 'TCPIP');

  // Save off the connectionOptions passed in for connection pooling
  self.connectionOptions = {};
  self.connectionOptions.minPoolSize = parseInt(settings.minPoolSize, 10) || 0;
  self.connectionOptions.maxPoolSize = parseInt(settings.maxPoolSize, 10) || 0;
  self.connectionOptions.connectionTimeout =
    parseInt(settings.connectionTimeout, 10) || 60;

  var dsn = settings.dsn;
  if (dsn) {
    self.connStr = dsn;

    var DSNObject = self.parseDSN(dsn);
    self.schema = DSNObject.CurrentSchema || DSNObject.UID;
  } else {
    var connStrGenerate =
      'DRIVER={' + name + '}' +
      ';DATABASE=' + this.dbname +
      ';HOSTNAME=' + this.hostname +
      ';UID=' + this.username +
      ';PWD=' + this.password +
      ';PORT=' + this.portnumber +
      ';PROTOCOL=' + this.protocol +
      ';AUTHENTICATION=SERVER';
    self.connStr = connStrGenerate;

    self.schema = this.username;
    if (settings.schema) {
      self.schema = settings.schema.toUpperCase();
    }
  }
};

/**
 * Escape the table name.  For informix this is a no-op as
 * the SQL processing doesn't accept escaped names.  This
 * function is maintained for consistency with other
 * connectors.
 */
Informix.prototype.tableEscaped = function(model) {
  var escapedName = this.escapeName(this.table(model));
  return escapedName;
};

/**
 * Ping function to used to validate connections to an
 * Informix database.
 */
Informix.prototype.ping = function(cb) {
  debug('Informix.prototype.ping');
  var self = this;
  var sql = 'SELECT COUNT(*) AS COUNT FROM SYSTABLES';

  if (self.dataSource.connection) {
    ping(self.dataSource.connection, cb);
  } else {
    self.connect(function(err, conn) {
      if (err) {
        return cb(err);
      }
      ping(conn, function(err, res) {
        conn.close(function(cerr) {
          if (err || cerr) {
            return cb(err || cerr);
          }
          return cb(null, res);
        });
      });
    });
  }

  function ping(conn, cb) {
    conn.query(sql, function(err, rows) {
      if (err) {
        return cb(err);
      }
      cb(null, rows.length > 0 && rows[0]['COUNT'] > 0);
    });
  }
};

/**
 * Escape an identifier such as the column name
 * Informix requires double quotes for case-sensitivity
 *
 * @param {string} name A database identifier
 * @returns {string} The escaped database identifier
 */
Informix.prototype.escapeName = function(name) {
  if (!name) return name;
  name.replace(/["]/g, '""');
  return name;
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
Informix.prototype.destroyAll = function(
  model, where, options, callback) {
  debug('Informix.prototype.destroyAll %j %j %j', model, where, options);
  var self = this;
  var tableName = self.tableEscaped(model);
  var id = self.idName(model);
  var deleteStmt = self.buildDelete(model, where, options);
  var selectStmt = new ParameterizedSQL('SELECT ' + id + ' FROM ' +
                                        tableName);
  selectStmt.merge(self.buildWhere(model, where));
  selectStmt.merge(' FOR UPDATE');
  self.parameterize(selectStmt);

  var executeTransaction = function(connection, cb) {
    connection.query(selectStmt, function(err, selectData) {
      debug('Informix.prototype.destroyAll stmt: %j data: %j', selectStmt,
             selectData);
      if (err) {
        return cb(err);
      }

      connection.query(deleteStmt, null, function(err, deleteData) {
        debug('Informix.prototype.destroyAll stmt: %j data: %j', deleteStmt,
              deleteData);
        if (err) {
          return cb(err);
        }

        return cb(null, {'count': selectData.length});
      });
    });
  };

  // If a transaction hasn't already been started, then start a local one now.
  // We will have to deal with cleaning this up in the event some error
  // occurs in the code below.
  if (options.transaction) {
    executeTransaction(options.transaction.connection, function(err, retVal) {
      if (err) {
        return callback(err);
      }
      callback(null, data);
    });
  } else {
    self.beginTransaction(Transaction.REPEATABLE_READ, function(err, conn) {
      if (err) {
        return callback(err);
      } else {
        executeTransaction(conn, function(err, data) {
          if (err) {
            self.rollback(conn, function(err) {});
            conn.close(function(err) {});
            return callback(err);
          }

          self.commit(conn, function(err) {});
          callback(null, data);
        });
      }
    });
  }
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
  debug('Informix.prototype.create %j %j, %j', model, data, options);
  var self = this;
  var insertStmt = self.buildInsert(model, data);

  var executeTransaction = function(connection, cb) {
    connection.query(insertStmt, options, function(err, data) {
      if (err) {
        cb(err);
      } else {
        connection.query('SELECT DBINFO (\'sqlca.sqlerrd1\') as id ' +
                     'FROM systables ' +
                     'WHERE tabid = 1', function(err, info) {
          if (err) {
            cb(err);
          } else {
            cb(err, info[0].id);
          }
        });
      }
    });
  };

  // If a transaction hasn't already been started, then start a local one now.
  // We will have to deal with cleaning this up in the event some error
  // occurs in the code below.
  if (options.transaction) {
    executeTransaction(options.transaction.connection, function(err, retVal) {
      if (err) {
        return callback(err);
      }
      callback(null, retVal);
    });
  } else {
    self.beginTransaction(Transaction.REPEATABLE_READ, function(err, conn) {
      if (err) {
        return callback(err);
      } else {
        executeTransaction(conn, function(err, retVal) {
          if (err) {
            self.rollback(conn, function(err) {});
            conn.close(function(err) {});
            return callback(err);
          }

          self.commit(conn, function(err) {});
          callback(null, retVal);
        });
      }
    });
  }
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
  var selectStmt = new ParameterizedSQL('SELECT ' + id + ' FROM ' +
                                        tableName);

  selectStmt.merge(self.buildWhere(model, where));
  selectStmt.merge(' FOR UPDATE');
  self.parameterize(selectStmt);

  var updateData;

  var executeTransaction = function(connection, cb) {
    connection.query(selectStmt.sql, selectStmt.params, function(err, data) {
      debug('Informix.prototype.update stmt: %j data: %j', selectStmt, data);
      if (err) {
        return cb(err);
      }

      connection.query(updateStmt.sql, updateStmt.params,
        function(err, updateData) {
          debug('Informix.prototype.update stmt: %j data: %j', updateStmt,
                updateData);
          if (err) {
            return cb(err);
          }

          return cb(null, data.length);
        });
    });
  };

  // If a transaction hasn't already been started, then start a local one now.
  // We will have to deal with cleaning this up in the event some error
  // occurs in the code below.
  if (options.transaction) {
    executeTransaction(options.transaction.connection, function(err, retVal) {
      if (err) {
        return callback(err);
      }
      callback(null, {count: retVal});
    });
  } else {
    self.beginTransaction(Transaction.REPEATABLE_READ, function(err, conn) {
      if (err) {
        return callback(err);
      } else {
        executeTransaction(conn, function(err, retVal) {
          if (err) {
            self.rollback(conn, function(err) {});
            conn.close(function(err) {});
            return callback(err);
          }
          self.commit(conn, function(err) {});
          callback(null, {count: retVal});
        });
      }
    });
  }
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
};

Informix.prototype.buildReplace = function(model, where, data, options) {
  process.nextTick(function() {
    throw Error(g.f('Function {{buildReplace}} not supported'));
  });
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

Informix.prototype.buildColumnDefinitions = function(model) {
  debug('Informix.prototype.buildColumnDefinitions');
  var self = this;
  var sql = [];
  var definition = self.getModelDefinition(model);
  var pks = self.idNames(model).map(function(i) {
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
  debug('Informix.prototype.buildColumnDefinition %j', prop);
  var p = this.getPropertyDefinition(model, prop);
  if (p.id && p.generated) {
    return 'SERIAL NOT NULL PRIMARY KEY';
  }
  var line = this.columnDataType(model, prop) + ' ' +
        ((this.isNullable(p)) ? '' : 'NOT NULL');
  return line;
};

Informix.prototype.columnDataType = function(model, property) {
  debug('Informix.prototype.columnDataType %j', property);
  var prop = this.getPropertyDefinition(model, property);
  if (!prop) {
    return null;
  }
  return this.buildColumnType(prop);
};

Informix.prototype.buildColumnType = function buildColumnType(propDefinition) {
  debug('Informix.prototype.buildColumnType %j', propDefinition);
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

/**
 * Build the clause for default values if the fields is empty
 *
 * @param {string} model The model name
 * @returns {string} default values statement
 */
Informix.prototype.buildInsertDefaultValues = function(model) {
  debug('IBMDB.prototype.buildInsertDefaultValues');
  var def = this.getModelDefinition(model);
  var values = [];
  var self = this;
  var result = '';

  Object.keys(def.properties).forEach(function(prop) {
    var p = self.getPropertyDefinition(model, prop);
    if (p.id && p.generated) {
      values.push('0');
    } else {
      values.push('NULL');
    }
  });
  result = 'VALUES( ' + values.join(', ') + ' )';
  return result;
};

/**
 * Transform the row data into a model data object
 * @param {string} model Model name
 * @param {object} rowData An object representing the row data from DB
 * @returns {object} Model data object
 */
Informix.prototype.fromRow = Informix.prototype.fromDatabase =
function(model, rowData) {
  if (rowData == null) {
    return rowData;
  }
  var props = this.getModelDefinition(model).properties;
  var data = {};
  for (var p in props) {
    var columnName = this.column(model, p);
    var columnValue = '';
    // Load properties from the row
    if (p === columnName) {
      columnValue = this.fromColumnValue(props[p],
                                         rowData[columnName.toLowerCase()]);
    } else {
      columnValue = this.fromColumnValue(props[p], rowData[columnName]);
    }

    if (columnValue !== undefined) {
      data[p] = columnValue;
    }
  }
  return data;
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
    debug('Informix.prototype.updateOrCreate (enter): model=%j, data=%j, ' +
          'options=%j ', model, data, options);
    var self = this;
    var idName = self.idName(model);
    var stmt;
    var tableName = self.tableEscaped(model);
    var meta = {};

    function executeWithConnection(connection, cb) {
      // Execution for updateOrCreate requires running two
      // separate SQL statements.  The second depends on the
      // result of the first.
      var where = {};
      where[idName] = data[idName];

      var countStmt = new ParameterizedSQL('SELECT COUNT(*) AS CNT FROM ');
      countStmt.merge(tableName);
      countStmt.merge(self.buildWhere(model, where));
      countStmt.noResults = false;

      connection.query(countStmt, function(err, countData) {
        debug('Informix.prototype.updateOrCreate (data): err=%j, ' +
        'countData=%j\n', err, countData);

        if (err) return cb(err);

        if (countData[0]['cnt'] > 0) {
          stmt = self.buildUpdate(model, where, data);
        } else {
          stmt = self.buildInsert(model, data);
        }

        stmt.noResults = true;

        connection.query(stmt, function(err, sData) {
          debug('Informix.prototype.updateOrCreate (data): err=%j, sData=%j\n',
                err, sData);

          if (err) return cb(err);

          meta.isNewInstance = countData[0]['cnt'] === 0;
          cb(null, data, meta);
        });
      });
    };

    if (options.transaction) {
      executeWithConnection(options.transaction.connection,
        function(err, data, meta) {
          if (err) {
            return callback && callback(err);
          } else {
            return callback && callback(null, data, meta);
          }
        });
    } else {
      self.beginTransaction(Transaction.READ_COMMITTED, function(err, conn) {
        if (err) {
          conn.close(function() {});
          return callback && callback(err);
        }
        executeWithConnection(conn, function(err, data, meta) {
          if (err) {
            conn.rollbackTransaction(function(err) {
              conn.close(function() {});
              return callback && callback(err);
            });
          } else {
            options.transaction = undefined;
            conn.commitTransaction(function(err) {
              conn.close(function() {});

              if (err) {
                return callback && callback(err);
              }

              return callback && callback(null, data, meta);
            });
          }
        });
      });
    }
  };

require('./migration')(Informix);
require('./discovery')(Informix);
require('./transaction')(Informix);
