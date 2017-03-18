// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-informix
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

var g = require('./globalize');
var debug = require('debug')('loopback:connector:informix');

module.exports = mixinDiscovery;

/**
* @param {Informix} Informix connector class
* @param {Object} informix
*/
function mixinDiscovery(Informix, informix) {
  var async = require('async');

  Informix.prototype.paginateSQL = function(sql, orderBy, options) {
    debug('Informix.prototype.applyPagination sql: %j orderBy: %j options: %j',
          sql, orderBy, options);
    return this.applyPagination(null, sql, orderBy);
  };

  /**
   * Build sql for listing schemas (databases in Informix)
   *
   * @param {Object} [options]
   * @returns {string} sql
   */
  Informix.prototype.buildQuerySchemas = function(options) {
    process.nextTick(function() {
      throw new Error(g.f('{{buildQuerySchemas()}} is ' +
      'not currently supported.'));
    });
  };

  /**
   * Build sql for listing tables
   *
   * @param {Object} options
   * @returns {string} sql
   */
  Informix.prototype.buildQueryTables = function(options) {
    var sqlTables = null;
    var statement = 'SELECT tabtype, tabname, trim(owner) as owner ' +
                    'FROM systables WHERE tabtype = \'T\'';
    var schema = options.schema || options.owner;

    if (!schema && !options.all) {
      schema = this.schema;
    }

    if (schema) {
      statement += ' AND owner = \'' + schema.toLowerCase() + '\'';
    }

    statement += ' ORDER BY tabname ';

    return this.applyPagination(null, statement, options);
  };

  /**
   * Build sql for listing views
   *
   * @param {Object} options
   * @returns {string} sql
   */
  Informix.prototype.buildQueryViews = function(options) {
    var sqlViews = null;
    if (options.views) {
      var schema = options.schema || options.owner;

      if (!schema && !options.all) {
        schema = this.schema;
      }
    }

    var statement = 'SELECT tabtype, tabname, trim(owner) as owner ' +
                    'FROM systables WHERE tabtype = \'V\'';

    if (schema) {
      statement += ' AND owner = \'' + schema.toLowerCase() + '\'';
    }

    statement += ' ORDER BY tabname ';

    return this.applyPagination(null, statement, options);
  };

  /**
   * Discover database schemas
   *
   // * @param {Object} options Options for discovery
   * @param {Function} [cb] The callback function
   */
  Informix.prototype.discoverDatabaseSchemas = function(cb) {
    this.execute(self.buildQuerySchemas({}), cb);
  };

  Informix.prototype.setDefaultOptions = function(options) {
    process.nextTick(function() {
      throw Error(g.f('Function {{setDefaultOptions}} not supported'));
    });
  };

  Informix.prototype.setNullableProperty = function(property) {
    process.nextTick(function() {
      throw Error(g.f('Function {{setNullableProperty}} not supported'));
    });
  };
  /**
   * Discover model definitions
   *
   * @param {Object} options Options for discovery
   * @param {Function} [cb] The callback function
   */
  Informix.prototype.discoverModelDefinitions = function(options, cb) {
    if (!cb && typeof options === 'function') {
      cb = options;
      options = {};
    }
    options = options || {};

    var self = this;
    var calls = [function(callback) {
      self.execute(self.buildQueryTables(options), callback);
    }];

    if (options.views) {
      calls.push(function(callback) {
        self.execute(self.buildQueryViews(options), callback);
      });
    }
    async.parallel(calls, function(err, data) {
      if (err) {
        cb(err, data);
      } else {
        var merged = [];
        merged = merged.concat(data.shift());
        if (data.length) {
          merged = merged.concat(data.shift());
        }
        cb(err, merged);
      }
    });
  };

  /**
   * Normalize the arguments
   *
   * @param {string} table
   * @param {Object} [options]
   * @param {Function} [cb]
   */
  Informix.prototype.getArgs = function(table, options, cb) {
    options = options || {};
    if (typeof options !== 'object') {
      throw new Error(g.f('options must be an object: %j', options));
    }

    return {
      schema: options.owner || options.schema,
      table: table,
      options: options,
      cb: cb,
    };
  };

  /**
   * Build the sql statement to query columns for a given table
   *
   * @param {string} schema
   * @param {string} table
   * @returns {string} The sql statement
   */
  Informix.prototype.buildQueryColumns = function(schema, table) {
    debug('Informix.prototype.buildQueryColumns schema: %j table: %j',
          schema, table);

    var sql = 'SELECT tabid, colname, coltype, collength ' +
              'FROM syscolumns WHERE syscolumns.tabid IN' +
              '  (SELECT tabid FROM systables ';

    if (schema) {
      sql += ' WHERE owner = \'' + schema + '\' AND' +
             ' tabname = \'' + table + '\') ';
    }

    sql += 'ORDER BY tabid, colno';

    return this.applyPagination(null, sql, {});
  };

  /**
   * Discover model properties from a table
   *
   * @param {string} table The table name
   * @param {Object} options The options for discovery
   * @param {Function} [cb] The callback function
   *
   */
  Informix.prototype.discoverModelProperties = function(table, options, cb) {
    var self = this;
    var args = this.getArgs(table, options, cb);
    var schema = args.schema;
    if (!schema) {
      schema = this.getDefaultSchema();
    }
    table = args.table;
    options = args.options;
    cb = args.cb;

    var sql = self.buildQueryColumns(schema, table);

    var callback = function(err, results) {
      if (err) {
        cb(err, results);
      } else {
        results.map(function(r) {
          r.type = self.buildPropertyType(r);
          r.nullable = r.nullable ? 'Y' : 'N';
        });
        cb(err, results);
      }
    };

    this.execute(sql, callback);
  };

  /**
   * Build the sql statement for querying primary keys of a given table
   *
   * @param {string} schema
   * @param {string} table
   * @returns {string}
   */
  Informix.prototype.buildQueryPrimaryKeys = function(schema, table) {
// constrid    3710
// constrname  u1888_3710
// owner       informix
// tabid       1888
// constrtype  P
// idxname      1888_3710
// collation   en_US.57372
    var sql = 'SELECT owner, tabid, ' +
      ' tabname AS tableName,' +
      ' colname AS columnName,' +
      ' colseq AS keySeq,' +
      ' constname AS pkName' +
      ' FROM syscat.keycoluse' +
      ' WHERE constname = \'PRIMARY\'';

    if (schema) {
      sql += ' AND tabschema = \'' + schema + '\'';
    }
    if (table) {
      sql += ' AND tabname = \'' + table + '\'';
    }
    sql += ' ORDER BY' +
      ' tabschema, constname, tabname, colseq';
    return sql;
  };

  /**
   * Discover primary keys for a given table
   *
   * @param {string} table The table name
   * @param {Object} options The options for discovery
   * @param {Function} [cb] The callback function
   */
  Informix.prototype.discoverPrimaryKeys = function(table, options, cb) {
    var args = this.getArgs(table, options, cb);
    var schema = args.schema;
    if (!schema) {
      schema = this.getDefaultSchema();
    }
    table = args.table;
    options = args.options;
    cb = args.cb;

    var sql = this.buildQueryPrimaryKeys(schema, table);

    this.execute(sql, cb);
  };

  /**
   * Build the sql statement for querying foreign keys of a given table
   *
   * @param {string} schema
   * @param {string} table
   * @returns {string}
   */
  Informix.prototype.buildQueryForeignKeys = function(schema, table) {
    var sql =
      'SELECT tabschema AS fkOwner,' +
      ' constname AS fkName,' +
      ' tabname AS fkTableName,' +
      ' reftabschema AS pkOwner, \'PRIMARY\' AS pkName,' +
      ' reftabname AS pkTableName,' +
      ' refkeyname AS pkColumnName' +
      ' FROM syscat.references';

    if (schema || table) {
      sql += ' WHERE';
      if (schema) {
        sql += ' tabschema LIKE \'' + schema + '\'';
      }
      if (table) {
        sql += ' AND tabname LIKE \'' + table + '\'';
      }
    }
    return sql;
  };

  /**
   * Discover foreign keys for a given table
   *
   * @param {string} table The table name
   * @param {Object} options The options for discovery
   * @param {Function} [cb] The callback function
   */
  Informix.prototype.discoverForeignKeys = function(table, options, cb) {
    var args = this.getArgs(table, options, cb);
    var schema = args.schema;
    if (!schema) {
      schema = this.getDefaultSchema();
    }
    table = args.table;
    options = args.options;
    cb = args.cb;

    var sql = this.buildQueryForeignKeys(schema, table);
    this.execute(sql, cb);
  };

  /**
   * Retrieves a description of the foreign key columns that reference the
   *
   * given table's primary key columns (the foreign keys exported by a table).
   * They are ordered by fkTableOwner, fkTableName, and keySeq.
   *
   * @param {string} schema
   * @param {string} table
   * @returns {string}
   */
  Informix.prototype.buildQueryExportedForeignKeys = function(schema, table) {
    var sql = 'SELECT a.constraint_name AS fkName,' +
      ' a.tabschema AS fkOwner,' +
      ' a.tabname AS fkTableName,' +
      ' a.colname AS fkColumnName,' +
      ' NULL AS pkName,' +
      ' a.referenced_table_schema AS pkOwner,' +
      ' a.referenced_table_name AS pkTableName,' +
      ' a.referenced_column_name AS pkColumnName' +
      ' FROM information_schema.key_column_usage a' +
      ' WHERE a.position_in_unique_constraint IS NOT NULL';
    if (schema) {
      sql += ' AND a.referenced_table_schema="' + schema + '"';
    }
    if (table) {
      sql += ' AND a.referenced_table_name="' + table + '"';
    }
    sql += ' ORDER BY a.table_schema, a.table_name, a.ordinal_position';

    return sql;
  };

  /**
   * Discover foreign keys that reference to the primary key of this table
   *
   * @param {string} table The table name
   * @param {Object} options The options for discovery
   * @param {Function} [cb] The callback function
   */
  Informix.prototype.discoverExportedForeignKeys = function(table,
                                                            options, cb) {
    var args = this.getArgs(table, options, cb);
    var schema = args.schema;
    if (!schema) {
      schema = this.getDefaultSchema();
    }
    table = args.table;
    options = args.options;
    cb = args.cb;

    var sql = this.buildQueryExportedForeignKeys(schema, table);
    this.execute(sql, cb);
  };

  Informix.prototype.buildPropertyType = function(columnDefinition) {
    var informixType = columnDefinition.dataType;
    var dataLength = columnDefinition.dataLength;

    var type = informixType.toUpperCase();
    switch (type) {
      case 'CHAR':
        if (dataLength === 1) {
          // Treat char(1) as boolean
          return 'Boolean';
        } else {
          return 'String';
        }
        break;
      case 'VARCHAR':
      case 'TINYTEXT':
      case 'MEDIUMTEXT':
      case 'LONGTEXT':
      case 'TEXT':
      case 'ENUM':
      case 'SET':
        return 'String';
      case 'TINYBLOB':
      case 'MEDIUMBLOB':
      case 'LONGBLOB':
      case 'BLOB':
      case 'BINARY':
      case 'VARBINARY':
      case 'BIT':
        return 'Binary';
      case 'TINYINT':
      case 'SMALLINT':
      case 'INT':
      case 'INTEGER':
      case 'MEDIUMINT':
      case 'YEAR':
      case 'FLOAT':
      case 'DOUBLE':
      case 'BIGINT':
        return 'Number';
      case 'DATE':
      case 'TIMESTAMP':
      case 'DATETIME':
        return 'Date';
      case 'POINT':
        return 'GeoPoint';
      default:
        return 'String';
    }
  };

  Informix.prototype.getDefaultSchema = function() {
    return this.schema;
  };
}
