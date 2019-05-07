// Copyright IBM Corp. 2016,2018. All Rights Reserved.
// Node module: loopback-connector-informix

'use strict';

var g = require('./globalize');

module.exports = mixinDiscovery;

/**
 * @param {Informix} Informix connector class
 * @param {Object} informix
 */
function mixinDiscovery(Informix, informix) {
  var async = require('async');

  Informix.prototype.paginateSQL = function(sql, orderBy, options) {
    options = options || {};
    var limitClause = '';
    if (options.offset || options.skip || options.limit) {
      // Offset starts from 0
      var offset = Number(options.offset || options.skip || 0);
      if (isNaN(offset)) {
        offset = 0;
      }
      if (options.limit) {
        var limit = Number(options.limit);
        if (isNaN(limit)) {
          limit = 0;
        }
        limitClause = ' FETCH FIRST ' + limit + ' ROWS ONLY';
      }
    }
    if (!orderBy) {
      sql += ' ORDER BY ' + orderBy;
    }

    // return sql + limitClause;
    return sql + limitClause;
  };

  /**
   * Build sql for listing schemas (databases in Informix)
   *
   * @param {Object} [options]
   * @returns {string} sql
   */
  Informix.prototype.buildQuerySchemas = function(options) {
    var sql =
      'SELECT definer as "catalog",' +
      ' schemaname as "schema"' +
      ' FROM syscat.schemata';

    return this.paginateSQL(sql, 'schema_name', options);
  };

  /**
   * Build sql for listing tables
   *
   * @param {Object} options
   * @returns {string} sql
   */
  Informix.prototype.buildQueryTables = function(options) {
    var sqlTables = null;
    var schema = options.owner || options.schema;

    if (options.all && !schema) {
      sqlTables = this.paginateSQL(
        'SELECT tabtype AS "type",' +
          ' tabname AS "name", owner as "owner"' +
          " FROM systables WHERE type = 'T'",
        'table_schema, table_name',
        options
      );
    } else if (schema) {
      sqlTables = this.paginateSQL(
        'SELECT tabtype AS "type",' +
          ' tabname AS "name", owner AS "owner"' +
          " FROM systables WHERE type = 'T'" +
          ' WHERE owner="' +
          schema +
          'table_schema, table_name',
        options
      );
    } else {
      sqlTables = this.paginateSQL(
        'SELECT tabtype AS "type",' +
          ' tabname AS "name", ' +
          ' owner AS "owner" FROM systables' +
          " WHERE type = 'T' AND owner = CURRENT USER",
        'tabname',
        options
      );
    }

    return sqlTables;
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
      var schema = options.owner || options.schema;
    }

    var sqlTables = null;
    if (options.all && !schema) {
      sqlTables = this.paginateSQL(
        'SELECT tabtype AS "type",' +
          ' tabname AS "name", owner as "owner"' +
          " FROM systables WHERE type = 'V'",
        'table_schema, table_name',
        options
      );
    } else if (schema) {
      sqlTables = this.paginateSQL(
        'SELECT tabtype AS "type",' +
          ' tabname AS "name", owner AS "owner"' +
          " FROM systables WHERE type = 'V'" +
          ' WHERE owner="' +
          schema +
          'table_schema, table_name',
        options
      );
    } else {
      sqlTables = this.paginateSQL(
        'SELECT tabtype AS "type",' +
          ' tabname AS "name", ' +
          ' owner AS "owner" FROM systables' +
          " WHERE type = 'V' AND owner = CURRENT USER",
        'tabname',
        options
      );
    }

    return sqlViews;
  };

  /**
   * Discover database schemas
   *
   // * @param {Object} options Options for discovery
   * @param {Function} [cb] The callback function
   */
  Informix.prototype.discoverDatabaseSchemas = function(cb) {
    var options = {};
    var self = this;
    // if (!cb && typeof options === 'function') {
    //   cb = options;
    //   options = {};
    // }
    // options = options || {};
    this.execute(self.buildQuerySchemas(options), cb);
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
    var calls = [
      function(callback) {
        self.execute(self.buildQueryTables(options), callback);
      },
    ];

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
    // if ('string' !== (typeof table || !table)) {
    //   throw new Error('table is a required string argument: ' + table);
    // }
    options = options || {};
    // if (!cb && 'function' === (typeof options)) {
    //   cb = options;
    //   options = {};
    // }
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
    var sql = null;
    if (schema) {
      sql = this.paginateSQL(
        'SELECT tabid,' +
          '       colname AS columnName,' +
          '       coltype AS dataType,' +
          '       collength AS dataLength' +
          ' FROM syscolumns WHERE syscolumns.tabid IN' +
          '      (SELECT tabid FROM systables ' +
          "       WHERE owner = '" +
          schema +
          "' AND " +
          "             tabname = '" +
          table +
          "')",
        'tabname, colno',
        {}
      );
    } else {
      sql = this.paginateSQL(
        'SELECT tabid,' +
          '       colname AS columnName,' +
          '       coltype AS dataType,' +
          '       collength AS dataLength' +
          ' FROM syscolumns WHERE syscolumns.tabid IN' +
          '      (SELECT tabid FROM systables ' +
          "       WHERE tabname = '" +
          table +
          "')",
        'tabname, colno',
        {}
      );
    }
    return sql;
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
    var sql =
      'SELECT tabschema AS "owner",' +
      ' tabname AS "tableName",' +
      ' colname AS "columnName",' +
      ' colseq AS "keySeq",' +
      ' constname AS "pkName"' +
      ' FROM syscat.keycoluse' +
      " WHERE constname = 'PRIMARY'";

    if (schema) {
      sql += " AND tabschema = '" + schema + "'";
    }
    if (table) {
      sql += " AND tabname = '" + table + "'";
    }
    sql += ' ORDER BY' + ' tabschema, constname, tabname, colseq';
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
      'SELECT tabschema AS "fkOwner",' +
      ' constname AS "fkName",' +
      ' tabname AS "fkTableName",' +
      // ' colname AS "fkColumnName",' +
      // ' colseq AS "keySeq",' +
      ' reftabschema AS "pkOwner", \'PRIMARY\' AS "pkName",' +
      ' reftabname AS "pkTableName",' +
      ' refkeyname AS "pkColumnName"' +
      ' FROM syscat.references';

    if (schema || table) {
      sql += ' WHERE';
      if (schema) {
        sql += " tabschema LIKE '" + schema + "'";
      }
      if (table) {
        sql += " AND tabname LIKE '\"" + table + "'";
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
    var sql =
      'SELECT a.constraint_name AS "fkName",' +
      ' a.tabschema AS "fkOwner",' +
      ' a.tabname AS "fkTableName",' +
      ' a.colname AS "fkColumnName",' +
      // ' a.ordinal_position AS "keySeq",' +
      ' NULL AS "pkName",' +
      ' a.referenced_table_schema AS "pkOwner",' +
      ' a.referenced_table_name AS "pkTableName",' +
      ' a.referenced_column_name AS "pkColumnName"' +
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
  Informix.prototype.discoverExportedForeignKeys = function(
    table,
    options,
    cb
  ) {
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
    return process.env['USER'];
    // if (this.dataSource && this.dataSource.settings &&
    //   this.dataSource.settings.database) {
    //   return this.dataSource.settings.database;
    // }
    // return undefined;
  };
}
