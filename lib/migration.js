// Copyright IBM Corp. 2016,2018. All Rights Reserved.
// Node module: loopback-connector-informix
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

/*!
 * Informix connector for LoopBack
 */
var g = require('./globalize');
var async = require('async');
var debug = require('debug')('loopback:connector:informix');
var SqlConnector = require('loopback-connector').SqlConnector;
var ParameterizedSQL = SqlConnector.ParameterizedSQL;

module.exports = function(Informix) {
  Informix.prototype.searchForPropertyInActual = function(
    model,
    propName,
    actualFields
  ) {
    process.nextTick(function() {
      throw new Error(
        g.f('{{searchForPropertyInActual()}} is ' + 'not currently supported.')
      );
    });
  };

  Informix.prototype.addPropertyToActual = function(model, propName) {
    process.nextTick(function() {
      throw new Error(
        g.f('{{addPropertyToActual()}} is ' + 'not currently supported.')
      );
    });
  };

  Informix.prototype.propertyHasNotBeenDeleted = function(model, propName) {
    process.nextTick(function() {
      throw new Error(
        g.f('{{propertyHasNotBeenDeleted()}} is ' + 'not currently supported.')
      );
    });
  };

  Informix.prototype.applySqlChanges = function(model, pendingChanges, cb) {
    process.nextTick(function() {
      return cb(
        Error(g.f('{{applySqlChanges()}} is not ' + 'currently supported.'))
      );
    });
  };

  Informix.prototype.showFields = function(model, cb) {
    process.nextTick(function() {
      return cb(Error(g.f('{{showFields()}} is not currently supported.')));
    });
  };

  Informix.prototype.showIndexes = function(model, cb) {
    process.nextTick(function() {
      return cb(Error(g.f('{{showIndexes()}} is not currently supported.')));
    });
  };

  /*
   * Perform autoupdate for the given models
   * @param {String[]} [models] A model name or an array of model names.
   * If not present, apply to all models
   * @param {Function} [cb] The callback function
   */
  Informix.prototype.autoupdate = function(models, cb) {
    debug('Informix.prototype.autoupdate %j', models);
    var self = this;

    if (!cb && typeof models === 'function') {
      cb = models;
      models = undefined;
    }
    // First argument is a model name
    if (typeof models === 'string') {
      models = [models];
    }

    models = models || Object.keys(this._models);

    async.each(
      models,
      function(model, done) {
        if (!(model in self._models)) {
          return process.nextTick(function() {
            done(new Error(g.f('Model not found: %s', model)));
          });
        }
        self.getTableStatus(model, function(err, fields, indexes) {
          if (err) {
            return done(err);
          } else {
            if (fields.length) {
              self.alterTable(model, fields, indexes, done);
            } else {
              self.createTable(model, done);
            }
          }
        });
      },
      cb
    );
  };

  /*
   * Discover the properties from a table
   * @param {String} model The model name
   * @param {Function} cb The callback function
   */
  Informix.prototype.getTableStatus = function(model, cb) {
    var self = this;

    var sql =
      'SELECT COLNO, COLLENGTH AS DATALENGTH, COLTYPE AS DATATYPE, ' +
      'COLNAME AS NAME FROM SYSCOLUMNS INNER JOIN SYSTABLES ON ' +
      '(SYSCOLUMNS.TABID = SYSTABLES.TABID) WHERE ' +
      "SYSTABLES.TABNAME LIKE '" +
      self.table(model).toLowerCase() +
      "'" +
      'ORDER BY COLNO';

    self.execute(sql, function(err, tableInfo) {
      debug('Informix.prototype.getTableStatus sql:%j data:%j', sql, tableInfo);
      if (err) {
        cb(err);
      } else {
        var indexSQL =
          'SELECT I.IDXNAME, T.TABNAME, I.IDXTYPE, ' +
          'I.PART1, I.PART2, I.PART3, I.PART4, I.PART5, I.PART6, I.PART7, ' +
          'I.PART8, I.PART9, I.PART10, I.PART11, I.PART12, I.PART13, ' +
          'I.PART14, I.PART15, I.PART16 ' +
          'FROM SYSINDEXES I INNER JOIN SYSTABLES T ' +
          'ON (I.TABID = T.TABID) ' +
          "WHERE T.TABNAME LIKE '" +
          self.table(model).toLowerCase() +
          "'";
        self.execute(indexSQL, function(err, indexInfo) {
          debug('Informix.prototype.getTableStatus sql:%j data:%j', indexInfo);
          if (err) {
            cb(err);
          } else {
            cb(err, tableInfo, indexInfo);
          }
        });
      }
    });
  };

  Informix.prototype.addIndexes = function(model, actualIndexes) {
    var ai = {};
    var self = this;
    var m = this.getModelDefinition(model);
    var indexes = m.settings.indexes || {};
    var indexNames = Object.keys(indexes).filter(function(name) {
      return !!m.settings.indexes[name];
    });
    var operations = [];
    var propNames = Object.keys(m.properties).filter(function(name) {
      return !!m.properties[name];
    });
    var sql = [];
    var type = '';

    if (actualIndexes) {
      actualIndexes.forEach(function(i) {
        var name = i.INDNAME;
        if (!ai[name]) {
          ai[name] = {
            info: i,
            columns: [],
          };
        }

        // i.COLNAMES.split(/\+\s*/).forEach(function(columnName, j) {
        //   // This is a bit of a dirty way to get around this but Informix returns
        //   // column names as a string started with and separated by a '+'.
        //   // The code below will strip out the initial '+' then store the
        //   // actual column names.
        //   if (j > 0)
        //     ai[name].columns[j - 1] = columnName;
        // });
      });
    }
    var aiNames = Object.keys(ai);

    // remove indexes
    aiNames.forEach(function(indexName) {
      if (
        ai[indexName].info.idxtype === 'U' ||
        (m.properties[indexName] && self.id(model, indexName))
      )
        return;

      operations.push('DROP INDEX ' + indexName);
      delete ai[indexName];

      // if (indexNames.indexOf(indexName) === -1 && !m.properties[indexName] ||
      //   m.properties[indexName] && !m.properties[indexName].index) {

      //   if (ai[indexName].info.UNIQUERULE === 'P') {
      //     operations.push('DROP PRIMARY KEY');
      //   } else if (ai[indexName].info.UNIQUERULE === 'U') {
      //     operations.push('DROP UNIQUE ' + indexName);
      //   }
      // } else {
      //   // first: check single (only type and kind)
      //   if (m.properties[indexName] && !m.properties[indexName].index) {
      //     // TODO
      //     return;
      //   }
      //   // second: check multiple indexes
      //   var orderMatched = true;
      //   if (indexNames.indexOf(indexName) !== -1) {
      //     m.settings.indexes[indexName].columns.split(/,\s*/).forEach(
      //       function(columnName, i) {
      //         if (ai[indexName].columns[i] !== columnName) orderMatched = false;
      //       });
      //   }

      //   if (!orderMatched) {
      //     if (ai[indexName].info.UNIQUERULE === 'P') {
      //       operations.push('DROP PRIMARY KEY');
      //     } else if (ai[indexName].info.UNIQUERULE === 'U') {
      //       operations.push('DROP UNIQUE ' + indexName);
      //     }

      //     delete ai[indexName];
      //   }
      // }
    });

    if (operations.length) {
      // Add the ALTER TABLE statement to the list of tasks to perform later.
      sql.push(
        'ALTER TABLE ' /* + self.schema */ +
          '.' +
          self.tableEscaped(model) +
          ' ' +
          operations.join(' ') +
          ';'
      );
    }

    // add single-column indexes
    propNames.forEach(function(propName) {
      var i = m.properties[propName].index;
      if (!i) {
        return;
      }
      var found = ai[propName] && ai[propName].info;
      if (!found) {
        var pName = propName;
        var statement = new ParameterizedSQL('CREATE');
        if (i.type) {
          statement.merge(i.type);
        }
        statement.merge('INDEX ' + pName + ' ON ' + self.schema + '.');
        statement.merge(self.tableEscaped(model));
        statement.merge('(' + self.escapeName(pName) + ')');
        sql.push(statement);
      }
    });

    // add multi-column indexes
    indexNames.forEach(function(indexName) {
      var i = m.settings.indexes[indexName];
      var found = ai[indexName] && ai[indexName].info;
      if (!found) {
        var iName = indexName;
        var statement = new ParameterizedSQL('CREATE');
        if (i.type) {
          statement.merge(i.type);
        }

        statement.merge('INDEX ' + iName + ' ON');
        statement.merge(self.schema + '.' + self.tableEscaped(model) + '(');

        var splitNames = i.columns.split(/,\s*/);
        splitNames.foreach(function(columnName) {
          columnName = self.escapeName(columnName);
        });
        var colNames = splitNames.join(',');
        statement.merge(colNames + ')');

        sql.push(statement);
      }
    });
    return sql;
  };

  Informix.prototype.isActual = function(models, cb) {
    debug('Informix.prototype.isActual %j %j', models, cb);
    var self = this;

    if (!cb && typeof models === 'function') {
      cb = models;
      models = undefined;
    }

    // First argument is a model name
    if (typeof models === 'string') {
      models = [models];
    }

    models = models || Object.keys(this._models);

    // var changes = [];
    async.each(
      models,
      function(model, done) {
        self.getTableStatus(model, function(err, fields, indexes) {
          // TODO: VALIDATE fields/indexes against model definition
          done(err);
        });
      },
      function done(err) {
        if (err) {
          return cb && cb(err);
        }
        var actual = true; // (changes.length === 0);
        if (cb) cb(null, actual);
      }
    );
  };
};
