// Copyright IBM Corp. 2016,2017. All Rights Reserved.
// Node module: loopback-connector-informix
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

var g = require('./globalize');
var debug = require('debug')('loopback:connector:informix:transaction');
var Transaction = require('loopback-connector').Transaction;

module.exports = mixinTransaction;

var mapIsolationLevel = function(isolationLevel) {
  var ret = 2;

  switch (isolationLevel) {
    default:
    case Transaction.READ_COMMITTED:
      ret = 1;
      break;
    case Transaction.SERIALIZABLE:
      ret = 4;
      break;
    case Transaction.REPEATABLE_READ:
      ret = 8;
      break;
    case Transaction.READ_COMMITTED:
    case Transaction.CURSOR_STABILITY:
      ret = 2;
      break;
  }

  return ret;
};

/*!
 * @param {Informix} Informix connector class
 */
function mixinTransaction(Informix, informix) {
  /**
   * Begin a new transaction

   * @param {Integer} isolationLevel
   * @param {Function} cb
   */
  Informix.prototype.beginTransaction = function(isolationLevel, cb) {
    debug('Begin a transaction with isolation level: %s', isolationLevel);

    var self = this;

    self.client.open(self.connStr, function(err, connection) {
      if (err) return cb(err);
      connection.beginTransaction(function(err) {
        if (err) {
          console.log('ERROR: ', err);
          return cb(err);
        }

        if (isolationLevel) {
          connection.setIsolationLevel(mapIsolationLevel(isolationLevel));
        }

        cb(err, connection);
      });
    });
  };

  /**
   * Commit a transaction
   *
   * @param {Object} connection
   * @param {Function} cb
   */
  Informix.prototype.commit = function(connection, cb) {
    debug('Commit a transaction');
    connection.commitTransaction(function(err) {
      if (err) return cb(err);
      connection.close(cb);
    });
  };

  /**
   * Roll back a transaction
   *
   * @param {Object} connection
   * @param {Function} cb
   */
  Informix.prototype.rollback = function(connection, cb) {
    debug('Rollback a transaction');
    connection.rollbackTransaction(function(err) {
      if (err) return cb(err);
      // connection.setAutoCommit(true);
      connection.close(cb);
    });
  };
}
