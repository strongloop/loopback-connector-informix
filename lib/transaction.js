var g = require('./globalize');
var debug = require('debug')('loopback:connector:informix:transaction');
var Transaction = require('loopback-connector').Transaction;

module.exports = mixinTransaction;

convertIsolationLevel = function(isolationLevel) {
  var isoString;

  switch (isolationLevel) {
    default:
    case(Transaction.READ_COMMITTED):
      isoString = 'COMMITTED READ';
      break;
    case(Transaction.SERIALIZABLE):
    case(Transcation.REPEATABLE_READ):
      isoString = 'REPEATABLE READ';
      break;
    case(Transaction.CURSOR_STABILITY):
      isoString = 'CURSOR STABILITY';
      break;
  }

  return isoString;
}

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

    if (isolationLevel !== Transaction.READ_COMMITTED &&
      isolationLevel !== Transaction.SERIALIZABLE) {
      var err = new Error(g.f('Invalid isolationLevel: %s', isolationLevel));
      err.statusCode = 400;
      return process.nextTick(function() {
        cb(err);
      });
    }

    self.client.open(self.connStr, function(err, connection) {
      if (err) return cb(err);
      connection.beginTransaction(function(err) {
        if (err) {
          return cb(err);
        }
        // if (isolationLevel) {
        //   var sql = 'SET ISOLATION ' +
        //              convertIsolationLevel(isolationLevel);
        //   connection.query(sql, function(err) {
        //     if (err) {
        //       return cb(err);
        //     }

        //     return cb(err, connection);
        //   });
        // } else {
        return cb(err, connection);
        // }
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
