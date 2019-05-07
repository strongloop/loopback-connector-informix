// Copyright IBM Corp. 2017. All Rights Reserved.
// Node module: loopback-connector-informix

'use strict';

var describe = require('./describe');

/* eslint-env node, mocha */
process.env.NODE_ENV = 'test';
require('./init.js');
var assert = require('assert');
var DataSource = require('loopback-datasource-juggler').DataSource;

var config;

before(function() {
  config = global.config;
});

describe('testConnection', function() {
  it('should pass with valid settings', function(done) {
    var db = new DataSource(require('../'), config);
    db.ping(function(err) {
      assert(!err, 'Should connect without err.');
      done();
    });
  });

  it('should pass when valid DSN overrides empty settings', function(done) {
    var dsn = generateDSN(config);
    var dbConfig = {
      dsn: dsn,
    };

    var db = new DataSource(require('../'), dbConfig);
    db.ping(function(err) {
      assert(!err, 'Should connect without err.');
      done();
    });
  });

  it('should pass when valid DSN overrides invalid settings', function(done) {
    var dsn = generateDSN(config);
    var dbConfig = {
      dsn: dsn,
      host: 'invalid-hostname',
      port: 80,
      database: 'invalid-database',
      username: 'invalid-username',
      password: 'invalid-password',
    };

    var db = new DataSource(require('../'), dbConfig);
    db.ping(function(err) {
      assert(!err, 'Should connect without err.');
      done();
    });
  });
});

function generateDSN(config) {
  var dsn =
    'DRIVER={INFORMIX 3.51 64 BIT}' +
    ';DATABASE=' + config.database +
    ';HOSTNAME=' + config.hostname +
    ';SERVER=' + config.servername +
    ';UID=' + config.username +
    ';PWD=' + config.password +
    ';PORT=' + config.port +
    ';PROTOCOL=TCPIP' +
    ';AUTHENTICATION=SERVER';
  return dsn;
}
