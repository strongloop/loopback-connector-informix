// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-informix
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

var describe = require('./describe');

module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = {
  username: process.env.INFORMIX_USERNAME,
  password: process.env.INFORMIX_PASSWORD,
  hostname: process.env.INFORMIX_HOSTNAME || 'localhost',
  port: process.env.INFORMIX_PORTNUM || 40000,
  database: process.env.INFORMIX_DATABASE || 'testdb',
  protocol: process.env.INFORMIX_PROTOCOL || 'TCPIP',
  servername: process.env.INFORMIX_SERVER || 'test_server',
  driver: process.env.INFORMIX_DRIVER || 'INFORMIX 3.51 64 BIT',
};

global.config = config;

global.getDataSource = global.getSchema = function(options) {
  var db = new DataSource(require('../'), config);
  return db;
};

global.sinon = require('sinon');
