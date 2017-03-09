// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-informix
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

var describe = require('./describe');

module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = {
  username: process.env.INFORMIX_USERNAME || 'informix',
  password: process.env.INFORMIX_PASSWORD || 'in4mix',
  hostname: process.env.INFORMIX_HOSTNAME || 'localhost',
  port: process.env.INFORMIX_PORTNUM || 9089,
  database: process.env.INFORMIX_DATABASE || 'loopback',
  protocol: process.env.INFORMIX_PROTOCOL || 'TCPIP',
  servername: process.env.INFORMIX_SERVER || 'dev',
  driver: process.env.INFORMIX_DRIVER || 'INFORMIX 3.51 64 BIT',
  authentication: process.env.INFORMIX_AUTH || 'SERVER',
  minPoolSize: 10,
  maxPoolSize: 300,
};

global.config = config;

global.getDataSource = global.getSchema = function(options) {
  var db = new DataSource(require('../'), config);
  return db;
};

global.connectorCapabilities = {
  ilike: false,
  nilike: false,
};

global.sinon = require('sinon');
