module.exports = require('should');

var DataSource = require('loopback-datasource-juggler').DataSource;

var config = {
  username: process.env.INFORMIX_USERNAME,
  password: process.env.INFORMIX_PASSWORD,
  hostname: process.env.INFORMIX_HOSTNAME || 'localhost',
  port: process.env.INFORMIX_PORTNUM || 60000,
  database: process.env.INFORMIX_DATABASE || 'testdb',
  schema: process.env.INFORMIX_SCHEMA || 'STRONGLOOP',
  supportColumnStore: process.env.INFORMIX_USECOLUMNSTORE || false,
  supportDashDB: process.env.INFORMIX_SUPPORT_DASHDB || false,
};

global.config = config;

global.getDataSource = global.getSchema = function(options) {
  var db = new DataSource(require('../'), config);
  return db;
};

global.sinon = require('sinon');
