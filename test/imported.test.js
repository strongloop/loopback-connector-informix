// Copyright IBM Corp. 2016,2017. All Rights Reserved.
// Node module: loopback-connector-informix

'use strict';

var describe = require('./describe');
require('./init.js');

/* eslint-env node, mocha */
describe('informix imported features', function() {
  require('loopback-datasource-juggler/test/common.batch.js');
  require('loopback-datasource-juggler/test/include.test.js');
});
