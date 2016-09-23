// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-informix
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

var describe = require('./describe');

/* eslint-env node, mocha */
describe('informix imported features', function() {
  before(function() {
    require('./init.js');
  });

  require('loopback-datasource-juggler/test/common.batch.js');
  require('loopback-datasource-juggler/test/include.test.js');
});
