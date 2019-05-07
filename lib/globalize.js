// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-informix

'use strict';

var path = require('path');
var SG = require('strong-globalize');

SG.SetRootDir(path.join(__dirname, '..'), {autonomousMsgLoading: 'all'});
module.exports = SG();
