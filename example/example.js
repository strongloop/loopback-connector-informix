'use strict';

var g = require('../../lib/globalize');
var DataSource = require('loopback-datasource-juggler').DataSource;
var Informix = require('../'); // loopback-connector-informix

var config = {
  username: process.env.INFORMIX_USERNAME,
  password: process.env.INFORMIX_PASSWORD,
  hostname: process.env.INFORMIX_HOSTNAME,
  port: 50000,
  database: 'SQLDB',
};

var db = new DataSource(Informix, config);

var User = db.define('User', {name: {type: String}, email: {type: String},
});

db.autoupdate('User', function(err) {
  if (err) {
    console.log(err);
    return;
  }

  User.create({
    name: 'Tony',
    email: 'tony@t.com',
  }, function(err, user) {
    console.log(err, user);
  });

  User.find({where: {name: 'Tony'}}, function(err, users) {
    console.log(err, users);
  });

  User.destroyAll(function() {
    g.log('example complete');
  });
});
