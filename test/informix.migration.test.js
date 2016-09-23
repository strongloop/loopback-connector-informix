///* eslint-env node, mocha */
//process.env.NODE_ENV = 'test';
//
//require('./init.js');
//require('should');
//var assert = require('assert');
//var Schema = require('loopback-datasource-juggler').Schema;
//
//var db, UserData, NumberData, DateData;
//
//describe('migrations', function() {
//  before(function(done) {
//    db = global.getDataSource();
//
//    UserData = db.define('UserData', {
//      email: { type: String, null: false, index: true,
//               informix: {columnName: 'email', dataType: 'VARCHAR',
//                     dataLength: 255, nullable: true} },
//      name: String,
//      bio: Schema.Text,
//      birthDate: Date,
//      pendingPeriod: Number,
//      createdByAdmin: Boolean },
//      { indexes: {index0: {columns: 'email,createdByAdmin'}}}
//    );
//
//    NumberData = db.define('NumberData', {
//      number: {type: Number, null: false, index: true, unsigned: true,
//        dataType: 'DECIMAL', precision: 10, scale: 3},
//      tinyInt: {type: Number, dataType: 'SMALLINT', display: 2},
//      mediumInt: {type: Number, dataType: 'INTEGER', unsigned: true,
//        required: true},
//      floater: {type: Number, dataType: 'DECIMAL', precision: 16, scale: 7},
//    });
//
//    DateData = db.define('DateData', {
//      dateTime: {type: Date, dataType: 'timestamp'},
//      timestamp: {type: Date, dataType: 'timestamp'},
//    });
//
//    db.automigrate(['UserData', 'NumberData', 'DateData'], done);
//  });
//
//  it('should run migration', function(done) {
//    db.automigrate(['UserData'], done);
//  });
//
//  it('UserData should have correct columns', function(done) {
//    db.adapter.getTableStatus('UserData', function(err, fields, indexes) {
//      if (err) {
//        return done && done(err);
//      } else {
//        fields.should.be.eql([
//          {
//            colno: 1,
//            datalength: 255,
//            datatype: 269,
//            name: 'email',
//          },
//          {
//            colno: 2,
//            datalength: 255,
//            datatype: 40,
//            name: 'name',
//          },
//          {
//            colno: 3,
//            datalength: 4096,
//            datatype: 40,
//            name: 'bio',
//          },
//          {
//            colno: 4,
//            datalength: 4365,
//            datatype: 10,
//            name: 'birthDate',
//          },
//          {
//            colno: 5,
//            datalength: 4,
//            datatype: 2,
//            name: 'pendingPeriod',
//          },
//          {
//            colno: 6,
//            datalength: 2,
//            datatype: 1,
//            name: 'createdByAdmin',
//          },
//          {
//            colno: 7,
//            datalength: 4,
//            datatype: 262,
//            name: 'id',
//          },
//        ]);
//      }
//
//      done();
//    });
//  });
//
//  it('UserData should have correct indexes', function(done) {
//    // Note: getIndexes truncates multi-key indexes to the first member.
//    // Hence index1 is correct.
//    db.adapter.getTableStatus('UserData', function(err, fields, indexes) {
//      if (err) {
//        return done(Error(err));
//      } else {
//        indexes[0].idxtype.should.be.eql('U');
//        indexes[0].tabname.should.be.eql('UserData');
//        indexes[0].part1.should.be.eql(7);
//        indexes[0].part2.should.be.eql(0);
//
//        indexes[1].idxtype.should.be.eql('D');
//        indexes[1].tabname.should.be.eql('UserData');
//        indexes[1].part1.should.be.eql(1);
//        indexes[1].part2.should.be.eql(6);
//      }
//
//      done();
//    });
//  });
//
//  // it('StringData should have correct columns', function(done) {
//  //   getFields('StringData', function(err, fields) {
//  //     if (err)
//  //       done();
//
//  //     fields.should.be.eql({
//  //       idString: { Field: 'idString',
//  //         Type: 'varchar(255)',
//  //         Null: 'NO',
//  //         Key: 'PRI',
//  //         Default: null,
//  //         Extra: ''},
//  //       smallString: { Field: 'smallString',
//  //         Type: 'char(127)',
//  //         Null: 'NO',
//  //         Key: 'MUL',
//  //         Default: null,
//  //         Extra: '' },
//  //       mediumString: { Field: 'mediumString',
//  //         Type: 'varchar(255)',
//  //         Null: 'NO',
//  //         Key: '',
//  //         Default: null,
//  //         Extra: '' },
//  //       tinyText: { Field: 'tinyText',
//  //         Type: 'tinytext',
//  //         Null: 'YES',
//  //         Key: '',
//  //         Default: null,
//  //         Extra: '' },
//  //       giantJSON: { Field: 'giantJSON',
//  //         Type: 'longtext',
//  //         Null: 'YES',
//  //         Key: '',
//  //         Default: null,
//  //         Extra: '' },
//  //       text: { Field: 'text',
//  //         Type: 'varchar(1024)',
//  //         Null: 'YES',
//  //         Key: '',
//  //         Default: null,
//  //         Extra: '' },
//  //     });
//  //     done();
//  //   });
//  // });
//
//  it('NumberData should have correct columns', function(done) {
//    db.adapter.getTableStatus('NumberData', function(err, fields, indexes) {
//      if (err) {
//        return done(Error(err));
//      } else {
//        fields.should.be.eql([
//          {
//            colno: 1,
//            datalength: 2563,
//            datatype: 261,
//            name: 'number',
//          },
//          {
//            colno: 2,
//            datalength: 2,
//            datatype: 1,
//            name: 'tinyInt',
//          },
//          {
//            colno: 3,
//            datalength: 4,
//            datatype: 258,
//            name: 'mediumInt',
//          },
//          {
//            colno: 4,
//            datalength: 4103,
//            datatype: 5,
//            name: 'floater',
//          },
//          {
//            colno: 5,
//            datalength: 4,
//            datatype: 262,
//            name: 'id',
//          },
//        ]);
//      }
//
//      done();
//    });
//  });
//
//  it('DateData should have correct columns', function(done) {
//    db.adapter.getTableStatus('DateData', function(err, fields, indexes) {
//      if (err) {
//        return done(Error(err));
//      } else {
//
//        fields.should.be.eql([
//          {
//            colno: 1,
//            datalength: 4365,
//            datatype: 10,
//            name: 'dateTime',
//          },
//          {
//            colno: 2,
//            datalength: 4365,
//            datatype: 10,
//            name: 'timestamp',
//          },
//          {
//            colno: 3,
//            datalength: 4,
//            datatype: 262,
//            name: 'id',
//          },
//        ]);
//      }
//
//      done();
//    });
//  });
//
//  it.skip('should autoupdate', function(done) {
//    var userExists = function(cb) {
//      query('SELECT * FROM ' + global.config.schema + '.\"UserData\"',
//        function(err, res) {
//          cb(!err && res[0].email === 'test@example.com');
//        });
//    };
//
//    UserData.create({email: 'test@example.com'}, function(err, user) {
//      if (err) {
//        return done(Error(err));
//      } else {
//        userExists(function(yep) {
//          assert.ok(yep, 'User does not exist');
//        });
//
//        UserData.defineProperty('email', { type: String });
//        UserData.defineProperty('name', {type: String,
//          dataType: 'char', limit: 50});
//        UserData.defineProperty('newProperty', {type: Number, unsigned: true,
//          dataType: 'bigInt'});
//
//        // UserData.defineProperty('pendingPeriod', false);
//        // This will not work as expected.
//        db.autoupdate(function(err) {
//          if (err)
//            return done(Error(err));
//
//          db.adapter.getTableStatus('UserData', function(err, fields, indexes) {
//            if (err)
//              return done(Error(err));
//
//            // change nullable for email
//            assert.equal(fields[0].NULLS, 'N',
//                         'Email does not allow null');
//
//            // change type of name
//            assert.equal(fields[1].DATATYPE, 'VARCHAR',
//                         'Name is not char(50)');
//            assert.equal(fields[1].DATALENGTH, 512, 'Length is not 512');
//
//            // add new column
//            assert.ok(fields[7].NAME, 'NEWPROPERTY',
//                      'New column was not added');
//
//            if (fields[7].NAME === 'NEWPROPERTY') {
//              assert.equal(fields[7].DATATYPE, 'BIGINT',
//                'New column type is not bigint(20) unsigned');
//            }
//
//            // drop column - will not happen.
//            // assert.ok(!fields.pendingPeriod,
//            // 'Did not drop column pendingPeriod');
//            // user still exists
//            userExists(function(yep) {
//              assert.ok(yep, 'User does not exist');
//            });
//          });
//        });
//      }
//
//      done && done();
//    });
//  });
//
//  it.skip('should check actuality of dataSource', function(done) {
//    // 'drop column'
//    UserData.dataSource.isActual(function(err, ok) {
//      if (err)
//        return done(Error(err));
//
//      // TODO: Need to validate columns/indexes to test actuality and return
//      // appropriate values.
//      // assert.ok(ok, 'dataSource is not actual (should be)');
//      UserData.defineProperty('essay', {type: Schema.Text});
//      // UserData.defineProperty('email', false); Can't undefine currently.
//      UserData.dataSource.isActual(function(err, ok) {
//        if (err)
//          return done(Error(err));
//
//        // assert.ok(!ok, 'dataSource is actual (shouldn\t be)');
//        done();
//      });
//    });
//  });
//
//  it('should allow numbers with decimals', function(done) {
//    NumberData.create({number: 1.1234567, tinyInt: 12345, mediumInt: -1234567,
//      floater: 123456789.1234567 }, function(err, obj) {
//      assert.ok(!err);
//      assert.ok(obj);
//      NumberData.findById(obj.id, function(err, found) {
//        if (err) {
//          return done(Error(err));
//        } else {
//          assert.equal(found.number, 1.123);
//          assert.equal(found.tinyInt, 12345);
//          assert.equal(found.mediumInt, -1234567);
//          assert.equal(found.floater, 123456789.1234567);
//        }
//        done();
//      });
//    });
//  });
//
//  it('should allow both kinds of date columns', function(done) {
//    DateData.create({
//      dateTime: new Date('Aug 9 1996 07:47:33 GMT'),
//      timestamp: new Date('Sep 22 2007 17:12:22 GMT'),
//    }, function(err, obj) {
//      assert.ok(!err);
//      assert.ok(obj);
//      DateData.findById(obj.id, function(err, found) {
//        if (err) {
//          return done(Error(err));
//        } else {
//          assert.equal(found.dateTime.toGMTString(),
//            'Fri, 09 Aug 1996 07:47:33 GMT');
//          assert.equal(found.timestamp.toGMTString(),
//            'Sat, 22 Sep 2007 17:12:22 GMT');
//        }
//        done();
//      });
//    });
//  });
//
//  it('should report errors for automigrate', function() {
//    db.automigrate('XYZ', function(err) {
//      assert(err);
//    });
//  });
//
//  it('should report errors for autoupdate', function() {
//    db.autoupdate('XYZ', function(err) {
//      assert(err);
//    });
//  });
//
//  it('should disconnect when done', function(done) {
//    db.disconnect();
//    done();
//  });
//
//});
//
//var query = function(sql, cb) {
//  db.adapter.execute(sql, cb);
//};
