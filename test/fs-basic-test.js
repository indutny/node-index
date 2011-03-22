var vows = require('vows'),
    assert = require('assert'),
    step = require('step'),
    coffee = require('coffee-script'),
    fs = require('fs');

var helpers = require('./helpers'),
    index = require('../lib/index'),
    FileStorage = require('../lib/index/file-storage');

var options = {};

var suite = vows.describe('Node index/fs basic test');

helpers.fileTest(suite, {}, {
  filename: __dirname + '/data/fbt.db'
}, options);

helpers.fileTest(suite, {}, {
  filename: __dirname +'/data/fbt.db',
  reopen: true
}, options).export(module);

