var vows = require('vows'),
    assert = require('assert'),
    step = require('step'),
    coffee = require('coffee-script'),
    fs = require('fs');

var helpers = require('./helpers'),
    index = require('../lib/index'),
    FileStorage = require('../lib/index/file-storage');

var options = {};

var suite = vows.describe('Node index/fs partition test');

suite = helpers.fileTest(suite, {}, {
  filename: __dirname + '/data/fpt.db',
  partitionSize: 2 * 1024 * 1024
}, options);

options.reopen = true;

helpers.fileTest(suite, {}, {
  filename: __dirname +'/data/fpt.db',
  partitionSize: 2 * 1024 * 1024
}, options).export(module);

