var vows = require('vows'),
    assert = require('assert'),
    step = require('step'),
    coffee = require('coffee-script'),
    fs = require('fs');

var helpers = require('./helpers'),
    index = require('../lib/index'),
    FileStorage = require('../lib/index/file-storage');

var options = {};

var suite = vows.describe('Node index/fs high order test');

helpers.fileTest(suite, {order: 256}, {
  filename: __dirname + '/data/fht.db'
}, options).export(module);

