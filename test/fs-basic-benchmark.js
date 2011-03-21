var vows = require('vows'),
    assert = require('assert'),
    step = require('step'),
    coffee = require('coffee-script'),
    fs = require('fs');

var index = require('../lib/index'),
    FileStorage = require('../lib/index/file-storage');

var I,
    fileStorage,
    filename = __dirname + '/data/fbb.db',
    num = 10000,
    start,
    end;

vows.describe('Node index/fs basic benchmark').addBatch({
  'Creating new file storage': {
    topic: function() {
      try {
        fs.unlinkSync(filename);
      } catch (e) {
      }

      FileStorage.createStorage({
        filename: filename
      }, this.callback)
    },
    'should be successfull': function(_fileStorage) {
      fileStorage = _fileStorage;
    }
  }
}).addBatch({
  'Creating new index': {
    topic: function() {
      return index.createIndex({
        storage: fileStorage
      });
    },
    'should create instance of Index': function(_I) {
      I = _I;
      assert.instanceOf(I, index.Index);
    }
  }
}).addBatch({
  'Adding 100k items': {
    topic: function() {
      step(function() {
        var group = this.group();

        start = +new Date;
        for (var i = 0; i < num; i++) {
          I.set(i, i, group());
        }
      }, this.callback);
    },
    'should be successfull': function() {
      end = +new Date;
      console.log('%d writes per second', 1000 * num / (end - start));
    }
  }
}).addBatch({
  'Getting 100k items': {
    topic: function() {
      step(function() {
        var group = this.group();

        start = +new Date;
        for (var i = 0; i < num; i++) {
          I.get(i, group());
        }
      }, this.callback);
    },
    'should return correct values': function(values) {
      end = +new Date;
      console.log('%d reads per second', 1000 * num / (end - start));
    }
  }
}).export(module);
