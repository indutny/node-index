var vows = require('vows'),
    assert = require('assert'),
    step = require('step'),
    coffee = require('coffee-script'),
    fs = require('fs');

var index = require('../lib/index'),
    FileStorage = require('../lib/index/file-storage');

var I,
    fileStorage,
    filename = __dirname + '/data/fbt.db',
    num = 10000;

vows.describe('Node index/fs basic test').addBatch({
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
        storage: fileStorage,
        order: 33
      });
    },
    'should create instance of Index': function(_I) {
      I = _I;
      assert.instanceOf(I, index.Index);
    }
  }
}).addBatch({
  'Setting few key-values': {
    topic: function() {
      step(function() {
        var group = this.group();
        I.set(0, 0, group());
        I.set(1, 1, group());
        I.set(3, 3, group());
        I.set(4, 4, group());
        I.set(2, 2, group());
        I.set(5, 5, group());
        I.set(6, 6, group());
        I.set(-1, -1, group());
      }, this.callback);
    },
    'should be successfull': function() {
    }
  }
}).addBatch({
  'Getting any of them': {
    topic: function() {
      step(function() {
        var group = this.group();
        for (var i = -1; i < 7; i++) {
          (function(callback, i) {
            I.get(i, function(err, value) {
              callback(err, {
                key: i,
                value: value
              });
            });
          })(group(), i);
          break;
        }
      }, this.callback);
    },
    'should return correct value': function(values) {
      assert.ok(values.every(function(item) {
        return item.key == item.value;
      }));
    }
  }
}).addBatch({
  'Adding 10k items': {
    topic: function() {
      step(function() {
        var group = this.group();

        for (var i = 0; i < num; i++) {
          I.set('k-' + i, i, group());
        }
      }, this.callback);
    },
    'should be successfull': function() {
    }
  }
}).addBatch({
  'Getting 10k items': {
    topic: function() {
      step(function() {
        var group = this.group();

        for (var i = 0; i < num; i++) {
          (function(i, callback) {
            I.get('k-' + i, function(err, value) {
              callback(null, {
                key: i,
                value: value
              });
            });
          })(i, group());
        }
      }, this.callback);
    },
    'should return correct values': function(values) {
      assert.ok(values.every(function(item) {
        return item.key == item.value;
      })); 
    }
  }
}).export(module);
