var vows = require('vows'),
    assert = require('assert'),
    step = require('step'),
    coffee = require('coffee-script'),
    fs = require('fs');

var index = require('../lib/index'),
    FileStorage = require('../lib/index/file-storage');

var I,
    fileStorage,
    filename = __dirname + '/data/fpt.db',
    num = 10000;

vows.describe('Node index/fs partitioning test').addBatch({
  'Creating new file storage': {
    topic: function() {
      try {
        fs.unlinkSync(filename);
        for (var i = 1; i < 100; i++) {
          fs.unlinkSync(filename + '.' + i);
        }
      } catch (e) {
      }

      FileStorage.createStorage({
        filename: filename,
        partitionSize: 1024 * 1024
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
}).addBatch({
  'Closing fds of File-Storage': {
    topic: function() {
      I.storage.close(this.callback);
    },
    'should be successfull': function() {
    }
  }
}).addBatch({
  'Creating new file storage (reopen)': {
    topic: function() {
      FileStorage.createStorage({
        filename: filename,
        partitionSize: 1024 * 1024
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
}).addBatch({
  'Compacting db': {
    topic: function() {
      I.compact(this.callback);
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
