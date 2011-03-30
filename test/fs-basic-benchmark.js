var vows = require('vows'),
    assert = require('assert'),
    step = require('step'),
    coffee = require('coffee-script'),
    fs = require('fs'),
    uuid = require('node-uuid');

var index = require('../lib/index'),
    FileStorage = require('../lib/index/file-storage');

var I,
    fileStorage,
    filename = __dirname + '/data/fbb.db',
    N = 1000000,
    dn = 10000,
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
  'Benchmark': {
    topic: function() {
      benchmark(this.callback);
    },
    'should be successfull': function(data) {
    }
  }
}).export(module);

function benchmark(callback) {
  var offset = 0,
      results = {
        write: [],
        read: []
      },
      keys = [],
      all_keys = [],
      times = 0,
      readfd = fs.openSync(__dirname + '/data/read.bench', 'w'),
      writefd = fs.openSync(__dirname + '/data/write.bench', 'w'),
      compactfd = fs.openSync(__dirname + '/data/compact.bench', 'w');

  function iterateWrite(callback) {
    var waiting = dn,
        i = -1;

    function next() {
      i++;
      if (i >= dn) return callback();
      I.set(keys[i], keys[i], next);
    }

    next();
  };

  function iterateRead(callback) {
    var waiting = dn;
    for (var i = 0; i < dn; i++) {
      I.get(keys[i], function() {
        if (--waiting == 0) callback();
      });
    }
  };

  function iterate(callback) {
    var writeTotal,
        readTotal,
        compactTotal,
        start = +new Date;

    keys = [];
    for (var i = 0; i < dn; i++) {
      var ind = uuid();
      keys.push(ind);
      all_keys.push(ind);
    }
    
    iterateWrite(function() {
      writeTotal = +new Date - start;

      times++;
      fs.write(writefd, (offset + dn) + ',' + (1e3 * writeTotal / dn) + '\r\n');

      keys = [];

      for (var i = 0; i < dn; i++) {
        keys.push(all_keys[i * times]);
      }

      start = +new Date;
      iterateRead(function() {
        readTotal = +new Date - start;

        fs.write(readfd, (offset + dn) + ',' + (1e3 * readTotal / dn) + '\r\n');

        start = +new Date;
        I.compact(function() {
          compactTotal = +new Date - start;

          fs.write(compactfd, (offset + dn) + ',' + compactTotal + '\r\n');

          callback();
        });
      });
    });
  };

  function next() {
    offset += dn;
    if (offset < N) {
      iterate(next);
    } else {
      fs.close(readfd);
      fs.close(writefd);
      callback(null, results);
    }
  };
  
  iterate(next);

};

