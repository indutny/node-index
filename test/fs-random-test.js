var vows = require('vows'),
    assert = require('assert'),
    step = require('step'),
    coffee = require('coffee-script'),
    fs = require('fs');

var index = require('../lib/index'),
    FileStorage = require('../lib/index/file-storage');

var I,
    fileStorage,
    filename = __dirname + '/data/frt.db',
    num = 10000,
    nums = [],
    queue = [];

for (var i = 0; i < num; i++) {
  nums[i] = i;
}

function randomize(nums) {
  nums = [].concat(nums);
  var len = nums.length,
      random_nums = [];
  while (len > 0) {
    var i = Math.floor(Math.random() * len);
    random_nums.push(nums[i]);
    nums.splice(i, 1);
    len--;
  }

  return random_nums;
}

function run_monkey(prefix, callback) {
  var fns = randomize(nums).map(function(num) {
    num = prefix + num;
    return [
      function(err) {
        if (err) return this.parallel()(err);

        I.set(num, num, this.parallel());
      },
      function(err) {
        if (err) return this.parallel()(err);
        
        I.get(num, this.parallel());
      },
      function(err, value) {
        if (err) return this.parallel()(err);
        if (value != num) return this.parallel()('Read failed');

        if (Math.random() > 0.5) {
          I.unset(num, this.parallel());
        }
        this.parallel()(null);
      }
    ];
  }).reduce(function(prev, fns) {
    return prev.concat(fns);
  }, []);

  fns.push(callback);
  step.apply(null, fns);
}

vows.describe('Node index/fs random test').addBatch({
  'Creating new file storage': {
    topic: function() {
      try {
        fs.unlinkSync(filename);
        for (var i = 0; i < 100; i++) {
          fs.unlinkSync(filename + '.' + i);
        }
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
  'Monkey insertion, read, deletion': {
    topic: function(_I) {
      step(function() {
        var group = this.group();

        for (var i = 0; i < 10; i++) {
          setTimeout(function(fn, prefix) {
            console.log('%d monkey has traveled to the space', prefix);
            run_monkey(prefix + ':', fn);
          }, 10 * i, group(), i);
        }
      }, this.callback);
    },
    'should end successfully': function() {
    }
  }
}).export(module);
