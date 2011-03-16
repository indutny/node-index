var vows = require('vows'),
    assert = require('assert'),
    step = require('step'),
    coffee = require('coffee-script');

var index = require('../lib/index');

var I;

vows.describe('Node index/memory traverse test').addBatch({
  'Creating new index': {
    topic: function() {
      return index.createIndex();
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

        for (var i = 0; i < 10000; i++) {
          I.set(i, i, group());
        };
      }, this.callback);
    },
    'should be successfull': function() {
    }
  }
}).addBatch({
  'Traversing through them': {
    topic: function() {
      var promise = I.traverse(function(kp, callback) {
        if (!kp[2] || kp[0] % 2) return callback(null, true);

        callback(null);
      });

      var result = true,
          count = 0;
      promise.on('data', function(value, kp) {
        count ++;
        result = result && (value % 2 == 1);
      });

      var callback = this.callback;
      promise.on('end', function() {
        callback(null, result && count);
      });

    },
    'should return correct value': function(result) {
      assert.ok(!!result);
      assert.equal(result, 5000);
    }
  }
}).export(module);
