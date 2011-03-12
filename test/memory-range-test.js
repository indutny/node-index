var vows = require('vows'),
    assert = require('assert'),
    step = require('step');

var index = require('../lib/index');

var I;

vows.describe('Node index/memory range test').addBatch({
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
  'Getting items in range 500-600': {
    topic: function() {
      var promise = I.rangeGet(500, 600);

      var result = true,
          count = 0;
      promise.on('data', function(value) {
        count++;
        result = result && value >= 500 && value <= 600;
      });

      var callback = this.callback;
      promise.on('end', function() {
        callback(null, result && count);
      });

    },
    'should return correct values': function(result) {
      assert.ok(!!result);
      assert.equal(result, 101);
    }
  }
}).export(module);
