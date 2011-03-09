var vows = require('vows'),
    assert = require('assert'),
    step = require('step');

var index = require('../lib/index'),
    memoryStorage = require('../lib/index/memory-storage');

var I;

vows.describe('Node index/memory basic test').addBatch({
  'Creating new index': {
    topic: function() {
      return index.createIndex({
        storage: memoryStorage.createStorage()
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
        I.set(6, 6, group());
        I.set(-1, -1, group());
      }, this.callback);
    },
    'should be successfull': function() {
    }
  }
}).addBatch({
  'Getting one of them': {
    topic: function() {
      I.get(4, this.callback);
    },
    'should return correct value': function(value) {
      assert.equal(value, 4);
    }
  }
}).export(module);

