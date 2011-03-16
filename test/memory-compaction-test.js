var vows = require('vows'),
    assert = require('assert'),
    step = require('step'),
    coffee = require('coffee-script');

var index = require('../lib/index');

var I;

var items = 10000;

vows.describe('Node index/memory unset test').addBatch({
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

        for (var i = 0; i < items; i++) {
          I.set(i, i, group());
        };
      }, this.callback);
    },
    'should be successfull': function() {
    }
  }
}).addBatch({
  'Running compaction for memory storage': {
    topic: function() {
      I.compact(this.callback);
    },
    'should be successfull': function() {
    }
  }
}).addBatch({
  'Search for every item': {
    topic: function() {
      step(function() {
        var group = this.group();

        for (var i = 0; i < items; i++) {
          I.get(i, group());
        }
      }, this.callback);
    },
    'should be successfull': function(values) {
    }
  }
}).export(module);
