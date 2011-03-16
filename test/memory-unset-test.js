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
  'Unseting odd items': {
    topic: function() {
      step(function() {
        var group = this.group();

        for (var i = 0; i < items; i++) {
          if (!(i % 2)) continue;
          I.unset(i, group());
        };
      }, this.callback);
    },
    'should be successfull': function() {
    }
  }
}).addBatch({
  'Getting every even item': {
    topic: function() {
      step(function() {
        var group = this.group();

        for (var i = 0; i < items; i++) {
          if (i % 2) continue;
          I.get(i, group());
        }
      }, this.callback);
    },
    'should return only even items': function(values) {
      assert.ok(values.every(function(value) {
        return value % 2 == 0;
      }));
    }
  }
}).addBatch({
  'Getting any odd item': {
    topic: function() {
      step(function() {
        var group = this.group();

        for (var i = 0; i < items; i++) {
          if (!(i % 2)) continue;
          (function(i, callback) {
            I.get(i, function(err) {
              callback((!err) ? 'Found' : null);
            });
          })(i, group());
        }
      }, this.callback);
    },
    'should be unsuccessfull': function() {
    }
  }
}).addBatch({
  'Unsetting half of items': {
    topic: function() {
      step(function() {
        var group = this.group();

        var half = items >> 1;
        for (var i = 0; i < half; i++) {
          I.unset(i, group());
        }
      }, this.callback);
    },
    'should be successfull': function() {
    }
  }
}).addBatch({
  'Adding this half of items and all odd again': {
    topic: function() {
      step(function() {
        var group = this.group();

        var half = items >> 1;
        for (var i = 0; i < half; i++) {
          I.set(i, i, group());
        };

        for (var i = half; i < items; i++) {
          if (i % 2 == 0) continue;
          I.set(i, i, group());
        }
      }, this.callback);
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
