/**
* Node index
*
* @author Fedor Indutny.
* Copyright 2011
*/

var step = require('step'),
    utils = require('./index/utils');

/** @const */
var DEFAULT_OPTIONS = {
  sort: function(a, b) {
    return (a === null || a < b) ?
              -1
              :
              (a === b ? 0 : 1);            
  },
  order: 33
};

/**
* Class @constructor
*/
var Index = exports.Index = function Index(options) {
  options = utils.merge(DEFAULT_OPTIONS, options);
  
  this.order = options.order;
  this.storage = options.storage ||
                 require('./index/memory-storage').createStorage();
  this.sort = options.sort;
  
  this.lockQueue = [];
};

/**
* Wrapper for class @constructor
*/
exports.createIndex = function createIndex(options) {
  return new Index(options);
};

/**
* Get functionality
*/
Index.prototype.get = require('./index/core/get').get;
Index.prototype.traverse = require('./index/core/get').traverse;
Index.prototype.rangeGet = require('./index/core/get').rangeGet;

/**
* Set functionality
*/
Index.prototype.set = require('./index/core/set').set;

/**
* Unset functionality
*/
Index.prototype.unset = require('./index/core/unset').unset;

/**
* Lock functionality
*/
Index.prototype.lock = function lock(fn) {
  if (this.locked) {
    this.lockQueue.push(fn);
    return true;
  };
  this.locked = true;
};

/**
* Release lock functionality
*/
Index.prototype.releaseLock = function release() {
  if (!this.locked) return;
  this.locked = false;

  var fn = this.lockQueue.shift();

  if (!fn && this.lockQueue.length <= 0) return;

  process.nextTick(function lockNextTick() {
    fn();
  });
};

