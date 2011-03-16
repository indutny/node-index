/**
* Memory storage for Node Index Module
*/

var util = require('util'),
    step = require('step');

/**
* Class @constructor
*/
var Storage = exports.Storage = function Storage(options) {
  this.data = [
    []
  ];
  this.root_pos = new Position(0);
};

/**
* @constructor wrapper
*/
exports.createStorage = function(options) {
  return new Storage(options);
};

var isPosition = Storage.prototype.isPosition = function isPosition(pos) {
  return pos instanceof Position;
};

Storage.prototype.read = function read(pos, callback) {
  if (!isPosition(pos)) return callback('pos should be a valid position');

  var that = this;
  process.nextTick(function() {
    callback(null, that.data[pos.index]);
  });
};

Storage.prototype.write = function write(data, callback) {
  var that = this;
  process.nextTick(function() {
    callback(null, new Position(that.data.push(data) - 1));
  });
};

Storage.prototype.readRoot = function readRoot(callback) {
  var that = this;
  process.nextTick(function() {
    callback(null, that.data[that.root_pos.index]);
  });
};

Storage.prototype.writeRoot = function writeRoot(root_pos, callback) {
  if (!isPosition(root_pos)) return callback('pos should be a valid position');

  var that = this;
  process.nextTick(function() {
    that.root_pos = root_pos;
    callback(null);
  });
};

Storage.prototype.inspect = function inspect() {
  this.data.forEach(function(line, i) {
    util.puts(i + ': ' + JSON.stringify(line));
  });

  util.puts('Root : ' + JSON.stringify(this.root_pos));
};

var Position = exports.Position = function Position(index) {
  this.index = index;
};

Storage.prototype.beforeCompact = function() {
  this._compactEdge = this.data.push('--------');
};

Storage.prototype.afterCompact = function() {
  for (var i = 0; i < this._compactEdge; i++) {
    this.data[i] = 0;
  }
};

