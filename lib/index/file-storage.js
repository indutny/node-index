/**
* Memory storage for Node Index Module
*/

var fs = require('fs'),
    util = require('util'),
    step = require('step');

/**
* Class @constructor
*/
var Storage = exports.Storage = function Storage(options) {
  
};

/**
* @constructor wrapper
*/
exports.createStorage = function(options) {
  return new Storage(options);
};

/**
* pos will have following structure
* {
*   f: file-number || undefined,
*   s: start-byte,
*   l: length-of-block
* }
*/
var isPosition = Storage.prototype.isPosition = function isPosition(pos) {
  return pos.f || pos.s || pos.l;
};

Storage.prototype.read = function read(pos, callback) {
  if (!isPosition(pos)) return callback('pos should be a valid position');

  var that = this;
  fs.read(...);
};

Storage.prototype.write = function write(data, callback) {
  var that = this;

  this._fsWrite(data, function() {
  });
};

Storage.prototype.readRoot = function readRoot(callback) {
  var that = this;
};

Storage.prototype.writeRoot = function writeRoot(root_pos, callback) {
  if (!isPosition(root_pos)) return callback('pos should be a valid position');
  var that = this;
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
