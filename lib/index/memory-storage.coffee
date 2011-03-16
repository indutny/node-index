###
  Memory storage for Node Index Module
###

util = require 'util'
step = require 'step'

###
  Class @constructor
###
Storage = exports.Storage = (options) ->
  this.data = [
    []
  ]
  this.root_pos = new Position 0
  this

###
  @constructor wrapper
###
exports.createStorage = (options) ->
  return new Storage options

isPosition = Storage.prototype.isPosition = (pos) ->
  return pos instanceof Position

Storage.prototype.read = (pos, callback) ->
  unless isPosition pos
    return callback 'pos should be a valid position'

  that = this
  process.nextTick ->
    callback null, that.data[pos.index]
  

Storage.prototype.write = (data, callback) ->
  that = this
  
  process.nextTick ->
    callback null, new Position(that.data.push(data) - 1)

Storage.prototype.readRoot = (callback) ->
  that = this
  process.nextTick ->
    callback null, that.data[that.root_pos.index]

Storage.prototype.writeRoot = (root_pos, callback) ->
  unless isPosition root_pos
    return callback 'pos should be a valid position'

  that = this
  process.nextTick ->
    that.root_pos = root_pos
    callback null

Storage.prototype.inspect = ->
  this.data.forEach (line, i) ->
    util.puts i + ': ' + JSON.stringify line

  util.puts 'Root : ' + JSON.stringify this.root_pos

Position = exports.Position = (index) ->
  this.index = index
  this

Storage.prototype.beforeCompact = ->
  this._compactEdge = this.data.push '--------'

Storage.prototype.afterCompact = ->
  this.data[i] = 0 for i in [0..this._compactEdge]

