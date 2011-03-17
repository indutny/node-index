###
  Memory storage for Node Index Module
###

util = require 'util'
step = require 'step'

###
  Class @constructor
###
Storage = exports.Storage = (options) ->
  @data = [
    []
  ]
  @root_pos = new Position 0
  @

###
  @constructor wrapper
###
exports.createStorage = (options) ->
  return new Storage options

Storage::isPosition = isPosition = (pos) ->
  return pos instanceof Position

Storage::read = (pos, callback) ->
  unless isPosition pos
    return callback 'pos should be a valid position'

  that = @
  process.nextTick ->
    callback null, that.data[pos.index]
  

Storage::write = (data, callback) ->
  that = @
  
  process.nextTick ->
    callback null, new Position(that.data.push(data) - 1)

Storage::readRoot = (callback) ->
  that = @
  process.nextTick ->
    callback null, that.data[that.root_pos.index]

Storage::writeRoot = (root_pos, callback) ->
  unless isPosition root_pos
    return callback 'pos should be a valid position'

  that = @
  process.nextTick ->
    that.root_pos = root_pos
    callback null

Storage::inspect = ->
  @data.forEach (line, i) ->
    util.puts i + ': ' + JSON.stringify line

  util.puts 'Root : ' + JSON.stringify @root_pos

Position = exports.Position = (@index) ->
  @

Storage::beforeCompact = ->
  @_compactEdge = @data.push '--------'

Storage::afterCompact = ->
  @data[i] = 0 for i in [0..@_compactEdge]

