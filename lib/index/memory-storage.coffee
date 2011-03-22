###
  Memory storage for Node Index Module

  This software is licensed under the MIT License.

  Copyright Fedor Indutny, 2011.

  Permission is hereby granted, free of charge, to any person obtaining a
  copy of this software and associated documentation files (the
  "Software"), to deal in the Software without restriction, including
  without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to permit
  persons to whom the Software is furnished to do so, subject to the
  following conditions:

  The above copyright notice and this permission notice shall be included
  in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
  OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
  OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
  USE OR OTHER DEALINGS IN THE SOFTWARE.
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

  process.nextTick =>
    callback null, @data[pos.index]

Storage::write = (data, callback) ->
  process.nextTick =>
    callback null, new Position(@data.push(data) - 1)

Storage::readRoot = (callback) ->
  process.nextTick =>
    callback null, @data[@root_pos.index]

Storage::writeRoot = (root_pos, callback) ->
  unless isPosition root_pos
    return callback 'pos should be a valid position'

  process.nextTick =>
    @root_pos = root_pos
    callback null

Storage::inspect = ->
  @data.forEach (line, i) ->
    util.puts i + ': ' + JSON.stringify line

  util.puts 'Root : ' + JSON.stringify @root_pos

Position = exports.Position = (@index) ->
  @

###
  Storage state
###
Storage::getState = ->
  {}

Storage::setState = (state) ->
  true

###
  Compaction flow
###

Storage::beforeCompact = (callback) ->
  @_compactEdge = @data.push '--------'
  callback null

Storage::afterCompact = (callback) ->
  @data[i] = 0 for i in [0...@_compactEdge]
  callback null

