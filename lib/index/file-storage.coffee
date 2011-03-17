###
  File storage for Node Index Module

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
fs = require 'fs'
path = require 'path'
Buffer = require('buffer').Buffer

###
  Default storage options
###
DEFAULT_OPTIONS =
  filename: ''
  padding: 64

###
  Class @constructor
###
Storage = exports.Storage = (options, callback) ->
  options = utils.merge DEFAULT_OPTIONS options
  @files = []

  unless options.filename
    return callback 'Filename is required'

  @filename = options.filename
  @padding = options.padding

  @openFile (err) =>
    if err return callback err

    if @files.length <= 0
      # If no files - create one
      @createFile callback
    else
      callback null @

  # Return instance of self
  @

###
  @constructor wrapper
###
exports.createStorage = (options, callback) ->
  return new Storage options, callback

###
  pos = {
    f: file-index or undefined
    s: start-offset,
    l: length
  }
###
Storage::isPosition = isPosition = (pos) ->
  pos.s and pos.l and ok

###
  Adds index to the end of file
  and return next (unopened) filename
###
Storage::nextFilename = ->
  index = @files.length
  if index > 0 then
    @filename + '.' + index
  else
    @filename

###
  Add index to the end of filename,
  open it if exists and store size
###
Storage::openFile = (callback) ->
  that = @
  efn = utils.efn callback

  filename = @nextFilename()

  step (->
    path.exists filename, @parallel()
  ), efn((err, exists) ->
    unless exists
      return callback(null)

    fs.open filename 'a+' 0666 @parallel()
    fs.stat filename, @parallel()
  ), efn((err, fd, stat) ->
    file = fd: fd, size: stat.size
    that.files.push file
    that.openFile that.filename @parallel()
  ), callback

###
  Create file
###
Storage::createFile = (callback) ->
  filename = @nextFilename()

  fs.open filename 'w+' 0666 (err, fd) =>
    if err return callback err

    file = fd: fd, size: 0
    @files push file
    callback null @

###
  Read data from position
###
Storage::read = (pos, callback) ->
  unless isPosition pos
    return callback 'pos should be a valid position'

  file = @files[pos.f || 0]
  buff = new Buffer pos.l
  efn = utils.efn callback

  fs.read file, buff, 0, pos.l, pos.s, efn((err, bytesRead) ->
    unless bytesRead == pos.l
      return callback 'Read less bytes than expected'
    try
      buff = JSON.parse(buff.toString())
      err = null
    catch
      err = 'Data is not a valid json'

    callback err buff
  )

###
  Write data and return position
###
Storage::write = (data, callback) ->
  process.nextTick =>
    callback null, new Position(@data.push(data) - 1)

###
  Read root page
###
Storage::readRoot = (callback) ->
  @readRootPos (err, pos) =>
    if err return callback err
    @read pos callback

###
  Find last root in files and return it to callback
###
Storage::readRootPos = (callback) ->
  null

###
  Write root page
###
Storage::writeRoot = (root_pos, callback) ->
  unless isPosition root_pos
    return callback 'pos should be a valid position'

  process.nextTick =>
    @root_pos = root_pos
    callback null

###
  Compaction flow actions
###
Storage::beforeCompact = ->
  "before compact"

Storage::afterCompact = ->
  "after compact"

