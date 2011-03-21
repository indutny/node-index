###
  File storage for Node Index Module

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

utils = require './utils'
step = require 'step'
fs = require 'fs'
path = require 'path'
Buffer = require('buffer').Buffer

###
  Default storage options
###
DEFAULT_OPTIONS =
  filename: ''
  padding: 32
  rootSize: 64
  partitionSize: 1024 * 1024 * 1024
  posBase: 36

###
  Class @constructor
###
Storage = exports.Storage = (options, callback) ->
  options = utils.merge DEFAULT_OPTIONS, options
  @files = []

  unless options.filename
    return callback 'Filename is required'

  {@posBase, @filename, @padding, @partitionSize, @rootSize} = options

  @buffer = []

  @openFile (err) =>
    if err
      return callback err

    if @files.length <= 0
      # If no files - create one
      @createFile callback
    else
      callback null, @

  # Return instance of self
  @

###
  @constructor wrapper
###
exports.createStorage = (options, callback) ->
  return new Storage options, callback

###
  pos = [
    start-offset,
    length
    file-index or undefined
  ]
###
Storage::isPosition = isPosition = (pos) ->
  pos? and Array.isArray(pos) and pos.length is 3 and true

###
  Adds index to the end of file
  and return next (unopened) filename
###
Storage::nextFilename = (i) ->
  index = i || @files.length
  if index > 0
    @filename + '.' + index
  else
    @filename

###
  Add index to the end of filename,
  open it if exists and store size
###
Storage::openFile = (callback) ->
  that = @

  filename = @nextFilename()

  step (->
    _callback = @parallel()
    path.exists filename, (exists) ->
      _callback null, exists
    return
  ), ((err, exists) ->
    if err
      @paralell() err
      return
    
    unless exists
      @parallel() null
      return

    fs.open filename, 'a+', 0666, @parallel()
    fs.stat filename, @parallel()

    return
  ), ((err, fd, stat) ->
    if err
      @parallel() err
      return

    unless fd and stat
      @parallel() null
      return

    index = that.files.length

    file =
      filename: filename
      fd: fd
      size: stat.size
      index: index

    that.files.push file
    that.openFile @parallel()
    return
  ), callback

###
  Create file
###
Storage::createFile = (writeRoot, callback) ->
  filename = @nextFilename()

  unless callback?
    callback = writeRoot
    writeRoot = true

  fs.open filename, 'w+', 0666, (err, fd) =>
    if err
      return callback err

    file =
      filename: filename
      fd: fd
      size: 0
      index: @files.length

    @files.push file

    unless writeRoot
      return callback null, @
    
    # Write new root
    @write [], (err, pos) =>
      if err
        return callback err

      @writeRoot pos, (err) =>
        if err
          return callback err
        
        callback null, @

###
  Read data from position
###
Storage::read = (pos, callback) ->
  unless isPosition pos
    return callback 'pos should be a valid position (read)'

  s = parseInt pos[0], @posBase
  l = parseInt pos[1], @posBase
  f = parseInt pos[2], @posBase

  file = @files[f || 0]
  buff = new Buffer l

  fs.read file.fd, buff, 0, l, s, (err, bytesRead) ->
    if err
      return callback err

    unless bytesRead == l
      return callback 'Read less bytes than expected'

    try
      buff = JSON.parse buff.toString()
      err = null
    catch e
      err = 'Data is not a valid json'

    callback err, buff

###
  Write data and return position
###
Storage::write = (data, callback) ->
  data = JSON.stringify data
  data = new Buffer data
  @_fsWrite data, callback

###
  Read root page
###
Storage::readRoot = (callback) ->
  if @root_pos
    @read @root_pos, callback
    return

  @readRootPos (err, pos) =>
    if err
      return callback err
    @read pos, callback

###
  Find last root in files and return it to callback

  it will be synchronous for now
  TODO: make it asynchronous
###
Storage::readRootPos = (callback) ->
  iterate = (index, callback) =>
    file = @files[index]
    unless file
      return callback 'root not found'

    buff = new Buffer @rootSize

    offset = file.size - @rootSize + @padding
    while (offset -= @padding) >= 0
      bytesRead = fs.readSync file.fd, buff, 0, @rootSize, offset
      unless bytesRead == @rootSize
        # Header not found
        offset = -1
        break

      if data = checkHash buff
        root = data.split('\n', 1)[0]
        try
          root = JSON.parse root
        catch e
          # Header is not JSON
          # Try in previous file
          offset = -1
          break
        return callback null, root

    process.nextTick () ->
      iterate (index - 1), callback
  
  checkHash = (buff) ->
    hash = buff.slice(0, utils.hash.len).toString()
    rest = buff.slice(utils.hash.len)
    rest.toString() if hash == utils.hash rest

  iterate (@files.length - 1), callback

###
  Write root page
###
Storage::writeRoot = (root_pos, callback) ->
  unless isPosition root_pos
    return callback 'pos should be a valid position (writeRoot)'

  _root_pos = JSON.stringify root_pos
  _root_pos_len = Buffer.byteLength _root_pos
  _padding_len = @rootSize - _root_pos_len
  _root_pos = [_root_pos].concat(new Array _padding_len).join '\n'
  buff = new Buffer @rootSize
  buff.write _root_pos, utils.hash.len

  hash = utils.hash buff.slice utils.hash.len
  buff.write hash, 0, 'binary'

  @_fsWrite buff, (err) =>
    if err
      return callback err

    @root_pos = root_pos
    @_fsFlush callback

###
  Low-level write

  buff - is Buffer
  
  Not writes data, but put it into @buffer
###
Storage::_fsWrite = (buff, callback) ->
  file = @currentFile()

  pos = [
    file.size.toString(@posBase),
    buff.length.toString(@posBase),
    file.index.toString(@posBase)
  ]

  file.size += buff.length

  @buffer.push buff

  callback null, pos

###
  Low-level flush
###
Storage::_fsFlush = (callback) ->
  file = @currentFile()
  fd = file.fd

  root = @buffer.pop()

  len = 0
  @buffer.forEach (buff) ->
    len += buff.length

  if len % @padding
    file.size += @padding - (len % @padding)
    len += @padding - (len % @padding)

  buff = new Buffer (len + root.length)

  @buffer.reduce ((prev, curr) ->
    curr.copy(buff, prev)
    prev + curr.length
  ), 0

  root.copy(buff, len)
  @buffer = []
 
  fs.write fd, buff, 0, buff.length, null, (err, bytesWritten) =>
    if err or (bytesWritten isnt buff.length)
      @_fsCheckSize (err2) ->
        callback err2 or err or 'Written less bytes than expected'
      return

    if file.size >= @partitionSize
      @createFile false, (err) ->
        callback err
    else
      callback null

###
  Recheck current file's length
###
Storage::_fsCheckSize = (callback)->
  file = @currentFile()
  filename = @nextFilename @file.index
  fs.stat filename, (err, stat) =>
    if err
      return callback err
    
    file.size = stat.size
    callback null

###
  Current file
###
Storage::currentFile = ->
  @files[@files.length - 1]

###
  Close all fds
###
Storage::close = (callback) ->
  files = @files

  step (() ->
    group = @group()

    len = files.length
    i = 0
    while i < len
      fs.close files[i].fd, group()
      i++
  ), callback

###
  Storage state
###
Storage::getState = () ->
  padding: @padding
  rootSize: @rootSize
  posBase: @posBase

Storage::setState = (state) ->
  {@padding, @posBase, @rootSize} = state

###
  Compaction flow actions
###
Storage::beforeCompact = (callback) ->
  @compact_index = @files.length
  @createFile false, callback

Storage::afterCompact = (callback) ->
  compact_index = @compact_index
  files = @files

  step (() ->
    group = @group()
    i = 0
    while i < compact_index
      fs.truncate files[i].fd, group()
      i++
  ), ((err) ->
    if err
      return @parallel() err

    group = @group()
    i = 0
    while i < compact_index
      fs.close files[i].fd, group()
      i++
  ), callback

