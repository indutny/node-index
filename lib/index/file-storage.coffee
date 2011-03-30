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
  padding: 16
  rootSize: 56
  partitionSize: 1024 * 1024 * 1024
  posBase: 36
  flushTimeout: 15000
  flushSize: 10000
  flushByteLimit: 10000000

###
  Class @constructor
###
Storage = exports.Storage = (options, callback) ->
  options = utils.merge DEFAULT_OPTIONS, options

  unless options.filename
    return callback 'Filename is required'

  {@posBase, @filename, @padding,
  @partitionSize, @rootSize, @flushTimeout,
  @flushSize} = options

  @flushTimeouted = false
  @_init callback

  flush = =>
    @flushTimer = setTimeout =>
      @flushTimeouted = true
    , @flushTimeout
    
  
  flush()

  # Return instance of self
  @

###
  Initializes storage
  May be used not from constructor
###
Storage::_init = (callback) ->
  @files = []
  @buffer = []
  @bufferMap = {}
  @bufferMapBytes = 0
  @filesOffset = 0

  @checkCompaction (err) =>
    if err
      return callback err

    @openFile (err) =>
      if err
        return callback err

      if @files.length <= 0
        # If no files - create one
        @createFile callback
      else
        callback null, @

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
Storage::nextFilename = (i, filename) ->
  index = i or @files.length
  index -= @filesOffset
  filename = filename or @filename
  if index > 0
    filename + '.' + index
  else
    filename

###
  Check if interrupted compaction is in place
###
Storage::checkCompaction = (callback) ->
  filename = @filename

  nextFilename = @nextFilename.bind @
  path.exists filename, (exists) =>
    path.exists filename + '.compact', (compactExists) =>
      if exists
        if compactExists
          # Probably compaction hasn't finished
          # Delete compaction files
          @iterateFiles filename + '.compact', (err, i) ->
            if err
              throw err

            if i isnt null
              compacted = nextFilename i, filename + '.compact'
              fs.unlink compacted, @parallel()
            else
              @parallel() null
        # Normal db file exists - use it
        callback null
      else
        if compactExists
          # Ok, compaction has finished
          # Move files
          @iterateFiles filename + '.compact', (err, i) ->
            if err
              throw err

            if i isnt null
              # still iterating
              compacted = nextFilename i, filename + '.compact'
              notcompacted = nextFilename i, filename
              _callback = @parallel()
              fs.unlink notcompacted, (err) ->
                if err and (not err.code or err.code isnt 'ENOENT')
                  _callback err
                else
                  fs.rename compacted, notcompacted, _callback
            else
              # finished iterating
              callback null
        else
          callback null


###
  Iterate through files in descending order
  filename.N
  filename.N - 1
  ...
  filename
###
Storage::iterateFiles = (filename, callback) ->
  files = []

  next = () =>
    _filename = @nextFilename files.length, filename
    step ->
      _callback = @parallel()
      path.exists _filename, (exists) ->
        _callback null, exists
      return
    , (err, exists) ->
      unless exists
        process.nextTick iterate
        return

      files.push files.length
      process.nextTick next

  iterate = () ->
    file = files.pop()
    unless file?
      file = null

    step ->
      @parallel() null, file
    , callback
    , (err) ->
      if err
        callback err
      else if file isnt null
        process.nextTick iterate
    
  next()

###
  Add index to the end of filename,
  open it if exists and store size
###
Storage::openFile = (callback) ->
  that = @

  padding = @padding
  files = @files
  filename = @nextFilename()
  file = {}

  step ->
    _callback = @parallel()
    path.exists filename, (exists) ->
      _callback null, exists
    return
  , (err, exists) ->
    if err
      @paralell() err
      return
    
    unless exists
      @parallel() null
      return

    fs.open filename, 'a+', 0666, @parallel()
    fs.stat filename, @parallel()

    return
  , (err, fd, stat) ->
    if err
      throw err

    unless fd and stat
      @parallel() null
      return

    index = files.length

    
    file =
      filename: filename
      fd: fd
      size: stat.size
      index: index

    if file.size % padding
      paddBuff = new Buffer padding - file.size % padding
      fs.write fd, paddBuff, 0, paddBuff.length,
               null, @parallel()
    else
      @parallel() null, 0

    @parallel() null, file

    files.push file

    return
  , (err, bytesWritten, file) ->
    if err
      throw err

    if bytesWritten?
      if file.size % padding
        if bytesWritten != padding - file.size % padding
          @parallel() 'Can\'t add padding to db file'
          return
        file.size += padding - file.size % padding
      that.openFile @parallel()
    else
      @parallel() null
  , callback

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
      index: @files.length - @filesOffset

    @files.push file

    unless writeRoot
      return callback null, @
    
    @root_pos = [
      '0',
      '2',
      '0'
    ]
    @root_pos_data = null

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

  posBase = @posBase
  s = parseInt pos[0], posBase
  l = parseInt pos[1], posBase
  f = parseInt pos[2], posBase

  file = @files[f || 0]
  buff = new Buffer l

  if not file
    return callback 'pos is incorrect'

  cachedVal = @bufferMap[pos.join '-']

  if cachedVal
    cachedVal = cachedVal.slice 0, l
    try
      cachedVal = JSON.parse cachedVal.toString()
      callback null, cachedVal
      return
    catch e

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
  data = new Buffer JSON.stringify data
  @_fsWrite data, callback

###
  Read root page
###
Storage::readRoot = (callback) ->
  if @root_pos_data
    callback null, @root_pos_data
    return

  cache_callback = (err, data) =>
    if err
      return callback err
    @root_pos_data = data
    callback null, data

  if @root_pos
    @read @root_pos, cache_callback
    return

  @readRootPos (err, pos) =>
    if err
      return callback err
    @read pos, cache_callback

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

    # If file has incorrect size (unpadded)
    # Substract difference from its size
    # B/c root can't be in that unpadded area
    offset = file.size - (file.size % @padding) -
             @rootSize + @padding

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

  @bufferedRoot = buff
  @_fsWrite buff, (err) =>
    if err
      return callback err

    @root_pos = root_pos
    @root_pos_data = null
    @_fsConditionalFlush callback

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

  @bufferMap[pos.join '-'] = buff
  @bufferMapBytes += buff.length

  callback null, pos

###
  Conditional flush
###
Storage::_fsConditionalFlush = (callback) ->
  if (@buffer.length > @flushSize) or @flushTimeouted or
     (@bufferMapBytes >  @flushByteLimit)
    @flushTimeouted = false
    @_fsFlush callback
  else
    callback null

###
  Low-level flush
###
Storage::_fsFlush = (callback) ->
  file = @currentFile()
  fd = file.fd

  if not @bufferedRoot
    return callback null

  root = @bufferedRoot

  buffer = @buffer

  len = -root.length
  buffer.forEach (buff) ->
    len += buff.length

  if len % @padding
    file.size += @padding - (len % @padding)
    len += @padding - (len % @padding)

  buff = new Buffer (len + root.length)

  offset = 0
  for i in [0...buffer.length]
    buffer[i].copy buff, offset
    offset += buffer[i].length

  root.copy(buff, len)

  @buffer = []
  @bufferedRoot = null
  @bufferMap = {}
  @bufferMapBytes = 0

  buffLen = buff.length
  fs.write fd, buff, 0, buffLen, null, (err, bytesWritten) =>
    if err or (bytesWritten isnt buffLen)
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

  clearTimeout @flushTimer

  @flushTimeouted = true

  @_fsConditionalFlush (err) =>
    if err
      return callback err

    step () ->
      group = @group()

      for i in [0...files.length]
        if files[i]
          fs.close files[i].fd, group()
    , callback

###
  Compaction flow actions
###
Storage::beforeCompact = (callback) ->
  @flushTimeouted = true
  @_fsConditionalFlush (err) =>
    if err
      return callback err

    @filesOffset = @files.length
    @filename += '.compact'
    @createFile false, callback

Storage::afterCompact = (callback) ->
  that = @
  filesOffset = @filesOffset
  files = @files
  @filename = @filename.replace /\.compact$/, ''

  step () ->
    for i in [0...files.length]
      fs.close files[i].fd, @parallel()
    return
  , (err) ->
    if err
      throw err

    for i in [0...filesOffset]
      fs.unlink files[i].filename, @parallel()
    return
  , (err) ->
    if err
      throw err
     
    fnsQueue = []
    compactedCount = files.length - filesOffset
    [0...compactedCount].forEach (i) ->
      compactedName = files[i + filesOffset].filename
      normalName = files[i].filename
      files[i] = files[i + filesOffset]
      files[i].filename = normalName

      fnsQueue.unshift (err) ->
        if err
          throw err
      
        fs.rename compactedName, normalName, @parallel()
        return

    fnsQueue.push @parallel()

    step.apply null, fnsQueue

    for i in [compactedCount...files.length]
      files.pop()

    that.filesOffset = 0
    return
  , (err) ->
    if err
      throw err

    [0...files.length].forEach (i) =>
      file = files[i]
      fn = @parallel()
      fs.open file.filename, 'a+', 0666, (err, fd) ->
        file.fd = fd
        fn err, file

    return
  , (err) ->
    if err
      step ->
        for i in [0...files.length]
          fs.close files[i].fd, @parallel()
        return
      , ->
        that._init @parallel()
        return
      , @parallel()
      return
    null
  , callback
