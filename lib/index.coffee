###
  Node index

  @author Fedor Indutny.
  Copyright 2011
###

step = require 'step'
utils = require './index/utils'

DEFAULT_OPTIONS =
  sort: (a, b) ->
    if (a is null) or (a < b)
      -1
    else
      if a is b
        0
      else
        1
  order: 33


###
  Class @constructor
###
Index = exports.Index = (options) ->
  options = utils.merge DEFAULT_OPTIONS, options
  
  @order = options.order
  @storage = options.storage ||
                 require('./index/memory-storage').createStorage()
  @sort = options.sort
  
  @lockQueue = []

  @

###
  Wrapper for class @constructor
###
exports.createIndex = (options) ->
  new Index options

###
 Get functionality
###
Index::get = require('./index/core/get').get
Index::traverse = require('./index/core/get').traverse
Index::rangeGet = require('./index/core/get').rangeGet

###
  Set functionality
###
Index::set = require('./index/core/set').set

###
  Unset functionality
###
Index::unset = require('./index/core/unset').unset

###
  Compaction functionality
###
Index::compact = require('./index/core/compact').compact

###
  Lock functionality
###
Index::lock = (fn) ->
  if @locked
    @lockQueue.push fn
    return true

  @locked = true
  false

###
  Release lock functionality
###
Index::releaseLock = ->
  unless @locked
    return

  @locked = false

  fn = @lockQueue.shift()

  unless fn or @lockQueue.length > 0
    return

  process.nextTick fn

