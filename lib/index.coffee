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
  
  this.order = options.order
  this.storage = options.storage ||
                 require('./index/memory-storage').createStorage()
  this.sort = options.sort
  
  this.lockQueue = []

  this

###
  Wrapper for class @constructor
###
exports.createIndex = (options) ->
  new Index options

###
 Get functionality
###
Index.prototype.get = require('./index/core/get').get
Index.prototype.traverse = require('./index/core/get').traverse
Index.prototype.rangeGet = require('./index/core/get').rangeGet

###
  Set functionality
###
Index.prototype.set = require('./index/core/set').set

###
  Unset functionality
###
Index.prototype.unset = require('./index/core/unset').unset

###
  Compaction functionality
###
Index.prototype.compact = require('./index/core/compact').compact

###
  Lock functionality
###
Index.prototype.lock = (fn) ->
  if this.locked
    this.lockQueue.push fn
    return true

  this.locked = true
  false


###
  Release lock functionality
###
Index.prototype.releaseLock = () ->
  if not this.locked
    return

  this.locked = false

  fn = this.lockQueue.shift()

  if not fn and this.lockQueue.length <= 0
    return

  process.nextTick fn

