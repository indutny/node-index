###
  Node index - Main Library file

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

step = require 'step'
utils = require './index/utils'

DEFAULT_OPTIONS =
  sort: (a, b) ->
    if (a is null) or (a < b) then -1 else
      if a is b then 0 else 1
  order: 64


###
  Class @constructor
###
Index = exports.Index = (options) ->
  options = utils.merge DEFAULT_OPTIONS, options

  @storage = options.storage ||
                 require('./index/memory-storage').createStorage()
  {@order, @sort, @conflictManager} = options
  @order--

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
  Bulk functionality
###
Index::bulk = require('./index/core/bulk').bulk

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

  if not fn and @lockQueue.length <= 0
    return

  process.nextTick fn

###
  Export storages
###
exports.storage =
  memory: require('./index/memory-storage'),
  file: require('./index/file-storage')
