###
  Get functionality for Node Index module
###

utils = require '../../index/utils'
step = require 'step'

###
  Get value by key
###
exports.get = (key, callback) ->
  that = this
  sort = this.sort
  storage = this.storage
  efn = utils.efn callback

  iterate = efn (err, index) ->

    item_index = utils.search index, sort, key
    item = index[item_index]

    # Item not found
    unless item
      return callback 'Not found'
    
    value = item[1]

    if item[2]
      # Key Value - return value
      if sort(item[0], key) isnt 0
        return callback 'Not found'

      # Read actual value
      storage.read value, efn (err, value) ->
        # value = [value, link-to-previous-value]
        callback null, value[0]
    else
      # Key Pointer - go further
      storage.read value, iterate

  storage.readRoot iterate

###
  Traverse btree
  filter can call callback w/ following result:
    true - if `traverse` should go deeper
    undefined - if `traverse` should skip element
    false - if `traverse` should stop traversing and return to higher level
 
  filter can
  @return promise.
###

exports.traverse = (filter) ->
  that = this
  promise = new process.EventEmitter
  
  # If no filter were provided - match all
  filter = filter || (kp, callback) -> callback(null, true)

  process.nextTick ->
    efn = utils.efn (err) ->
      promise.emit 'error', err
      promise.emit 'end'
    
    sort = that.sort
    storage = that.storage

    iterate = (callback) ->
      efn (err, page) ->
        index = -1
        pagelen = page.length

        asyncFilter = ->
          if ++index >= pagelen
            if callback
              callback null
            else
              promise.emit 'end'
            
            return

          current = page[index]
          
          filter.call that, current, (err, filter_value) ->
            if filter_value is true
              if current[2]
                # emit value
                storage.read current[1], (err, value) ->
                  # value = [value, link-to-previous-value]
                  promise.emit 'data', value[0], current
                  asyncFilter()
                
              else
                # go deeper
                storage.read current[1], iterate asyncFilter
              
            else
              if filter_value is false
                index = pagelen
              
              asyncFilter()
            
        asyncFilter()

    storage.readRoot iterate()

  promise

###
  Get in range
###
exports.rangeGet = (start_key, end_key) ->
  sort = this.sort
  promise = new process.EventEmitter

  traverse_promise = this.traverse (kp, callback) ->
    start_cmp = sort kp[0], start_key
    end_cmp = sort kp[0], end_key

    if kp[2]
      if start_cmp >= 0 and end_cmp <= 0
        return callback null, true

      if end_cmp > 0
        return callback null, false
    else
      if end_cmp <= 0
        return callback null, true
      if end_cmp > 0
        return callback null, false
    
    callback null

  traverse_promise.on 'data', (value) ->
    promise.emit 'data', value

  traverse_promise.on 'end', ->
    promise.emit 'end'

  promise

