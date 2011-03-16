###
  Set functionality for Node index module
###

step = require 'step'

utils = require '../../index/utils'

###
  Set
###
exports.set = (key, value, _callback) ->
  that = this
  sort = this.sort
  order = this.order
  storage = this.storage

  if this.lock(() -> that.set key, value, _callback)
    return
  
  callback = (err, data) ->
    that.releaseLock()

    process.nextTick () ->
      _callback && _callback err, data
  

  efn = utils.efn callback

  iterate = (page, callback) ->
    item_index = utils.search page, sort, key
    item = page[item_index]

    if item and not item[2]
      # Index

      # Read next page and try to insert kv in it
      step (() ->
        storage.read item[1], this.parallel()
      ), efn((err, page) ->
        iterate page, this.parallel()
      ), efn((err, result) ->
        if storage.isPosition result
          # Page is just should be overwrited
          page[item_index][1] = result

          storage.write page, callback
        else
          ###
          Result is = {
             left_page: [...],
             middle_key: ...,
             right_page: [...]
          }
          ###

          page[item_index][1] = result.left_page
          page.splice item_index + 1, 0,
                      [result.middle_key, result.right_page]

          splitPage false, storage, order, page, callback
      )
    else
      # Leaf
      step (() ->
        # Found dublicate
        if item and sort(item[0], key) is 0
          if not that.conflictManager
            this.parallel() 'Can\'t insert item w/ dublicate key'
            return

          # Invoke conflictManager
          step(
            () ->
              storage.read item[1], this.parallel()
            ,
            efn((err, old_value) ->
              this.parallel() null, old_value
              that.conflictManager old_value, value, this.parallel()
            ),
            this.parallel()
          )

          return

        this.parallel() null, value
      ), efn((err, value, old_value) ->
        # Value should be firstly written in storage
        item_index = if item_index is null then 0 else item_index + 1
        storage.write [value, old_value], this.parallel()
      ), efn((err, value) ->
        # Than inserted in leaf page
        page.splice item_index, 0, [key, value, 1]

        splitPage true, storage, order, page, callback
      )

  step (() ->
    # Read initial data
    storage.readRoot this.parallel()
  ), efn((err, root) ->
    # Initiate sequence
    iterate root, this.parallel()
  ), efn((err, result) ->
    if storage.isPosition result
      # Write new root
      this.parallel() null, result
    else
      # Split root
      storage.write [
        [null, result.left_page],
        [result.middle_key, result.right_page]
      ], this.parallel()
  ), efn((err, new_root_pos) ->
    storage.writeRoot new_root_pos, this.parallel()
  ), efn(callback)

###
  Check page length
  If exceed - split it into two and return left_page, right_page, middle_key
###
splitPage = (in_leaf, storage, order, page, callback) ->
  # If item needs to be splitted
  if page.length > order
    mid_index = page.length >> 1
    mid_key = page[mid_index][0]

    # Write splitted pages
    step (() ->
      left_page = page.slice 0, mid_index
      storage.write left_page, this.parallel()

      right_page = page.slice mid_index

      if not in_leaf
        right_page[0][0] = null

      storage.write right_page, this.parallel()
    ), ((err, left_page, right_page) ->
      callback err, {
        left_page: left_page,
        middle_key: mid_key,
        right_page: right_page
      }
    )

  else
    # Just overwrite it
    storage.write page, callback

