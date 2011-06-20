###
  Set functionality for Node index module

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

utils = require '../../index/utils'

###
  Set
###
exports.set = (key, value, _callback) ->
  sort = @sort
  order = @order
  storage = @storage
  conflictManager = @conflictManager

  if @lock(=> @set key, value, _callback)
    return

  callback = (err, data) =>
    @releaseLock()

    process.nextTick ->
      _callback and _callback err, data


  iterate = (page, callback) ->
    item_index = utils.search page, sort, key
    item = page[item_index]

    if item and not item[2]
      # Index

      # Read next page and try to insert kv in it
      step ->
        storage.read item[1], @parallel()
      , (err, page) ->
        if err
          throw err

        iterate page, @parallel()
      , (err, result) ->
        if err
          callback err
          return

        if storage.isPosition result
          # Page is just should be overwrited
          page[item_index][1] = result

          storage.write page, callback
        else
          #  Result is = {
          #    left_page: [...],
          #    middle_key: ...,
          #    right_page: [...]
          #  }

          page[item_index][1] = result.left_page
          page.splice item_index + 1, 0,
                      [result.middle_key, result.right_page]

          splitPage false, storage, order, page, callback

    else
      # Leaf
      step ->
        # Found dublicate
        if item and sort(item[0], key) is 0
          unless conflictManager
            throw 'Can\'t insert item w/ dublicate key'

          # Invoke conflictManager
          step ->
            storage.read item[1], @parallel()
          , (err, old_value) ->
            if err
              throw err

            @parallel() null, old_value
            conflictManager old_value, value, @parallel()
          , @parallel()

          return

        @parallel() null, value
      , (err, value, old_value) ->
        if err
          throw err

        # Value should be firstly written in storage
        item_index = if item_index is null then 0 else item_index + 1
        storage.write [value, old_value], @parallel()
      , (err, value) ->
        if err
          callback err
          return

        # Then inserted in leaf page
        page.splice item_index, 0, [key, value, 1]

        splitPage true, storage, order, page, callback


  step ->
    # Read initial data
    storage.readRoot @parallel()
  , (err, root) ->
    if err
      throw err

    # Initiate sequence
    iterate root, @parallel()
  , (err, result) ->
    if err
      throw err

    if storage.isPosition result
      # Write new root
      @parallel() null, result
    else
      # Split root
      storage.write [
        [null, result.left_page],
        [result.middle_key, result.right_page]
      ], @parallel()
  , (err, new_root_pos) ->
    if err
      throw err

    storage.writeRoot new_root_pos, @parallel()
  , callback

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
    step ->
      left_page = page[0...mid_index]
      storage.write left_page, @parallel()

      right_page = page[mid_index...]

      right_page[0][0] = null unless in_leaf

      storage.write right_page, @parallel()
    , (err, left_page, right_page) ->
      callback err, {
        left_page: left_page,
        middle_key: mid_key,
        right_page: right_page
      }

  else
    # Just overwrite it
    storage.write page, callback

