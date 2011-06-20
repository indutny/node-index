###
  Unset functionality for Node index module

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

exports.unset = (key, _callback) ->
  callback = (err, data) =>
    @releaseLock()

    process.nextTick ->
      _callback and _callback err, data

  storage = @storage
  sort = @sort

  if @lock(=> @unset key, _callback)
    return

  iterate = (page, callback) ->
    item_index = utils.search page, sort, key
    item = page[item_index]

    if item_index is null
      # Not found
      # Even in that case unset should be successfull
      callback null
      return

    if item[2]
      # Leaf

      if sort(item[0], key) isnt 0
        # Actually key doesn't match
        # So do nothing
        callback null
        return

      # Delete from leaf and if one will be empty
      # remove leaf from parent
      page.splice item_index, 1
      if page.length > 0
        # If resulting page isn't empty
        step ->
          storage.write page, @parallel()
        , callback
        return

      # Notify that item should be removed from parent index
      callback null, false
    else
      # Index page
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

        if result is false
          # Delete item from index page
          page.splice item_index, 1
          if page.length <= 1
            callback null, page[0][1]
            return
        else if storage.isPosition result
          page[item_index][1] = result
        else
          callback null
          return

        step ->
          storage.write page, @parallel()
        , callback


  step ->
    storage.readRoot @parallel()
  , (err, root) ->
    if err
      throw err

    iterate root, @parallel()
  , (err, result) ->
    if err
      throw err

    if result is false
      # Create new root
      storage.write [], @parallel()
    else if storage.isPosition(result)
      # Overwrite old root
      @parallel() null, result
    else
      @parallel() null
  , (err, position) ->
    if err
      throw err

    if storage.isPosition position
      storage.writeRoot position, @parallel()
    else
      @parallel() null
  , callback

