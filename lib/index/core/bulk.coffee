###
  Bulk set/unset functionality for Node index module

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
  Bulk set/unset

  kvs = [
    [key, value, 1] - set
    [key] - unset
  ]
###
exports.bulk = (kvs, _callback) ->
  that = @
  sort = @sort
  order = @order
  storage = @storage
  conflictManager = @conflictManager

  if @lock(=> @bulk kvs, _callback)
    return

  callback = (err, data) =>
    @releaseLock()

    process.nextTick ->
      _callback and _callback err, conflicts

  # conflicts
  conflicts = []

  # clone kvs
  kvs = [].concat kvs
  # sort kvs
  kvs = kvs.sort (a, b) -> sort a[0], b[0]

  unless kvs.length > 0
    return callback null, []

  # do bulk ops on page
  # kvs is part of original kvs that matches page range
  # (range is stored in parent page of course)
  #
  # return values
  #   null, [array of kps] = [ [null, pos], [key1, pos], ...]
  #   null, []
  iterate = (callback, kvs) ->
    _iterate = (err, page) ->
      if err
        return callback err

      kv = kvs.shift()

      if not kv
        # all kvs has been processed split or delete page
        splitPage page, order, storage, callback
        return

      index = utils.search page, sort, kv[0]
      item = page[index]

      if item and not item[2]
        # index page

        kvs.unshift kv
        if page[index + 1]
          kv_index = utils.search kvs, sort, page[index + 1][0]

          if not kvs[kv_index]
            kv_index++
          else if sort(kvs[kv_index][0], page[index + 1][0]) isnt 0
            kv_index++
          _kvs = kvs.splice 0, kv_index
        else
          _kvs = kvs

        step () ->
          storage.read item[1], @parallel()
        , iterate(
          (err, kps) ->
            if err
              return callback err

            if kps.length == 0
              page.splice index, 1
            else
              kps[0][0] = item[0]
              page.splice.apply page, [index, 1].concat kps

            _iterate(null, page)
          , _kvs
        )
      else
        # leaf page
        # so insert all kvs here
        # and remove
        if not item or (sort item[0], kv[0]) < 0
          if kv[2]
            # just insert item and continue iterating
            index = if index is null then 0 else index + 1
            page.splice index, 0, kv
            storage.write [kv[1],], (err, pos) ->
              kv[1] = pos
              process.nextTick ->
                _iterate null, page

            return
          else
            # do nothing, b/c item not found and we can't delete it
        else
          # ok, item exists
          if not kv[2]
            # if we're removing that's ok
            # just remove
            page.splice index, 1
          else
            # manage conflicts if inserting
            if conflictManager
              # TODO: write conflictManager
              conflicts.push kv[0]
            else
              conflicts.push kv[0]

        process.nextTick ->
          _iterate null, page

  step ->
    storage.readRoot @parallel()
  , iterate(
    recSplit = (err, page) ->
      if err
        return callback err

      if page.length == 0
        storage.write [], (err, pos) ->
          if err
            return callback err
          storage.writeRoot pos, callback
      else if page.length == 1
        storage.writeRoot page[0][1], callback
      else
        splitPage page, order, storage, recSplit
    , kvs
  )

splitPage = (page, order, storage, callback) ->
  if page.length == 0
    pages = []
  else if page.length <= order
    pages = [page]
  else
    pages = []
    i = 0
    len = page.length
    while len > order
      len = len >> 1

    while page.length > 0
      pages.push page.splice 0, len

  step ->
    group = @group()

    for page in pages
      pre_page = [].concat page
      if not page[0][2]
        pre_page[0] = [].concat pre_page[0]
        pre_page[0][0] = null
      storage.write pre_page, group()
    return
  , (err, pages_pos) ->
    if err
      return callback err

    pages = pages_pos.map (pos, i) ->
      if i == 0
        [null, pos]
      else
        [pages[i][0][0], pos]

    @parallel() null, pages
  ,  callback
