###
  Compaction for Node Index

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

exports.compact = (_callback) ->
  storage = @storage

  callback = (err, data) =>
    @releaseLock()

    process.nextTick ->
      _callback and _callback err, data

  if @lock(=> @compact _callback)
    return

  iterate = (callback) ->
    (err, page) ->
      if err
        return callback err

      in_leaf = page[0] && page[0][2]
      fns = page.map (item) ->
        ->
          step ->
            storage.read item[1], @parallel(), in_leaf
            return
          , (err, data) ->
            if err
              throw err

            if in_leaf
              # data is actual value
              storage.write data, @parallel(), in_leaf
              return

            iterate(@parallel()) null, data
            return
          , (err, new_pos) ->
            if err
              throw err

            item[1] = new_pos
            @parallel() null
            return
          , @parallel()

      fns.push (err) ->
        if err
          throw err

        storage.write page, @parallel()
        return

      fns.push callback

      step.apply null, fns


  step ->
    # will allow storage controller
    # to prepare it for
    if storage.beforeCompact
      storage.beforeCompact @parallel()
    else
      @parallel() null
    return
  , (err) ->
    if err
      throw err

    storage.readRoot iterate @parallel()
    return
  , (err, new_root_pos) ->
    if err
      throw err

    storage.writeRoot new_root_pos, @parallel()
    return
  , (err) ->
    if err
      throw err

    # @will allow storage to finalize all actions
    if storage.afterCompact
      storage.afterCompact @parallel()
    else
      @parallel() null
    return
  , callback

