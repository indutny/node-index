###
  Compaction for Node Index

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

exports.compact = (callback) ->
  storage = @storage
  efn = utils.efn callback

  # @will allow storage controller
  # to prepare it for
  storage.beforeCompact && storage.beforeCompact()

  iterate = (callback) ->
    efn((err, page) ->
      in_leaf = page[0] && page[0][2]
      fns = page.map (item) ->
        ->
          step (->
            storage.read item[1], @parallel()
          ), efn((err, data) ->
            if in_leaf
              # data is actual value
              # remove old revision referense
              data[1] = undefined
              storage.write data, @parallel()
              return

            iterate(@parallel()) null, data
          ), efn((err, new_pos) ->
            item[1] = new_pos
            @parallel() null
          ), @parallel()

      fns.push(efn (err) ->
        storage.write page, @parallel()
      )
      fns.push callback

      step.apply null, fns
    )

  step (->
    storage.readRoot iterate @parallel()
  ), efn((err, new_root_pos) ->
    storage.writeRoot new_root_pos, @parallel()
  ), efn((err) ->
    # @will allow storage to finalize all actions
    storage.afterCompact && storage.afterCompact()
    @parallel() null
  ), callback

