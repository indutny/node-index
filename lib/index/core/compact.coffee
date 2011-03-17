###
  Compaction for Node Index
###

step = require 'step'

utils = require '../../index/utils'

exports.compact = (callback) ->
  that = @
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

