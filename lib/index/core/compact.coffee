###
  Compaction for Node Index
###

step = require 'step'

utils = require '../../index/utils'

exports.compact = (callback) ->
  that = this
  storage = this.storage
  efn = utils.efn callback

  # This will allow storage controller
  # to prepare it for
  storage.beforeCompact && storage.beforeCompact()

  iterate = (callback) ->
    efn((err, page) ->
      in_leaf = page[0] && page[0][2]
      fns = page.map (item) ->
        () ->
          step (() ->
            storage.read item[1], this.parallel()
          ), efn((err, data) ->
            if in_leaf
              # data is actual value
              # remove old revision referense
              data[1] = undefined
              storage.write data, this.parallel()
              return

            iterate(this.parallel()) null, data
          ), efn((err, new_pos) ->
            item[1] = new_pos
            this.parallel() null
          ), this.parallel()

      fns.push(efn (err) ->
        storage.write page, this.parallel()
      )
      fns.push callback

      step.apply null, fns
    )

  step (() ->
    storage.readRoot iterate this.parallel()
  ), efn((err, new_root_pos) ->
    storage.writeRoot new_root_pos, this.parallel()
  ), efn((err) ->
    # This will allow storage to finalize all actions
    storage.afterCompact && storage.afterCompact()
    this.parallel() null
  ), callback

