###
  LRU cache
###

utils = require('../index/utils')

DEFAULT_OPTIONS =
  maxSize: 8000
  normalSize: 4000

exports.LRU = LRU = (options) ->
  {@maxSize, @normalSize} = utils.merge DEFAULT_OPTIONS, options
  @hashmap = {}
  @keys = []
  @keysLen = 0

LRU::set = (key, value) ->
  hashmap = @hashmap
  existing = hashmap[key]
  if existing
    existing.fitness++
  else
    keys = @keys
    keys.push hashmap[key] =
      fitness: @keysLen / 10
      key: key
      value: value

    if ++@keysLen > @maxSize
      keys.sort (a, b) ->
        if a.fitness < b.fitness
          1
        else if a.fitness == b.fitness
          0
        else
          -1

      rest = keys.slice @normalSize
      @keys = keys.slice 0, @normalSize

      rest.forEach (item) ->
        delete hashmap[item.key]

      @keysLen = @normalSize
  value

LRU::get = (key) ->
  existing = @hashmap[key]
  if existing
    existing.fitness++
    existing.value
  else
    undefined
