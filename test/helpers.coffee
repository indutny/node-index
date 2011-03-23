step = require 'step'
fs = require 'fs'
vows = require 'vows'
assert = require 'assert'

index = require '../lib/index'

few = [0, 1, 3, 4, 2, 6, -1]

N = 100
half_of_N = N >> 1
half_of_N_1 = half_of_N + 1

bulk = [1..N]

many = [1..N]
left_half_many = [1..half_of_N]
right_half_many = [half_of_N_1..N]
unexist = [1..10]

setArray = (I, prefix, data, callback) ->
  step ->
    group = @group()
    I.set (prefix + i), (prefix + i), group() for i in data
    return
  , callback

  return

getArray = (I, prefix, data, callback) ->
  step ->
    group = @group()
    fn = (key) ->
      _callback = group()
      (err, value) ->
        if err
          _callback err
        else
          _callback null, value is key

    I.get (prefix + i), fn(prefix + i) for i in data
    return
  , callback
  return

notgetArray = (I, prefix, data, callback) ->
  step ->
    group = @group()
    fn = (key) ->
      _callback = group()
      (err, value) ->
        if err
          return _callback null
        _callback 'found!'

    I.get (prefix + i), fn() for i in data
    return
  , callback
  return

unsetArray = (I, prefix, data, callback) ->
  step ->
    group = @group()
    I.unset (prefix + i), group() for i in data
    return
  , callback

  return

exports.consistencyTest = (suite, options) ->
  I = null
  suite
  .addBatch
    'Unsetting not-existing values from empty tree':
      topic: ->
        I = options.I
        unsetArray I, 'unexist:', unexist, @callback
      'should be still successfull': ->
        return
  .addBatch
    'Running bulk insertion op':
      topic: ->
        I.bulk (bulk.map (i) ->
          ['bulk:' + i, 'bulk:' + i, 1]
        ), @callback
      'should not have conflicts': (conflicts) ->
        assert.equal conflicts.length, 0
  .addBatch
    'Getting items from that bulk set':
      topic: ->
        getArray I, 'bulk:', bulk, @callback
      'should return right values': (oks) ->
        assert.ok oks.every (ok) -> ok is true
  .addBatch
    'Running bulk insertion op again':
      topic: ->
        I.bulk (bulk.map (i) ->
          ['bulk:' + i, 'bulk:' + i, 1]
        ), @callback
      'should have conflicts': (conflicts) ->
        assert.equal conflicts.length, bulk.length
  .addBatch
    'Running bulk removal op':
      topic: ->
        I.bulk (bulk.map (i) ->
          ['bulk:' + i]
        ), @callback
      'should be successfull': ->
        return
  .addBatch
    'Getting items from that bulk set':
      topic: ->
        notgetArray I, 'bulk:', bulk, @callback
      'should not return right values': (oks) ->
        assert.ok oks.every (ok) -> ok isnt true
  .addBatch
    'Running bulk insertion for half':
      topic: ->
        I.bulk (bulk
                .filter (i) ->
                  i % 2
                .map (i) ->
                  ['bulk:' + i, 'bulk:' + i, 1]
                ), @callback
      'should have no conflicts': (conflicts) ->
        assert.equal conflicts.length, 0
  .addBatch
    'Running bulk mixed action (insertion/removal)':
      topic: ->
        I.bulk (bulk.map (i) ->
          if i % 2
            ['bulk:' + i]
          else
            ['bulk:' + i, 'bulk:' + i, 1]
        ), @callback
      'should have no conflicts': (conflicts) ->
        assert.equal conflicts.length, 0
  .addBatch
    'Getting items from half of bulk set':
      topic: ->
        getArray I, 'bulk:', bulk.filter (i) ->
          i % 2 == 1
        , @callback
      'should return right values': (oks) ->
        assert.ok oks.every (ok) -> ok is true
  .addBatch
    'Getting items from another half of bulk set':
      topic: ->
        notgetArray I, 'bulk:', bulk.filter (i) ->
          i % 2
        , @callback
      'should not return right values': (oks) ->
        assert.ok oks.every (ok) -> ok isnt true
  .addBatch
    'Running bulk removal for another half':
      topic: ->
        I.bulk (bulk
                .filter (i) ->
                  i % 2 == 1
                .map (i) ->
                  ['bulk:' + i]
                ), @callback
      'should have no conflicts': (conflicts) ->
        assert.equal conflicts.length, 0
  .addBatch
    'Setting':
      'few key-values':
        topic: ->
          setArray I, 'few:', few, @callback
        'should be successfull': ->
          return
      'many values':
        topic: ->
          setArray I, 'many:', many, @callback
        'should be successfull': ->
          return
  .addBatch
    'Getting':
      'few key-values':
        topic: ->
          getArray I, 'few:', few, @callback
        'should return right values': (oks) ->
          assert.ok oks.every (ok) -> ok is true
      'many values':
        topic: ->
          getArray I, 'many:', many, @callback
        'should return right values': (oks) ->
          assert.ok oks.every (ok) -> ok is true
  .addBatch
    'Running compaction':
      topic: ->
        I.compact @callback
      'should be successfull': ->
        return
  .addBatch
    'Getting few key-values':
      topic: ->
        getArray I, 'few:', few, @callback
      'should return right values': (oks) ->
        assert.ok oks.every (ok) -> ok is true
    'Getting many key-values':
      topic: ->
        getArray I, 'many:', many, @callback
      'should return right values': (oks) ->
        assert.ok oks.every (ok) -> ok is true
  .addBatch
    'Unsetting half of values':
      topic: ->
        unsetArray I, 'many:', left_half_many, @callback
      'should be successfull': ->
        return
  .addBatch
    'Getting values from that half':
      topic: ->
        notgetArray I, 'many:', left_half_many, @callback
      'should be not successfull': (oks) ->
        assert.ok oks.every (ok) -> ok isnt true
    'Getting values from another half':
      topic: ->
        getArray I, 'many:', right_half_many, @callback
      'should be successfull': (oks) ->
        assert.ok oks.every (ok) -> ok is true
  .addBatch
    'Unsetting another half of values':
      topic: ->
        unsetArray I, 'many:', right_half_many, @callback
      'should be successfull': ->
        return
  .addBatch
    'Getting any value from unsetted':
      topic: ->
        notgetArray I, 'many:', many, @callback
      'should be not successfull': (oks) ->
        assert.ok oks.every (ok) -> ok isnt true
  .addBatch
    'Inserting those values again':
      topic: ->
        setArray I, 'many:', many, @callback
      'should be successfull': ->
        return
  .addBatch
    'Getting any of them':
      topic: ->
        getArray I, 'many:', many, @callback
      'should return right values': (oks) ->
        assert.ok oks.every (ok) -> ok is true
  .addBatch
    'Running compaction again':
      topic: ->
        I.compact @callback
      'should be successfull': ->
        return
  .addBatch
    'And all values':
      topic: ->
        getArray I, 'many:', many, @callback
      'should be in place': (oks) ->
        assert.ok oks.every (ok) -> ok is true
  .addBatch
    'Unsetting non-existant values':
      topic: ->
        unsetArray I, 'unexist', unexist, @callback
      'should be still successfull': ->
        return

exports.memoryTest = (suite, index_options, options) ->
  suite = suite.addBatch
    'Creating new index':
      topic: ->
        index.createIndex(index_options)
      'should create instance of Index': (I) ->
       options.I = I
       assert.instanceOf I, index.Index

  exports.consistencyTest suite, options

exports.fileTest = (suite, index_options, fs_options, options) ->
  if fs_options.reopen
    suite = suite
    .addBatch
      'Closing fds for old storage':
        topic: ->
          options.I.storage.close @callback
        'should be successfull': ->
          return

  suite = suite
  .addBatch
    'Creating new file-storage':
      topic: ->
        if fs_options.reopen
          delete fs_options.reopen
        else
          try
            fs.unlinkSync fs_options.filename
            fs.unlinkSync (fs_options.filename + '.' + i) for i in [1..300]
          catch err
            true
        index.storage.file.createStorage fs_options, @callback
        return
      'should be successfull': (storage) ->
        index_options.storage = storage
  .addBatch
    'Creating new index':
      topic: ->
        index.createIndex(index_options)
      'should create instance of Index': (I) ->
        options.I = I
        assert.instanceOf I, index.Index

  if fs_options.reopen
    suite = suite
    .addBatch
      'Unsetting all old values':
        'many':
          topic: ->
            unsetArray options.I, 'many:', many, @callback
          'should be successfull': ->
            return
        'few':
          topic: ->
            unsetArray options.I, 'few:', few, @callback
          'should be successfull': ->
            return

  exports.consistencyTest suite, options

