step = require 'step'
{spawn, exec} = require 'child_process'

run = (args, callback) ->
  proc = spawn 'coffee', args
  proc.stderr.on 'data', (buffer) -> console.log buffer.toString()
  proc.on 'exit', (status) ->
    unless status is 0
      return process.exit 1
    callback null

build = (dir, files, callback) ->
  run ['-c', '-o', dir].concat(files), callback

step (() ->
  console.log 'Starting building node-index'

  build 'build', ['lib/index.coffee'], @parallel()

  build 'build/index', [
    'lib/index/utils.coffee'
    'lib/index/memory-storage.coffee'
    'lib/index/file-storage.coffee'
  ], @parallel()

  build 'build/index/core', [
   'lib/index/core/get.coffee'
   'lib/index/core/set.coffee'
   'lib/index/core/unset.coffee'
   'lib/index/core/compact.coffee'
   'lib/index/core/bulk.coffee'
  ], @parallel()

  return
), (() ->
  console.log 'Done.'
)
