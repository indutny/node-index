###
  Various utilities for Node index library
###

utils = exports

###
 Merge two objects
###

merge = utils.merge = (a, b) ->
  unless a and b
    return a or b or {}

  c = {}
  for k,v of  a
    unless a.hasOwnProperty k
      continue
    c[k] = v
  
  for k, v of b
    unless b.hasOwnProperty k
      continue

    c[k] = if typeof c[k] is 'object'
             merge c[k], v
           else
             v
  c

###
  Perform a binary search in following array
  [[key, value], [key, value], ...]
 
  @return value or undefined.
 
###
utils.search = (index, sort, key) ->
  i = index.length - 1

  while i >= 0 and sort(index[i][0], key) > 0
    i--

  if i >= 0
    i
  else
    null

###
  Wrapper for asynchronous callback
###
utils.efn = (callback) ->
  (fn) ->
    # Callback can be empty
    unless fn
      fn = -> null

    (err) ->
      if err
        return callback err

      fn.apply @, arguments

