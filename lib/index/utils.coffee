###
  Various utilities for Node index library
###

utils = exports

###
 Merge two objects
###

merge = utils.merge = (a, b) ->
  if not a or not b
    return a || b || {}

  c = {}
  for k,v of  a
    if not a.hasOwnProperty k
      continue
    c[k] = v
  
  for k, v of b
    if not b.hasOwnProperty k
      continue

    c[k] = typeof c[k] is 'object' ?  c[k] = merge c[k], v :
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
    if not fn
      fn = () -> null

    (err) ->
      if err
        return callback err

      fn.apply this, arguments

