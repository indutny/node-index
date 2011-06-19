###
  Various utilities for Node index library

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

crypto = require 'crypto'

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
  len = index.length - 1
  i = len

  while i >= 0 and sort(index[i][0], key) > 0
    i--

  if i == len and len >= 0 and sort(index[i][0], key) == 0
    null
  if i < 0
    null
  else
    i

###
  Hash function wrapper
###
utils.hash = (data) ->
  hash = crypto.createHash 'md5'
  hash.update data
  hash.digest 'hex'

utils.hash.len = 32
