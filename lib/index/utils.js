/**
* Various utilities for Node index library
*/

var utils = exports;

/**
* Merge two objects
*/

var merge = utils.merge = function merge(a, b) {
  if (!a) return b || {};
  if (!b) return a || {};

  var c = {};
  for (var i in a) {
    if (!a.hasOwnProperty(i)) continue;
    c[i] = a[i];
  }

  for (var i in b) {
    if (!b.hasOwnProperty(i)) continue;
    c[i] = typeof c[i] === 'object' ? merge(c[i], b[i]) : b[i];
  }

  return c;
};

/**
* Perform a binary search in following array
* [[key, value], [key, value], ...]
*
* @return value or undefined.
*
*/
utils.search = function search(index, sort, key) {
  for (var i = 0, len = index.length; i < len; i++) {
    if (sort(index[i][0], key) <= 0 &&
        (!index[i+1] || sort(index[i+1][0], key) > 0)) {

      return i;
    }
  };

  return null;
}

/**
* Wrapper for asynchronous callback
*/
utils.efn = function(callback) {
  return function efn_wrapper(fn) {
    // Callback can be empty
    fn = fn || function() {};

    return function efn_callback(err) {
      if (err) return callback(err);
      return fn.apply(this, arguments);
    };
  };
};

