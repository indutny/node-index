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
};

