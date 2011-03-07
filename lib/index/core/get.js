/**
* Get functionality for Node Index module
*/

var step = require('step');

/**
* Get value by key
*/
exports.get = function get(key, callback) {
  var that = this;

  function iterate(err, pos) {
    if (err) return callback(err);

    step(function() {
      that.storage.read(pos, this.parallel());
    }, function(err, index) {
      if (err) return callback(err);

      var value = search(root, key);
    
      if (that.storage.isPosition(value)) {
        // Key Pointer - go further
        that.storage.read(value, iterate);
      } else {
        // Key Value - return value
        callback(null, value);
      }
    });
  };

  this.storage.readRoot(iterate);
};

/**
* Get values by key range
*/
exports.getRange = function getRange(start_key, end_key, callback) {
};

/**
* Perform a binary search in following array
* [[key, value], [key, value], ...]
*
* @return value or undefined.
*
*/
function search(index, key) {
  for (var i = 0, len = index.length; i < len; i++) {
 
  };
};
