/**
* Get functionality for Node Index module
*/

var utils = require('../../index/utils'),
    step = require('step');

/**
* Get value by key
*/
exports.get = function get(key, callback) {
  var that = this,
      efn = utils.efn(callback);

  var iterate = efn(function iterate(err, pos) {

    step(function() {
      that.storage.read(pos, this.parallel());
    }, efn(function(err, index) {

      var item_index = utils.search(index, that.sort, key),
          item = index[item_index];
    
      // Item not found
      if (!item_index || that.sort(item[0], key) !== 0) {
        return callback(null, false);
      }

      var value = item[1];

      if (that.storage.isPosition(value)) {
        // Key Pointer - go further
        that.storage.read(value, iterate);
      } else {
        // Key Value - return value
        callback(null, value);
      }
    }));
  });

  this.storage.readRoot(iterate);
};

/**
* Get values by key range
*
* @return promise.
*/
exports.getRange = function getRange(start_key, end_key) {
};

