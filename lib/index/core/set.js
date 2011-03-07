/**
* Set functionality for Node index module
*/

var utils = require('../../index/utils');

/**
* Set
*/
exports.set = function set(key, value, callback) {
  var that = this,
      efn = utils.efn(callback);

  function iterate(page, callback) {
    var item_index = utils.search(page, that.sort, key),
        item = page[item_index];

    if (that.storage.isPosition(item[1])) {
      // Index
      
    } else {
      // Leaf
    }
  };

  step(function() {
    // Read initial data
    that.storage.readRoot(this.parallel());
  }, efn(function(err, root) {
    // Initiate sequence
    iterate(root, this.parallel());
  }), efn(function(err, new_root_pos) {
    // Write new root
    that.storage.writeRoot(new_root_pos, this.parallel());
  }), efn(callback));
};


