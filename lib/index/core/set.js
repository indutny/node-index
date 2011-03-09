/**
* Set functionality for Node index module
*/

var step = require('step');

var utils = require('../../index/utils');

/**
* Set
*/
exports.set = function set(key, value, _callback) {
  var that = this;

  if (this.lock(function onLockRelease() {
    that.set(key, value, _callback);
  })) {
    return;
  }
  
  function callback(err, data) {
    that.releaseLock();

    process.nextTick(function() {
      _callback(err, data);
    });
  };

  var efn = utils.efn(callback);

  function iterate(page, callback) {
    var item_index = utils.search(page, that.sort, key),
        item = page[item_index];

    if (item && !item[2]) {
      // Index

      // Read next page and try to insert kv in it
      step(function() {
        that.storage.read(item[1], this.parallel());
      }, efn(function(err, page) {
        iterate(page, this.parallel());
      }), efn(function(err, result) {
        if (that.storage.isPosition(result)) {
          // Page is just should be overwrited
          page[item_index][1] = result;

          that.storage.write(page, callback);
        } else {
          // Result is = {
          //   left_page: [...],
          //   middle_key: ...,
          //   right_page: [...]
          // }
          page[item_index][1] = result.left_page;
          page.splice(item_index + 1, 0, [result.middle_key, result.right_page]);

          splitPage(false, that.storage, that.order, page, callback); 
        } 
      }));
    } else {
      // Leaf

      // Found dublicate
      if (item && that.sort(item[0], key) === 0) {
        // For now throw error
        callback('Can\'t insert item w/ dublicate key');
      } else {
        item_index = item_index === null ? 0 : item_index + 1;

        // Value should be firstly written in storage
        step(function() {
          that.storage.write(value, this.parallel());
        }, efn(function(err, value) {
          // Than inserted in leaf page
          page.splice(item_index, 0, [key, value, 1]);

          splitPage(true, that.storage, that.order, page, callback);
        }));
      }
    }
  };

  step(function() {
    // Read initial data
    that.storage.readRoot(this.parallel());
  }, efn(function(err, root) {
    // Initiate sequence
    iterate(root, this.parallel());
  }), efn(function(err, result) {
    if (that.storage.isPosition(result)) {
      // Write new root
      this.parallel()(null, result);
    } else {
      // Split root
      that.storage.write([
        [null, result.left_page],
        [result.middle_key, result.right_page]
      ], this.parallel());
    }
  }), efn(function(err, new_root_pos) {
    that.storage.writeRoot(new_root_pos, this.parallel());
  }), efn(callback));
};

/**
* Check page length
* If exceed - split it into two and return left_page, right_page, middle_key
*/
function splitPage(in_leaf, storage, order, page, callback) {
  // If item needs to be splitted
  if (page.length > order) {
    var mid_index = page.length >> 1,
        mid_key = page[mid_index][0];

    // Write splitted pages
    step(function() {
      var left_page = page.slice(0, mid_index);
      storage.write(left_page, this.parallel());

      var right_page = page.slice(mid_index);

      if (!in_leaf) {
        right_page[0][0] = null;
      }

      storage.write(right_page, this.parallel());
    }, function(err, left_page, right_page) {
      callback(err, {
        left_page: left_page,
        middle_key: mid_key,
        right_page: right_page
      });
    });

  } else {
    // Just overwrite it
    storage.write(page, callback);
  }
};

