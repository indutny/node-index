/**
* Unset functionality for Node index module
*/

var step = require('step');

var utils = require('../../index/utils');

exports.unset = function unset(key, _callback) {
  function callback(err, data) {
    that.releaseLock();

    process.nextTick(function() {
      _callback && _callback(err, data);
    });
  };

  var efn = utils.efn(callback),
      that = this,
      storage = this.storage,
      sort = this.sort;

  if (this.lock(function onLockRelease() {
    that.unset(key, _callback);
  })) {
    return;
  }

  function iterate(page, callback) {
    var item_index = utils.search(page, sort, key),
        item = page[item_index];

    if (item_index === null) {
      // Not found
      // Even in that case unset should be successfull
      callback(null);
      return;
    }

    if (item[2]) {
      // Leaf

      if (sort(item[0], key) !== 0) {
        // Actually key doesn't match
        // So do nothing
        callback(null);
        return;
      }

      // Delete from leaf and if one will be empty
      // remove leaf from parent
      page.splice(item_index, 1);
      if (page.length > 0) {
        // If resulting page isn't empty
        step(function() {
          storage.write(page, this.parallel());
        }, efn(callback));
        return;
      }

      // Notify that item should be removed from parent index
      callback(null, false);
    } else {
      // Index page
      step(function() {
        storage.read(item[1], this.parallel());
      }, efn(function(err, page) {
        iterate(page, this.parallel());
      }), efn(function(err, result) {
        if (result === false) { 
          // Delete item from index page
          page.splice(item_index, 1);
          if (page.length <= 1) {
            callback(null, page[0][1]);
            return;
          }
        } else if (storage.isPosition(result)) {
          page[item_index][1] = result;
        } else {
          callback(null);
          return;
        }

        step(function() {
          storage.write(page, this.parallel());
        }, efn(callback));
      }));
    }
  }

  step(function() {
    storage.readRoot(this.parallel());
  }, efn(function(err, root) {
    iterate(root, this.parallel());
  }), efn(function(err, result) {
    if (result === false) {
      // Create new root
      storage.write([], this.parallel());
    } else if (storage.isPosition(result)) {
      // Overwrite old root
      this.parallel()(null, result);
    } else {
      callback(null);
    }
  }), efn(function(err, position) {
    storage.writeRoot(position, this.parallel());
  }), callback);
};

