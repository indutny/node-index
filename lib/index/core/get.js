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
      sort = this.sort,
      storage = this.storage,
      efn = utils.efn(callback);

  var iterate = efn(function iterate(err, index) {

    var item_index = utils.search(index, sort, key),
        item = index[item_index];

    // Item not found
    if (!item) {
      return callback('Not found');
    }
    
    var value = item[1];

    if (!item[2]) {
      // Key Pointer - go further
      that.storage.read(value, iterate);
    } else {
      // Key Value - return value
      if (sort(item[0], key) !== 0) return callback('Not found');

      // Read actual value
      storage.read(value, efn(function(err, value) {
        callback(null, value);
      }));
    }
  });

  storage.readRoot(iterate);
};

/**
* Travers btree
* filter can return:
*   true - if `traverse` should go deeper
*   undefined - if `traverse` should skip element
*   false - if `traverse` should stop traversing and return to higher level
*
* @return promise.
*/
exports.traverse = function traverse(filter) {
  var that = this,
      promise = new process.EventEmitter();
  
  // If no filter were provided - match all
  filter = filter || function() {return true;};

  process.nextTick(function() {
    var efn = utils.efn(function(err) {
          promise.emit('error', err);
          promise.emit('end');
        }),
        sort = that.sort,
        storage = that.storage;

    var iterate = function (callback) {
      return efn(function(err, page) {
        var index = -1,
            pagelen = page.length;

        function asyncFilter() {
          if (++index >= pagelen) {
            if (callback) {
              callback(null);
            } else {
              promise.emit('end');
            }
            return; 
          }

          var current = page[index];
          
          filter.call(that, current, function(err, filter_value) {
            if (filter_value === true) {
              if (current[2]) {
                // emit value
                storage.read(current[1], function(err, value) {                
                  promise.emit('data', value, current);
                  asyncFilter();
                });
              } else {
                // go deeper
                storage.read(current[1], iterate(asyncFilter));
              }
            } else {
              if (filter_value === false) {
                index = pagelen;
              }
              asyncFilter();
            }
          });
        };

        asyncFilter();
      });
    };

    storage.readRoot(iterate());
  });

  return promise;
};

