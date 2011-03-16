/**
* Compaction for Node Index
*/

var step = require('step');

var utils = require('../../index/utils');

exports.compact = function(callback) {
  var that = this,
      storage = this.storage,
      efn = utils.efn(callback);

  // This will allow storage controller
  // to prepare it for
  storage.beforeCompact && storage.beforeCompact();

  function iterate(callback) {
    return efn(function(err, page) {
      var in_leaf = !!page[0] && page[0][2],
          fns = page.map(function(item) {
            return function() {
              step(function() {
                storage.read(item[1], this.parallel());
              }, efn(function(err, data) {
                if (in_leaf) {
                  // data is actual value
                  // remove old revision referense
                  data[1] = undefined;
                  storage.write(data, this.parallel());
                  return;
                }

                iterate(this.parallel())(null, data);
              }), efn(function(err, new_pos) {
                item[1] = new_pos;
                this.parallel()(null);
              }), this.parallel());
            };
          });

      fns.push(efn(function(err) {
        storage.write(page, this.parallel());
      }));
      fns.push(callback);

      step.apply(null, fns);
    });
  }

  step(function() {
    storage.readRoot(iterate(this.parallel()));
  }, efn(function(err, new_root_pos) {
    storage.writeRoot(new_root_pos, this.parallel());
  }), efn(function(err) {
    // This will allow storage to finalize all actions
    storage.afterCompact && storage.afterCompact();
    this.parallel()(null);
  }), callback);

};

