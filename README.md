Node index
==========

This module is a implementation of a append-only B+ Tree fully written in 
[coffee-script](https://github.com/jashkenas/coffee-script).

Benchmark
---------

![read benchmark](https://github.com/indutny/node-index/raw/master/bench-data/read.png)
![write benchmark](https://github.com/indutny/node-index/raw/master/bench-data/write.png)

Basics
------

    // Create basic B+ Tree index that will be stored
    // in memory (all settings are default, see below)
    var index = require('index').createIndex();

    // Store value in storage (callback is optional)
    index.set('key', 'value', function(err) {
      // ... your code here ...
    });

    // Get value from storage
    index.get('key', function(err, value) {
      // ... your code here ...
    });

    // Remove value from storage
    index.unset('key', function(err) {
    });

    // Bulk op
    index.bulk([
      ['key', 'value', 1], // insert kv
      ['key'] // remove kv
    ], function(err, conflicts) {
    });

    // Compaction
    index.compact(function(err) {
    });

Options
-------

    require('index').createIndex({
      order: 32, // Maximum number of items in page
                 // Tree's height depends on that
      storage: require('index').storage.memory // Place where all tree data will be stored
                    .createStorage(),          // (see more description below)
      sort: function(a, b) {
        return (a === null || a < b) ? -1 : a == b ? 0 : -1;
      } // Function that will be used to compare keys
        // Note that null is a system value and sort should always return negative
        // result if first argument is null
    });

License
-------

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

