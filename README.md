# mmap-kit
[![Build Status](https://travis-ci.org/dimichgh/mmap-kit.svg?branch=master)](https://travis-ci.org/dimichgh/mmap-kit) [![NPM](https://img.shields.io/npm/v/mmap-kit.svg)](https://www.npmjs.com/package/mmap-kit)
[![Downloads](https://img.shields.io/npm/dm/mmap-kit.svg)](http://npm-stat.com/charts.html?package=mmap-kit)

The module provides a higher level API of memory mapped functionality in a form of page, big array and big queue (limited by disk space) based on mmap page.
Most functionality and tests were borrowed from [java bigqueue](https://github.com/bulldog2011/bigqueue)

### Install
```bash
npm install mmap-kit
```

### Limitations
* There is no inter-process synchronization, hence one should use it in a form of one way socket communication, which is a single producer always appends, a single consumer should only remove (shift or dequeue) and multiple readers can explore the array or queue in a read-only mode.
* Uses mmap.js which has no support for Windows (PR pending and blocked by https://github.com/nodejs/node/issues/6369)

### Usage

#### Page API

This is a still low-level API for manipulating memory-mapped pages.

##### Writing to a page
```javascript
var PageFactory = require('mmap-kit').PageFactory;
mappedPageFactory = new PageFactory(1024 * 1024, './test');
var mappedPage = mappedPageFactory.acquirePage(0);
var buffer = mappedPage.getLocal(0);
buffer.write('hello world');
mappedPage.setDirty(true);
mappedPage.flush();
mappedPage.close(); // gc
```

##### Reading a page
```javascript
var PageFactory = require('mmap-kit').PageFactory;
mappedPageFactory = new PageFactory(1024 * 1024, './test');
var mappedPage = mappedPageFactory.acquirePage(0);
var buffer = mappedPage.getLocal(0);
console.log(buffer.slice(0, 11).toString()); // out: hello world
mappedPage.close(); // gc
```

##### Release page
```javascript
mappedPageFactory.releasePage(0); // release page 0
```

##### Delete page
```javascript
// delete page 0
mappedPageFactory.deletePage(0, function (err) {
    console.log(err || 'deleted');
});
```

##### Delete pages
```javascript
mappedPageFactory.deletePages([2, 3], function (err) {
    console.log(err || 'deleted');
});
```

##### Delete all pages
```javascript
mappedPageFactory.deleteAllPages(function (err) {
    console.log(err || 'deleted');
});
```

##### Other page API
* getBackPageFiles(callback: fn(err: Error, fileSet: Array)) - get all pages files on the disk.
* getBackPageFileSize(callback: fn(err: Error, size: Number)) - get total size of the pages on the disk.
* getExistingBackFileIndexes(callback: fn(err: Error, indexSet: Array)) - get a list of existing indexes used for the pages.
* getCacheSize(): Number - get number of entries in the cache.
* deletePagesByIndexRange(ranges: [?], callback: fn(err: Error)) - delete pages in the given range (start and end inclusive.)
* flush() - flush all pages in cache.
* close(pageIndex) - close a page at pageIndex and removes it from cache.
* releaseCachedPages() - release all cached pages.
* releaseCachedPage(pageIndex: Number) - release a page at pageIndex.

#### Big Array

Provides a high-level API for managing memory-mapped pages in a form of an array.

Architecture:
![Big Array](https://raw.githubusercontent.com/dimichgh/mmap-kit/master/docs/images/big-array-structure.png)

##### Create array
```javascript
var BigArray = require('mmap-kit').BigArray;
var bigArray = new BigArray('./tmp', 'test');
```

##### Add data
```javascript
bigArray.append(new Buffer('hello'));
bigArray.append(new Buffer('world'));
bigArray.flush(); // I found that at least on OSX it auto-flushes without flush for in memory pages at least
```

##### Read data
```javascript
var bigArray = new BigArray('./tmp', 'test');
console.log(bigArray.size()); // out: 2
console.log(bigArray.shift()); // out: hello
console.log(bigArray.shift()); // out: world
console.log(bigArray.size()); // out: 0
```

##### Disable auto-sync
By default array would sync with memory mapped file for metadata changes. It can be disabled in case one knows that no data can be deleted over the time.
```javascript
bigArray.setAutoSync(false);
```

##### Array API
* BigArray(options) - constructor
    * options:
        * dir: String - directory for array data store
        * name: String - the name of the array, will be appended as last part of the array directory
        * dataPageSize: Number - the back data file size per page in bytes, see minimum allowed, default 32Mb
        * maxDataSize: Number|Bignum - maxDataSize in Mb, the max back data file size, see minimum allowed, default 32Mb
        * backlog: fn(ByteBuffer) - a function to be called when the array space is maxed out and the oldest entries will be auto-backlogged to free up the space.
        * backlogBatchSize: Number - a number of entries to auto backlog when max size of array is reached
* append(Buffer) - append buffer data to the end of the array.
* close() - unload the array from memory.
* deletePagesOutsideIndexRange(headIndex: Number|Bignum, tailIndex: Number|Bignum, fn(err: Error)) - delete pages outside given range.
* each(fn(element: ByteBuffer, index, next)) - iterate through all array elements asynchronously
* eachSync(fn(element: ByteBuffer, index)) - iterate through all array elements in sync mode
* flush() - flush array to disk.
* gc() - force gc to remove unused files on the disk.
* get(arrayIndex): ByteBuffer - get element at the given index.
* getPageIndex(arrayIndex): Bignum - get page index for the given array index.
* getDataPageIndex(arrayIndex): Bignum - get data page index for the given array index.
* getIndexItemBuffer(arrayIndex): ByteBuffer - get page buffer for the given array index.
* getIndexPageOffset(arrayIndex): Bignum - get page offset for the given array index.
* getBackFileSize(callback: fn(err: Error, size: Bignum)) - get total size of all files allocated for the array on the disk.
* dataSize(): Bignum - get total size of data stored in the array.
* size(): Bignum - get the total number of elements in the array
* shift() - take element from the head of the array and schedule GC.
* scheduleGC() - schedule GC to remove unused page files on the disk.
* sync() - sync up meta (indexes) and data pages to mapped memory.
* syncMeta() - sync only meta pages to mapped memory.
* syncData() - sync only data pages to mapped memory.
* syncTailDataIndexFromMemory() - sync tail data from mapped memory.
* syncHeadIndexToMemory() - sync head indexes (head meta) to mapped memory.
* syncTailIndexToMemory() - sync tail indexes (tail meta) to mapped memory.
* removeAll() - remove all data/indexes.
* isEmpty() - check if array is empty.
* isFull() - check if array is full.
* isValidIndex(arrayIndex) - checks if given array index is valid.

#### Big Queue

Provides a high-level API for managing memory-mapped pages in a form of a queue. It is based on BigArray.

##### Create queue
```javascript
var BigQueue = require('mmap-kit').BigQueue;
var bigQueue = new BigQueue('./.tmp', 'test');
```

##### Add to queue
```javascript
bigQueue.enqueue(new Buffer('hello'));
console.log(bigQueue.size()); // out: 1
bigQueue.enqueue(new Buffer('hello'));
console.log(bigQueue.size()); // out: 2
bigQueue.flush();
```

##### Peek
```javascript
console.log(bigQueue.size()); // out: 2
console.log(bigQueue.peek()); // out: hello
console.log(bigQueue.size()); // out: 2
```

##### Iterate through queue in sync mode
```javascript
bigQueue.eachSync(function iter(el, i) {
    console.log(el.toString(), ' at ' + i.toNumber());
});
console.log(bigQueue.size()); // out: 2
```

##### Iterate through queue in async mode
```javascript
bigQueue.eachSync(function iter(el, i, next) {
    console.log(el.toString(), ' at ' + i.toNumber());
    next();
}, function onComplete() {
    console.log(bigQueue.size()); // out: 2
});
```

##### Get from queue
```javascript
console.log(bigQueue.size()); // out: 2
console.log(bigQueue.dequeue()); // out: hello
console.log(bigQueue.size()); // out: 1
console.log(bigQueue.dequeue()); // out: world
```

##### API
* BigQueue(options) - constructor
    * options:
        * dir: String - directory for array data store
        * name: String - the name of the array, will be appended as last part of the array directory
        * dataPageSize: Number - the back data file size per page in bytes, see minimum allowed, default 32Mb
        * maxDataSize: Number|Bignum - maxDataSize in Mb, the max back data file size, see minimum allowed, default 32Mb
        * backlog: fn(ByteBuffer) - a function to be called when the array space is maxed out and the oldest entries will be auto-backlogged to free up the space.
        * backlogBatchSize: Number - a number of entries to auto backlog when max size of array is reached
* enqueue(Buffer) - put buffer to the end of queue
* dequeue(): ByteBuffer - get element from head of the queue and removes it from the queue
* peek(): ByteBuffer - get element from head of the queue
* each(fn(element: ByteBuffer, index, next)) - iterate through all queue elements asynchronously
* eachSync(fn(element: ByteBuffer, index)) - iterate through all queue elements in sync mode
* close() - close and gc queue from the memory
* flush() - flush the queue
* size(): Bignum - get size of the queue
* removeAll

#### ByteBuffer

Extends Buffer to provide auto positioning when reading/writing data from/to the buffer.

#### Usage

##### Create byte buffer
```javascript
var buffer = require('mmap-kit').ByteBuffer.create(16);
```

##### Extend existing buffer
```javascript
require('mmap-kit').ByteBuffer.extend(new Buffer(16));
```

##### Writing to buffer
```javascript
buffer.putBigInt(Bignum(0x01020304));
buffer.putBigLong(Bignum('1122334455667788', 16));
// using existing API
buffer.putUInt16LE(20);
console.log('current position:', buffer.position);
```

##### Reading from buffer
```javascript
// set position the the start if reading above buffer
buffer.position = 0;
console.log('big int: %s', buffer.getBigInt());
console.log('big long: %s', buffer.getBigLong());
console.log('unsigned int: %s', buffer.getUInt16LE());
```

##### API

* extend(Buffer) - extends existing buffer with auto-positioning API

###### Extended API
* getUInt8 - read and move offset 1 byte forward
* getUInt16LE - read and move offset 2 bytes forward
* getUInt16BE - read and move offset 2 bytes forward
* getUInt32LE - read and move offset 4 bytes forward
* getUInt32BE - read and move offset 4 bytes forward
* getInt8 - read and move offset 1 byte forward
* getInt16LE - read and move offset 2 bytes forward
* getInt16BE - read and move offset 2 bytes forward
* getInt32LE - read and move offset 4 bytes forward
* getInt32BE - read and move offset 4 bytes forward
* getFloatLE - read and move offset 4 bytes forward
* getFloatBE - read and move offset 4 bytes forward
* getDoubleLE - read and move offset 8 bytes forward
* getDoubleBE - read and move offset 8 bytes forward
* __getBigLong__ - read 64 bit Bignum number and move offset 8 bytes forward
* __getBigInt__ - read 32 bit Bignum number and move offset 4 bytes forward
* putUInt8 - write and move offeset 1 byte forward,
* putUInt16LE - write and move offeset 2 bytes forward,
* putUInt16BE - write and move offeset 2 bytes forward,
* putUInt32LE - write and move offeset 4 bytes forward,
* putUInt32BE - write and move offeset 4 bytes forward,
* putInt8 - write and move offeset 1 byte forward,
* putInt16LE - write and move offeset 2 bytes forward,
* putInt16BE - write and move offeset 2 bytes forward,
* putInt32LE - write and move offeset 4 bytes forward,
* putInt32BE - write and move offeset 4 bytes forward,
* putFloatLE - write and move offeset 4 bytes forward,
* putFloatBE - write and move offeset 4 bytes forward,
* putDoubleLE - write and move offeset 8 bytes forward,
* putDoubleBE - write and move offeset 8 bytes forward
* __putBigLong__ - write 64 bit Bignum number and move offset 8 bytes forward
* __putBigInt__ - write 32 bit Bignum number and move offset 4 bytes forward

### To do:
* use/check watchFile to detect changes
* PR to mmap.js to support windows, currently blocked by https://github.com/indutny/mmap.js/issues/3
# mmap-kit
