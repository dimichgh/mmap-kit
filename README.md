# mmap-kit
===========
The module provides a higher level API to mmap functionality in a form of mmap page and infinite array and queue (limited by disk space) based on mmap page.
Most functionality was borrowed from [java bigqueue](https://github.com/bulldog2011/bigqueue)

### Install
```bash
npm install mmap-kit
```

### Limitations
* There is no inter-process synchronization, hence one should use it in a form of one way socket communication, which is a single producer always appends, a single consumer should only remove (shift or dequeue) and multiple readers can explore the array or queue in a read-only mode.
* Uses mmap.js which has no support for Windows

### Usage

#### Page API

This is a still low-level API for manipulating memory-mapped pages

##### Writing to a page
```javascript
var PageFactory = require('../lib/page-factory').PageFactory;
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
var PageFactory = require('../lib/page-factory').PageFactory;
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

#### ByteBuffer

Extends Buffer to provide auto positioning when reading/writing data from/to the buffer.

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

#### Big Array

Provides a high-level API for managing memory-mapped pages in a form of an array.

Architecture:
![Big Array](https://raw.githubusercontent.com/dimichgh/mmap-kit/master/docs/images/big-array-structure.jpg)

##### Create array
```javascript
var BigArray = require('../lib/big-array').BigArray;
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
* append(Buffer) - append buffer data to the end of the array.
* close() - unload the array from memory.
* deletePagesOutsideIndexRange(headIndex: Number|Bignum, tailIndex: Number|Bignum, fn(err: Error)) - delete pages outside given range.
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

Provides a high-level API for managing memory-mapped pages in a form of a queue.

##### Create queue
```javascript
var bigQueue = new BigQueue('./.tmp', 'test');
var BigQueue = require('../lib/big-queue').BigQueue;
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
console.log(bigQueue.deenqueue()); // out: hello
console.log(bigQueue.size()); // out: 1
console.log(bigQueue.deenqueue()); // out: world
```


### To do:
* use/check watchFile to detect changes
* PR to mmap.js to support windows using https://github.com/ozra/mmap-io
# mmap-kit
