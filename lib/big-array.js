'use strict';

var Assert = require('assert');
var Path = require('path');
var Fs = require('fs');
var NodeUtils = require('util');

var Async = require('async');
var Bignum = require('bignum');
var debug = require('debug')('mmap-kit/big-array/' + process.pid);

var PageFactory = require('./page-factory').PageFactory;

/**
 * A big array implementation supporting sequential append and random read.
 *
 * Main features:
 * 1. FAST : close to the speed of direct memory access, extremely fast in append only and sequential read modes,
 *           sequential append and read are close to O(1) memory access, random read is close to O(1) memory access if
 *           data is in cache and is close to O(1) disk access if data is not in cache.
 * 2. MEMORY-EFFICIENT : automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently read/append the array without data corruption.
 * 4. PERSISTENT - all array data is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the array data is only limited by the available disk space.
 *
 *
 * converted from https://github.com/bulldog2011/bigqueue/blob/master/src/main/java/com/leansoft/bigqueue/BigArrayImpl.java
 *
 */

// folder name for index page
var INDEX_PAGE_FOLDER = 'index';
// folder name for data page
var DATA_PAGE_FOLDER = "data";
// folder name for meta data page
var META_DATA_PAGE_FOLDER = "meta_data";

// 2 ^ 20 = 1024 * 1024
var INDEX_ITEMS_PER_PAGE_BITS = 20; // 1024 * 1024
// number of items per page
var INDEX_ITEMS_PER_PAGE = 1 << INDEX_ITEMS_PER_PAGE_BITS;
// 2 ^ 5 = 32
var INDEX_ITEM_LENGTH_BITS = 5;
// length in bytes of an index item
var INDEX_ITEM_LENGTH = 1 << INDEX_ITEM_LENGTH_BITS;
// size in bytes of an index page
var INDEX_PAGE_SIZE = INDEX_ITEM_LENGTH * INDEX_ITEMS_PER_PAGE;

// default size in bytes of a data page
var DEFAULT_DATA_PAGE_SIZE = 128 * 1024 * 1024;
// minimum size in bytes of a data page
var MINIMUM_DATA_PAGE_SIZE = 32 * 1024 * 1024;
// seconds, time to live for index page cached in memory
var INDEX_PAGE_CACHE_TTL = 1000;
// seconds, time to live for data page cached in memory
var DATA_PAGE_CACHE_TTL = 1000;
// 2 ^ 4 = 16
var META_DATA_ITEM_LENGTH_BITS = 4;
// size in bytes of a meta data page
var META_DATA_PAGE_SIZE = 1 << META_DATA_ITEM_LENGTH_BITS;

//	private final static int INDEX_ITEM_DATA_PAGE_INDEX_OFFSET = 0;
//	private final static int INDEX_ITEM_DATA_ITEM_OFFSET_OFFSET = 8;
var INDEX_ITEM_DATA_ITEM_LENGTH_OFFSET = 12;
// timestamp offset of an data item within an index item
var INDEX_ITEM_DATA_ITEM_TIMESTAMP_OFFSET = 16;

// only use the first page
var META_DATA_PAGE_INDEX = 0;

var Long = {
    MAX_VALUE: Bignum.pow(2, 64).sub(1)
};

module.exports.Long = Long;

/**
 * A big array implementation supporting sequential write and random read.
 *
 * @param options
 * @param or @options.arrayDir directory for array data store
 * @param or @options.arrayName the name of the array, will be appended as last part of the array directory
 * @param or @options.dataPageSize the back data file size per page in bytes, see minimum allowed {@link #MINIMUM_DATA_PAGE_SIZE}.
 * @param or @options.maxDataSize (Mb) the max back data file size, see minimum allowed {@link #MINIMUM_DATA_PAGE_SIZE}}.
 *           @options.backlog is a function to be called when the array space is maxed out and the oldest entries will be auto-backlogged
               if the backlog options is not provided, it will throw out of space error.
             @options.backlogBatchSize is a number of entries to auto backlog when max size of array is reached
 * @throws IOException exception throws during array initialization
 */
function BigArray(arrayDir, arrayName, dataPageSize, maxDataSize) {
    var options = arrayDir !== null && typeof arrayDir === 'object' ? arrayDir : {
        arrayDir: arrayDir,
        arrayName: arrayName,
        dataPageSize: dataPageSize,
        maxDataSize: maxDataSize
    };

    arrayDir = options.arrayDir;
    arrayName = options.arrayName;
    dataPageSize = options.dataPageSize;
    maxDataSize = options.maxDataSize;

    this.backlog = options.backlog;
    this.backlogBatchSize = options.backlogBatchSize;

    this.MAX_INDEX = options.MAX_INDEX ?
        toBignum(options.MAX_INDEX) :
        Long.MAX_VALUE;

    // added to have more control of sizing in unit tests
    this.MINIMUM_DATA_PAGE_SIZE = options.overrideMinDataPageSize ? toBignum(options.overrideMinDataPageSize) :
        MINIMUM_DATA_PAGE_SIZE;

    dataPageSize = PageFactory.normalizePageSize(dataPageSize || DEFAULT_DATA_PAGE_SIZE);
    Assert.ok(dataPageSize >= this.MINIMUM_DATA_PAGE_SIZE, 'Page size (' + dataPageSize + ') must be greater then ' +
        this.MINIMUM_DATA_PAGE_SIZE);
    debug('normalized page size %d', dataPageSize);
    this.dataPageSize = toBignum(dataPageSize);

    if (maxDataSize) {
        maxDataSize = toBignum(maxDataSize).mul(1024 * 1024);
        Assert.ok(maxDataSize.ge(this.MINIMUM_DATA_PAGE_SIZE),
            'Limit data size (' + maxDataSize.toString() + ') must be greater then ' + this.MINIMUM_DATA_PAGE_SIZE);
        this.maxDataSize = PageFactory.normalizeBignumSize(maxDataSize, this.MINIMUM_DATA_PAGE_SIZE);
    }
    else {
        this.maxDataSize = Long.MAX_VALUE;
    }

    this.maxDataFiles = this.maxDataSize.div(this.dataPageSize);
    debug('normalized max data size %d, max number of data files %s', this.maxDataSize, this.maxDataFiles);

    this.arrayDirectory = Path.join(arrayDir, arrayName);
    this.setAutoSync(true);
    this.init();
}

module.exports.BigArray = BigArray;

BigArray.MINIMUM_DATA_PAGE_SIZE = MINIMUM_DATA_PAGE_SIZE;

var proto = BigArray.prototype;

function toBignum(val) {
    return val !== undefined && val !== null && val.add === undefined ? Bignum(val) : val;
}

function nextIndex(index, max) {
    index = index.add(1);
    if (index.cmp(max) === 0) {
        index = Bignum(0); // wrap
    }
    return index;
}

function prevIndex(index, max) {
    index = index.sub(1);
    if (index.lt(0)) {
        index = toBignum(max).sub(1); // wrap
    }
    return index;
}

BigArray.nextIndex = nextIndex;
BigArray.prevIndex = prevIndex;

proto.setAutoSync = function setAutoSync(enable) {
    this.autoSync = enable;
};

proto.init = function init() {
    this.INDEX_ITEMS_PER_PAGE_BITS = this.INDEX_ITEMS_PER_PAGE_BITS || INDEX_ITEMS_PER_PAGE_BITS;
    // size in bytes of an index page
    this.INDEX_ITEMS_PER_PAGE = 1 << this.INDEX_ITEMS_PER_PAGE_BITS;
    this.INDEX_PAGE_SIZE = INDEX_ITEM_LENGTH * this.INDEX_ITEMS_PER_PAGE;

    // initialize page factories
    this.indexPageFactory = new PageFactory(this.INDEX_PAGE_SIZE,
        Path.resolve(this.arrayDirectory, INDEX_PAGE_FOLDER),
        INDEX_PAGE_CACHE_TTL);

    this.dataPageFactory = new PageFactory(this.dataPageSize,
        Path.resolve(this.arrayDirectory, DATA_PAGE_FOLDER),
        DATA_PAGE_CACHE_TTL);

    // the ttl does not matter here since meta data page is always cached
    this.metaPageFactory = new PageFactory(META_DATA_PAGE_SIZE,
        Path.resolve(this.arrayDirectory, META_DATA_PAGE_FOLDER));

    // head index of the big array, this is the read write barrier.
    // readers can only read items before this index, and writes can write this index or after
    this.arrayHeadIndex = Bignum(0); // bignum 64bit, long
    // tail index of the big array,
    // readers can't read items before this tail
    this.arrayTailIndex = Bignum(0); // bignum 64bit, long

    // head index of the data page, this is the to be appended data page index
    this.headDataPageIndex = Bignum(0); // bignum 64bit, long
    // head offset of the data page, this is the to be appended data offset
    this.headDataItemOffset = Bignum(0); // int 32bit, int

    // initialize array indexes
    this.sync();
};

proto.sync = function sync() {
    this.syncMeta();
    this.syncData();
};

proto.syncMeta = function sync() {
    debug('sync meta');
    this.initArrayIndex();
};

proto.syncData = function sync() {
    debug('sync data');
    this.initDataPageIndex();
};

proto.isEmpty = function isEmpty() {
    return this.arrayHeadIndex.cmp(this.arrayTailIndex) === 0;
};

proto.initArrayIndex = function initArrayIndex() {
	var metaDataPage = this.metaPageFactory.acquirePage(META_DATA_PAGE_INDEX);
    var metaBuf = metaDataPage.getLocal(0);
    this.arrayHeadIndex = metaBuf.getBigLong();
    this.arrayTailIndex = metaBuf.getBigLong();
    debug('head index is %s', this.arrayHeadIndex);
    debug('tail index is %s', this.arrayTailIndex);
};

proto.getIndexPageOffset = function getIndexPageOffset(index) {
    index = toBignum(index);
    return mod(index, this.INDEX_ITEMS_PER_PAGE_BITS).shiftLeft(INDEX_ITEM_LENGTH_BITS);
};

proto.initDataPageIndex = function initDataPageIndex() {
	if (!this.isEmpty()) {
        debug('array is not empty');
		var previousIndexPageIndex = Bignum(-1);
        var previousIndex = prevIndex(this.arrayHeadIndex, this.MAX_INDEX);
        previousIndexPageIndex = this.getPageIndex(previousIndex);
        var previousIndexPageOffset = this.getIndexPageOffset(previousIndex);
        var previousIndexPage = this.indexPageFactory.acquirePage(previousIndexPageIndex);
        var previousIndexItemBuffer = previousIndexPage.getLocal(previousIndexPageOffset);
        var previousDataPageIndex = previousIndexItemBuffer.getBigLong();
        var previousDataItemOffset = previousIndexItemBuffer.getBigInt();
        var perviousDataItemLength = previousIndexItemBuffer.getBigInt();

        this.headDataPageIndex = previousDataPageIndex;
        this.headDataItemOffset = previousDataItemOffset.add(perviousDataItemLength);
	}
    else {
        this.headDataPageIndex = Bignum(0);
        this.headDataItemOffset = Bignum(0);
    }
    debug('head data index is %s', this.headDataPageIndex);
    debug('head data offset is %s', this.headDataItemOffset);

    this.syncTailDataIndexFromMemory();
};

proto.syncTailDataIndexFromMemory = function syncTailDataIndexFromMemory() {
    debug('syncTailDataIndexFromMemory');

    if (!this.isEmpty()) {
        var tailIndex = this.arrayTailIndex;
        var tailIndexPageIndex = this.getPageIndex(tailIndex);
        var tailIndexPage = this.indexPageFactory.acquirePage(tailIndexPageIndex);
        var tailIndexPageOffset = this.getIndexPageOffset(tailIndex);
        var tailIndexItemBuffer = tailIndexPage.getLocal(tailIndexPageOffset);
        this.tailDataPageIndex = tailIndexItemBuffer.getBigLong();
        this.tailDataItemOffset = tailIndexItemBuffer.getBigInt();
    }
    else {
        this.tailDataPageIndex = Bignum(0);
        this.tailDataItemOffset = Bignum(0);
    }
    debug('tail data index is %s', this.tailDataPageIndex);
    debug('tail data offset is %s', this.tailDataItemOffset);
};

proto.removeAll = function removeAll(callback) {
    callback = callback || function noop() {};
    Async.parallel([
        this.indexPageFactory.deleteAllPages.bind(this.indexPageFactory),
        this.dataPageFactory.deleteAllPages.bind(this.dataPageFactory),
        this.metaPageFactory.deleteAllPages.bind(this.metaPageFactory),
    ], function onComplete(err) {
        if (err) {
            return callback(err);
        }
        this.init();
        callback();
    }.bind(this));

};

proto.isValidIndex = function isValidIndex(index) {
	if (this.arrayTailIndex.le(this.arrayHeadIndex)) {
        if (this.arrayTailIndex.gt(index) || this.arrayHeadIndex.le(index)) {
			return false;
		}
	} else {
		if (this.arrayTailIndex.gt(index) && this.arrayHeadIndex.le(index)) {
            return false;
		}
	}
    return true;
};

proto.validateIndex = function validateIndex(index) {
	if (!this.isValidIndex(index)) {
        throw new Error(NodeUtils.format('Invalid index %s, head: %s, tail: %s',
            index, this.arrayHeadIndex, this.arrayTailIndex));
	}
};

proto.getIndexItemBuffer = function getIndexItemBuffer(index) {
    index = toBignum(index);
    var indexPageIndex = this.getPageIndex(index);
    var indexPage = this.indexPageFactory.acquirePage(indexPageIndex);
    var indexItemOffset = this.getIndexPageOffset(index);
    return indexPage.getLocal(indexItemOffset);
};

// @deprecated use deletePagesOutsideIndexRange
proto.removeBeforeIndex = function removeBeforeIndex(index, callback) {
    index = typeof index === 'number' ? Bignum(index) : index;
    this.validateIndex(index);

    var headIndex = prevIndex(this.arrayHeadIndex, this.MAX_INDEX);
    debug('removeBeforeIndex: [tail:%s. head:%s]', index, headIndex);

    this.deletePagesOutsideIndexRange(headIndex, index, function () {
        this.arrayTailIndex = index;
        this.syncTailIndexToMemory();
        this.syncTailDataIndexFromMemory();
        callback();
    }.bind(this));
};

proto.size = function size() {
    this.autoSync && this.sync();
    var value;
    if (this.arrayTailIndex.le(this.arrayHeadIndex)) {
        value = this.arrayHeadIndex.sub(this.arrayTailIndex);
    } else {
        value = this.MAX_INDEX.sub(this.arrayTailIndex).add(this.arrayHeadIndex);
    }
    return value;
};

proto.dataSize = function dataSize() {
    this.autoSync && this.sync();
    var headValue = this.headDataPageIndex.mul(this.dataPageSize).add(this.headDataItemOffset);
    var tailValue = this.tailDataPageIndex.mul(this.dataPageSize).add(this.tailDataItemOffset);
    var val;
    if (tailValue.le(headValue)) {
        val = headValue.sub(tailValue);
    } else {
        val = this.maxDataFiles.mul(this.dataPageSize).sub(tailValue).add(headValue);
    }
    return val;
};

proto.isFull = function isFull() {
    this.autoSync && this.sync();
    var index = nextIndex(this.arrayHeadIndex, this.MAX_INDEX);
    return index.cmp(this.arrayTailIndex) === 0;
};

proto.isDataFull = function isDataFull(bytesToAdd) {
    var dataSize = this.dataSize();
    return dataSize.add(bytesToAdd || 0).add(1).gt(this.maxDataSize);
};

proto.close = function close() {
    if (this.metaPageFactory !== null) {
        this.metaPageFactory.releaseCachedPages();
    }
    if (this.indexPageFactory !== null) {
        this.indexPageFactory.releaseCachedPages();
    }
    if (this.dataPageFactory !== null) {
        this.dataPageFactory.releaseCachedPages();
    }
};

proto.getDataItemLength = function getDataItemLength(index) {
   var indexItemBuffer = this.getIndexItemBuffer(index);
   indexItemBuffer.position += INDEX_ITEM_DATA_ITEM_LENGTH_OFFSET;
   return indexItemBuffer.getBigInt();
};

// inner getBackFileSize
proto._getBackFileSize = function _getBackFileSize(callback) {
    Async.parallel({
        indexBackPageFileSize: this.indexPageFactory.getBackPageFileSize.bind(this.indexPageFactory),
        dataBackPageFileSize: this.dataPageFactory.getBackPageFileSize.bind(this.dataPageFactory)
    }, function onComplete(err, sizes) {
        callback(err, !err ? sizes.indexBackPageFileSize.add(sizes.dataBackPageFileSize) : undefined);
    });
};

proto.getBackFileSize = function getBackFileSize(callback) {
    this._getBackFileSize(callback);
};

proto.getItemLength = function getItemLength(index) {
    this.validateIndex(index);

    return this.getDataItemLength(index);
};

/**
 * Append the bytearray data into the head of the array
 * throws out of space error (err.code = 'ENOSPC')
 */
proto.append = function append(data) {
    autoBacklog(this, this.backlog);

    debug('-----b------>');
    debug('head index is %s', this.arrayHeadIndex);
    debug('head data index is %s', this.headDataPageIndex);
    debug('head data offset is %s', this.headDataItemOffset);
    debug('tail data index is %s', this.tailDataPageIndex);
    debug('<-----------');

    // prepare the data pointer
    if (this.headDataItemOffset.add(data.length).gt(this.dataPageFactory.pageSize)) { // not enough space
        this.headDataPageIndex = nextIndex(this.headDataPageIndex, this.maxDataFiles);
        this.headDataItemOffset = Bignum(0);
        debug('move data head to next page %s', this.headDataPageIndex);
    }

    var toAppendDataPageIndex = this.headDataPageIndex;
    var toAppendDataItemOffset = this.headDataItemOffset;

    // append data
    var toAppendDataPage = this.dataPageFactory.acquirePage(toAppendDataPageIndex);
    var toAppendDataPageBuffer = toAppendDataPage.getLocal(toAppendDataItemOffset);
    toAppendDataPageBuffer.putBuffer(data);
    toAppendDataPage.setDirty(true);
    // update to next
	this.headDataItemOffset = this.headDataItemOffset.add(data.length);

    var toAppendArrayIndex = this.arrayHeadIndex;
    var toAppendIndexPageIndex = this.getPageIndex(toAppendArrayIndex);
    var toAppendIndexPage = this.indexPageFactory.acquirePage(toAppendIndexPageIndex);
    var toAppendIndexItemOffset = this.getIndexPageOffset(toAppendArrayIndex);

    // update index
    var toAppendIndexPageBuffer = toAppendIndexPage.getLocal(toAppendIndexItemOffset);
    toAppendIndexPageBuffer.putBigLong(toAppendDataPageIndex);
    toAppendIndexPageBuffer.putBigInt(toAppendDataItemOffset);
    toAppendIndexPageBuffer.putBigInt(Bignum(data.length));
    toAppendIndexPageBuffer.putBigLong(Bignum(Date.now()));
    toAppendIndexPage.setDirty(true);

    // advance the head
    this.arrayHeadIndex = nextIndex(this.arrayHeadIndex, this.MAX_INDEX);
    // update meta data
    this.syncHeadIndexToMemory();

    this.dataPageFactory.releasePage(toAppendDataPageIndex);
    this.indexPageFactory.releasePage(toAppendIndexPageIndex);

    debug('----------->');
    debug('head index is %s', this.arrayHeadIndex);
    debug('head data index is %s', this.headDataPageIndex);
    debug('head data offset is %s', this.headDataItemOffset);
    debug('tail data index is %s', this.tailDataPageIndex);
    debug('<-----------');

    function autoBacklog(array, backlog) {
        var backlogBatchSize = array.backlogBatchSize || 10;

        var err;
        if (array.isFull()) { // end of the world check:)
            err = new Error(NodeUtils.format('index space of long type used up (index:%s, max:%s), the end of the world!!!',
                array.arrayHeadIndex, array.MAX_INDEX));
        }
        if (!err && array.isDataFull(data.length)) { // end of the world check:)
            err = new Error(NodeUtils.format('data space of size %s used up, the end of the world!!!',
                array.dataSize()));
        }
        if (err) {
            err.code = 'ENOSPC';
        }

        if (err) {
            if (backlog) {
                // auto deqeue a few
                for (var i = 0; i < backlogBatchSize; i++) {
                    setImmediate(backlog.bind(null, array.shift()));
                }
                autoBacklog(array); // do not pass backlog to detect if attempt to clean up space is not enough
                return;
            }
            throw err;
        }
    }
};


proto.syncHeadIndexToMemory = function syncHeadIndexToMemory() {
    debug('syncHeadIndexToMemory');
    var metaDataPage = this.metaPageFactory.acquirePage(META_DATA_PAGE_INDEX);
    var metaDataBuf = metaDataPage.getLocal(0);
    metaDataBuf.putBigLong(this.arrayHeadIndex);
    metaDataPage.setDirty(true);
};

proto.syncTailIndexToMemory = function syncTailIndexToMemory() {
    debug('syncTailIndexToMemory');
    var metaDataPage = this.metaPageFactory.acquirePage(META_DATA_PAGE_INDEX);
    var metaDataBuf = metaDataPage.getLocal(8);
    metaDataBuf.putBigLong(this.arrayTailIndex);
    metaDataPage.setDirty(true);
};

proto.flush = function flush() {
    this.metaPageFactory.flush();
    this.indexPageFactory.flush();
    this.dataPageFactory.flush();
};

proto.shift = function shift() {
    if (this.isEmpty()) {
        return;
    }

    debug('shift index %s', this.arrayTailIndex);
    var data = this.get(this.arrayTailIndex);

    var nextTailIndex = nextIndex(this.arrayTailIndex, this.MAX_INDEX);
    // do we need gc afterwards? let's check
    var needGC = this.getPageIndex(this.arrayTailIndex)
        .cmp(this.getPageIndex(nextTailIndex)) !== 0;

    // update tail index
    this.arrayTailIndex = nextTailIndex;
    debug('next tail index %s', this.arrayTailIndex);
    this.syncTailIndexToMemory();
    this.syncTailDataIndexFromMemory();

    if (needGC) {
        debug('gc is required');
        this.scheduleGC();
    }

    return data;
};

proto.scheduleGC = function scheduleGC() {
    if (this._gcTimer) {
        // already scheduled
        return;
    }
    this._gcTimer = setTimeout(function onGCTime() {
        this.gc();
        this._gcTimer = undefined;
    }.bind(this));
};

proto.get = function get(index) {

    index = toBignum(index);
    this.validateIndex(index);

    var indexItemBuffer = this.getIndexItemBuffer(index);
    var dataPageIndex = indexItemBuffer.getBigLong();
    var dataItemOffset = indexItemBuffer.getBigInt();
    var dataItemLength = indexItemBuffer.getBigInt();

    var dataPage = this.dataPageFactory.acquirePage(dataPageIndex);
    var data = dataPage.getLocal(dataItemOffset, dataItemLength);
    debug('getting from index %s, offset %s, length: %s', dataPageIndex, dataItemOffset, dataItemLength);
    this.dataPageFactory.releasePage(dataPageIndex);

    return data;
};

proto.getPageIndex = function getPageIndex(index) {
    index = toBignum(index);
    return index.shiftRight(this.INDEX_ITEMS_PER_PAGE_BITS);
};

proto.getDataPageIndex = function getDataPageIndex(index) {
    var indexItemBuffer = this.getIndexItemBuffer(index);
    return indexItemBuffer.getBigLong();
};

function deletePagesInRange(headPageIndex, tailPageIndex, max, pageFactory, callback) {
    headPageIndex = toBignum(headPageIndex);
    tailPageIndex = toBignum(tailPageIndex);
    max = toBignum(max);
    if (tailPageIndex.gt(headPageIndex)) {
        // may happen after cycling through max
        // remove between tail and the head
        debug('deletePagesInRange: [tail:%s, head:%s]', tailPageIndex, headPageIndex);
        pageFactory.deletePagesByIndexRange([headPageIndex, tailPageIndex],  callback);
    }
    else {
        debug('deletePagesInRange: [head:0, tail:%s] and [head:%s, tail:%s]',
            tailPageIndex, headPageIndex, max);
        // most of the time this will be the case where tail chasing the head till it cycles through max
        pageFactory.deletePagesByIndexRange([
            [Bignum(0), tailPageIndex],
            [headPageIndex, max]
        ],  callback);
    }
}

proto.deletePagesOutsideIndexRange = function deletePagesOutsideIndexRange(headIndex, tailIndex, callback) {
    var tasks = [];
    var headPageIndex = this.getPageIndex(headIndex);
    var tailPageIndex = this.getPageIndex(tailIndex);
    debug('deletePagesOutsideIndexRange.index [tail:%s, head:%s]', tailPageIndex, headPageIndex);
    if (tailIndex.cmp(headIndex) !== 0) {
        tasks.push(deletePagesInRange.bind(null,
            nextIndex(headPageIndex, this.MAX_INDEX),
            prevIndex(tailPageIndex, this.MAX_INDEX),
            this.MAX_INDEX,
            this.indexPageFactory));
    }

    // no see if we need to clean up data pages
    var headDataPageIndex = this.getDataPageIndex(headIndex);
    var tailDataPageIndex = this.getDataPageIndex(tailIndex);
    debug('deletePagesOutsideIndexRange.data [tail:%s, head:%s]', tailDataPageIndex, headDataPageIndex);
    if (tailDataPageIndex.cmp(headDataPageIndex) !== 0) {
        tasks.push(deletePagesInRange.bind(null,
            nextIndex(headDataPageIndex, this.maxDataFiles),
            prevIndex(tailDataPageIndex, this.maxDataFiles),
            this.maxDataFiles,
            this.dataPageFactory));
    }

    if (tasks.length === 0) {
        callback();
        return;
    }

    Async.parallel(tasks, callback);
};

proto.gc = function gc() {

    this.deletePagesOutsideIndexRange(this.arrayHeadIndex, this.arrayTailIndex, function (err) {
        if (err) {
            console.log('got error while cleaning up array', err && err.stack || err);
        }
    });
};

function mod(val, bits) {
    return val.sub(val.shiftRight(bits).shiftLeft(bits));
}
