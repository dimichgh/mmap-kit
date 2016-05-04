'use strict';

var Path = require('path');

var Async = require('async');
var Bignum = require('bignum');
var debug = require('debug')('mmap-kit/big-queue/' + process.pid);

var BigArray = require('./big-array').BigArray;
var Long = require('./big-array').Long;
var PageFactory = require('./page-factory').PageFactory;

/**
 * A big, fast and persistent queue implementation.
 * <p/>
 * Main features:
 * 1. FAST : close to the speed of direct memory access, both enqueue and dequeue are close to O(1) memory access.
 * 2. MEMORY-EFFICIENT : automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple in-process workers can concurrently enqueue and dequeue without data corruption.
 * 4. PERSISTENT - all data in queue is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the queued data is only limited by the available disk space.
 *
 * converted from https://github.com/bulldog2011/bigqueue/blob/master/src/main/java/com/leansoft/bigqueue/BigArrayImpl.java
 */

/**
 * A big, fast and persistent queue implementation.
 *
 * @param queueDir  the directory to store queue data
 * @param queueName the name of the queue, will be appended as last part of the queue directory
 * @param pageSize  the back data file size per page in bytes, see minimum allowed {@link BigArrayImpl#MINIMUM_DATA_PAGE_SIZE}
 * @param maxDataSize (Mb) the max back data file size, see minimum allowed {@link #MINIMUM_DATA_PAGE_SIZE}}.
 */
function BigQueue(queueDir, queueName, pageSize, maxDataSize) {

    this.innerArray = new BigArray(queueDir, queueName, pageSize, maxDataSize);

    this.setAutoSync(true);
    this.sync(true);
}

module.exports.BigQueue = BigQueue;

var proto = BigQueue.prototype;

proto.setAutoSync = function setAutoSync(enable) {
    this.autoSync = enable;
    this.innerArray.setAutoSync(enable);
};

proto.sync = function sync() {
    this.innerArray.sync();
};

proto.isEmpty = function isEmpty() {
    return this.innerArray.isEmpty();
};

/**
 * throws out of space error (err.code = 'ENOSPC')
*/
proto.enqueue = function enqueue(data) {
    this.innerArray.append(data);
};

proto.dequeue = function dequeue() {
    if (this.isEmpty()) {
        return;
    }

    return this.innerArray.shift();
};

/*
 * We do not expect this to be called at runtime, unless to clean up everything
 * Hence no locking.
 * If one needs to synchonize actions, locing should be done in higher layer of API
*/
proto.removeAll = function removeAll(callback) {
    this.innerArray.removeAll(callback);
};

proto.peek = function peek() {
    if (this.isEmpty()) {
        return;
    }
    return this.innerArray.get(this.innerArray.arrayTailIndex);
};

/**
 * Sync version of iterator
*/
proto.eachSync = function eachSync(iteratorFn) {
    if (this.isEmpty()) {
        return;
    }

    var index = this.innerArray.arrayTailIndex;
    var size = this.innerArray.size();
    for (var i = index; i.lt(size); i = i.add(1)) {
        iteratorFn(this.innerArray.get(i), i);
    }
};

/**
 * Async version of iterator
 * Not conflict safe, one must implement locking if multiple flows modify the queue elements
 * interatorFnAsync(element, index, callback)
*/
proto.each = function each(iteratorFnAsync, callback) {
    if (this.isEmpty()) {
        return callback();
    }

    var size = this.innerArray.size();
    var innerArray = this.innerArray;
    var index = this.innerArray.arrayTailIndex.sub(1);
    Async.whilst(function test() {
        index = index.add(1);
        return index.lt(size);
    }, function iterate(next) {
        setImmediate(function asyncFn() {
            try {
                iteratorFnAsync(innerArray.get(index), index, next);
            }
            catch (err) {
                callback(err);
            }
        });
    }, callback);
};

proto.close = function close() {
    this.innerArray.close();
};

proto.flush = function flush() {
    this.innerArray.flush();
};

proto.size = function size() {
    return this.innerArray.size();
};
