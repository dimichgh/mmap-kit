'use strict';

var Bignum = require('bignum');
var MMap = require('mmap.js');
var debug = require('debug')('mmap-kit/page/' + process.pid);

var ByteBuffer = require('./byte-buffer');

function Page(buffer, pageFile, index) {
    this.buffer = buffer;
    this.pageFile = pageFile;
    this.index = index;
    this.dirty = false;
}

module.exports.Page = Page;

var proto = Page.prototype;

proto.close = function close() {
    if (!this.buffer) {
        return;
    }
    this.flush();
    this.buffer = null;
    debug('page is closed');
};

proto.isClosed = function isClosed() {
    return this.buffer === null;
};

/*
 * @throws Error
*/
proto.flush = function flush() {
    if (this.dirty) {
        MMap.sync(this.buffer);
        this.dirty = false;
        debug('mmap page %s has been flushed', this.index);
    }
};

proto.setDirty = function setDirty(dirty) {
    this.dirty = dirty;
};

proto.getLocal = function getLocal(position, length) {
    position = position || 0;
    position = position.toNumber ? position.toNumber() : position;
    length = length || this.buffer.length;
    var end = position + (length && length.toNumber ? length.toNumber() : length);
    return ByteBuffer.extend(this.buffer.slice(position, Math.min(end, this.buffer.length)));
};
