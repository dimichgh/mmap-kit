'use strict';

var Fs = require('fs');
var Path = require('path');

var Bignum = require('bignum');
var Async = require('async');
var MMap = require('mmap.js');
var LRU = require('lru-cache');
var debug = require('debug')('mmap-kit/page-factory/' + process.pid);

var mkdir = require('./utils').mkdir;
var Page = require('./page').Page;

var PAGE_FILE_NAME = 'page';
var PAGE_FILE_SUFFIX = '.dat';

function PageFactory(pageSize, pageDir, options) {
    options = options || {};
    this.cache = new LRU({
        dispose: function onDispose(key, page) {
            debug('disposing page %s from cache, file: %s', key, page.pageFile);
            page.close();
        },
        max: options.max || 100000, // 400Mb for page size of 4k
        maxAge: options.ttl || 1000 * 60 * 20 // 20 minutes in memory
    });

    this.pageSize = PageFactory.normalizePageSize(pageSize);
    debug('using page size', this.pageSize);
    this.pageDir = pageDir;
    this.options = options;

    // make sure the page dir exists
    mkdir(pageDir);

    this.pageFile = Path.resolve(this.pageDir, PAGE_FILE_NAME + '-');
}

PageFactory.normalizeBignumSize = function normalizeBignumSize(size, defaultSize) {
    return size ? size.div(MMap.PAGE_SIZE).mul(MMap.PAGE_SIZE) : defaultSize;
};

PageFactory.normalizePageSize = function normalizePageSize(pageSize) {
    return parseInt(pageSize/MMap.PAGE_SIZE) * MMap.PAGE_SIZE || pageSize && MMap.PAGE_SIZE || MMap.PAGE_SIZE * 1024 * 32; // 32Mb by default
};

module.exports.PageFactory = PageFactory;
module.exports.PageFactory.PAGE_FILE_NAME = PAGE_FILE_NAME;
module.exports.PageFactory.PAGE_FILE_SUFFIX = PAGE_FILE_SUFFIX;

var proto = PageFactory.prototype;

proto.getCacheSize = function getCacheSize() {
    return this.cache.length;
};

/*
 * @throws Error
*/
proto.acquirePage = function acquirePage(index) {
    var key = '' + index;
    var page = this.cache.get(key);
    if (page) {
        // debug('using existing page', key);
        return page;
    }

    debug('acquiring page %s', index);

    var fileName = this.getFileNameByIndex(key);

    debug('filename %s for the page %d', fileName, index);

    var isNewPage = false;
    if (!Fs.existsSync(fileName)) {
        debug('creating page file', fileName);
        var initialBuffer;
        // below node 4.x, zero the buffers
        if (parseInt(process.versions.node.split('.')[0]) < 6) {
            initialBuffer = new Buffer(this.pageSize);
            initialBuffer.fill(0);
        }
        else {
            initialBuffer = Buffer.alloc(this.pageSize);
        }
        Fs.writeFileSync(fileName, initialBuffer);
        isNewPage = true;
    }
    var fd = Fs.openSync(fileName, 'r+');

    var buffer = MMap.alloc(
        this.pageSize,
        MMap.PROT_READ | MMap.PROT_WRITE,
        MMap.MAP_SHARED,
        fd,
        0);

    Fs.closeSync(fd);

    page = new Page(buffer, fileName, index);

    this.cache.set(key, page);

    debug('cached mmap page %s', fileName);

    return page;
};

proto.releaseCachedPages = function releaseCachedPages() {
    this.cache.reset();
};

proto.releaseCachedPage = function releaseCachedPage(index) {
	this.cache.del(''+index);
};

proto.close = function close(index) {
    var key = ''+index;
	var page = this.cache.get(key);
    if (page) {
        this.cache.del(key);
        page.close();
    }
};

proto.deleteAllPages = function deleteAllPages(callback) {
    callback = callback || function noop(err) {
        if (err) {
            debug(err);
        }
    };
    this.cache.reset();
    this.getExistingBackFileIndexes(function handleIndexes(err, indexes) {
        if (err) {
            return callback(err);
        }
        this.deletePages(indexes, function (err) {
            if (err) {
                debug('fail to delete pages', err);
            }
            else {
                debug('all pages (%s total) has been deleted', indexes.length);
            }
            callback.apply(null, arguments);
        });
    }.bind(this));
};

proto.releasePage = function releasePage(index) {
	this.cache.del(index);
};

proto.deletePages = function deletePages(indexes, callback) {
    debug('deleting pages', indexes);
    // TODO: do we want to fail immediately and collect all failed attempts and show them?
    // For now we fail immediately
    Async.each(indexes, function forEach(index, done) {
        this.deletePage(index, done);
    }.bind(this), callback);
};

proto.deletePage = function deletePage(index, callback) {
    debug('deleting page', index);
    this.releasePage(''+index);

    var fileName = this.getFileNameByIndex(index);

    return removeFile(5, fileName);

    function removeFile(count, fileName) {
        count--;
        Fs.unlink(fileName, function handle(err) {
            if (err && Fs.existsSync(fileName)) {
                // retry
                if (count > 0) {
                    debug('retrying deletion of %s, attempt %d', fileName, 5 - count);
                    return setTimeout(removeFile.bind(null. count, fileName), 100);
                }
            }
            if (err) {
                debug('failed to delete page %d', index, fileName);
            }
            else {
                debug('deleted page %d', index, fileName);
            }

            callback(err);
        });
    }
};

proto.getFileNameByIndex = function getFileNameByIndex(index) {
	return this.pageFile + index + PAGE_FILE_SUFFIX;
};

proto.flush = function flush() {
    this.cache.values().forEach(function (page) {
        page.flush();
    });
};

proto.getBackPageFiles = function getBackPageFiles(callback) {
    debug('getting back page files for', this.pageDir);
    Fs.readdir(this.pageDir, function handle(err, files) {
        if (err) {
            debug('failed to read pages from %s', this.pageDir, err);
            return callback(err);
        }
        var list = files.reduce(function map(memo, file) {
            if (Path.extname(file) === PAGE_FILE_SUFFIX) {
                memo.push(Path.resolve(this.pageDir, file));
            }
            return memo;
        }.bind(this), []);
        callback(null, list);
    }.bind(this));
};

proto.getExistingBackFileIndexes = function getExistingBackFileIndexes(callback) {
    this.getBackPageFiles(function generateIndexes(err, files) {
        if (err) {
            return callback(err);
        }

        var indexes = files.map(function map(file) {
            return getIndexByFileName(file);
        }, []);

        callback(null, indexes);
    });
};

proto.getBackPageFileSize = function getBackPageFileSize(callback) {
    this.getBackPageFiles(function calculateSize(err, files) {
        if (err) {
            return callback(err);
        }

        var total = Bignum(0);
        Async.each(files, function addSize(file, next) {
            Fs.stat(file, function handleStats(err, stat) {
                if (stat) {
                    total = total.add(stat.size);
                }
                next(err);
            });
        }, function handleTotal(err) {
            if (err) {
                return callback(err);
            }
            callback(undefined, total);
        });
    });
};

/*
 * @deprecated, please use deletePagesByIndexRange
*/
proto.deletePagesBeforePageIndex = function deletePagesBeforePageIndex(pageIndex, callback) {
    this.getExistingBackFileIndexes(function handle(err, indexes) {
        indexes = indexes.reduce(function reduce(memo, index) {
            if (index.lt(pageIndex)) {
                memo.push(index);
            }
            return memo;
        }, []);

        this.deletePages(indexes, callback);
    }.bind(this));
};

/*
 * Delete pages by index range or ranges
 *   range is [start, end], including end
*/
proto.deletePagesByIndexRange = function deletePagesByIndexRange(ranges, callback) {
    ranges = Array.isArray(ranges[0]) ? ranges : [ranges];
    this.getExistingBackFileIndexes(function handle(err, indexes) {
        indexes = indexes.reduce(function reduce(memo, index) {
            if (ranges.some(function some(range) {
                return index.ge(range[0]) && index.le(range[1]);
            })) {
                memo.push(index);
            }
            return memo;
        }, []);

        this.deletePages(indexes, callback);
    }.bind(this));
};

function getIndexByFileName(fileName) {
	var beginIndex = fileName.lastIndexOf('-');
	beginIndex += 1;
	var endIndex = fileName.lastIndexOf(PAGE_FILE_SUFFIX);
	var sIndex = fileName.substring(beginIndex, endIndex);
	return Bignum(sIndex);
}
