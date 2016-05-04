'use strict';

var NodeUtils = require('util');
var Fs = require('fs');
var Path = require('path');
var Test = require('tape');
var Async = require('async');
var Bignum = require('bignum');
var mkdir = require('shelljs').mkdir;
var rm = require('shelljs').rm;
var BigArray = require('../lib/big-array').BigArray;
var Long = require('../lib/big-array').Long;
var Utils = require('./fixtures/utils');

var testDir = Path.resolve(__dirname, '.tmp', 'bigarray/unit');

Test(__filename, function (t) {

    t.test('before', function (t) {
        // make sure it is cleaned if not
        rm('-rf', Path.resolve(__dirname, '.tmp', 'bigarray'));
        t.end();
    });

    t.test('simpleTest', function (t) {
        t.timeoutAfter(5000);

	    var bigArray = new BigArray(testDir, 'simple_test');

        var st;
        Async.timesSeries(3, function iter(i, next) {
            t.equal(bigArray.arrayTailIndex.toString(10), Bignum(0).toString(10), 'tail index should be 0');
            t.equal(bigArray.arrayHeadIndex.toString(10), Bignum(0).toString(10), 'head index should be 0');
            t.equal(bigArray.size().toString(10), Bignum(0).toString(10), 'initial size should be 0');
            t.ok(bigArray.isEmpty(), 'initial array should be empty');
            t.ok(!bigArray.isFull(), 'initial array should not be full');
            t.ok(!bigArray.isDataFull(), 'array should not be data full');

			try {
				bigArray.get(0);
				t.fail('IndexOutOfBoundsException should be thrown here');
			} catch (ex) {
			}
			try {
				bigArray.get(1);
				t.fail('IndexOutOfBoundsException should be thrown here');
			} catch (ex) {
			}
			try {
				bigArray.get(Long.MAX_VALUE);
				t.fail('IndexOutOfBoundsException should be thrown here');
			} catch (ex) {
			}

			bigArray.append(new Buffer('hello'));
            t.equal(bigArray.arrayTailIndex.toString(10), Bignum(0).toString(10), 'tail index should be 0');
            t.equal(bigArray.arrayHeadIndex.toString(10), Bignum(1).toString(10), 'head index should get incremented');
            t.equal(bigArray.size().toString(10), Bignum(1).toString(10), 'size should be 1');
            t.ok(!bigArray.isEmpty(), 'array should not be empty');
            t.ok(!bigArray.isFull(), 'array should not be full');
            t.ok(!bigArray.isDataFull(), 'array should not be data full');
            t.equal(bigArray.get(0).toString(), 'hello');

			bigArray.flush();

			bigArray.append(new Buffer('world'));
            t.equal(bigArray.arrayTailIndex.toString(10), Bignum(0).toString(10), 'tail index should be 0');
            t.equal(bigArray.arrayHeadIndex.toString(10), Bignum(2).toString(10), 'head index should be 2');
            t.equal(bigArray.size().toString(10), Bignum(2).toString(10), 'size should be 2');
            t.equal(bigArray.dataSize().toNumber(), 10, 'data size should be 10');
            t.ok(!bigArray.isEmpty(), 'array should not be empty');
            t.ok(!bigArray.isFull(), 'array should not be full');
            t.ok(!bigArray.isDataFull(), 'array should not be data full');
            t.equal(bigArray.get(0).toString(), 'hello', 'should have hello');
            t.equal(bigArray.get(1).toString(), 'world', 'should have world');

			bigArray.removeAll(next);

        }, function (err) {
            t.ok(!err, err && err.stack);
            t.end();
        });

	});

    t.test('persistence', function (t) {
        t.timeoutAfter(5000);

        var st;
        Async.series([
            function write(next) {
                var bigArray = new BigArray(testDir, 'persistence_test');
                t.equal(bigArray.arrayTailIndex.toString(10), Bignum(0).toString(10), 'tail index should be 0');
                t.equal(bigArray.arrayHeadIndex.toString(10), Bignum(0).toString(10), 'head index should be 0');
                t.equal(bigArray.size().toNumber(), 0, 'initial size should be 0');
                t.equal(bigArray.dataSize().toNumber(), 0, 'initial size should be 0');
                t.ok(bigArray.isEmpty(), 'initial array should be empty');
                t.ok(!bigArray.isFull(), 'initial array should not be full');
                t.ok(!bigArray.isDataFull(), 'array should not be data full');

    			bigArray.append(new Buffer('hello'));
                t.equal(bigArray.arrayTailIndex.toNumber(), 0, 'tail index should be 0');
                t.equal(bigArray.arrayHeadIndex.toNumber(), 1, 'head index should get incremented');
                t.equal(bigArray.size().toNumber(), 1, 'size should be 1');
                t.ok(!bigArray.isEmpty(), 'array should not be empty');
                t.ok(!bigArray.isFull(), 'array should not be full');
                t.ok(!bigArray.isDataFull(), 'array should not be data full');
                t.equal(bigArray.get(0).toString(), 'hello');

    			bigArray.flush();

    			bigArray.append(new Buffer('world'));
                t.equal(bigArray.arrayTailIndex.toNumber(), 0, 'tail index should be 0');
                t.equal(bigArray.arrayHeadIndex.toNumber(), 2, 'head index should be 2');
                t.equal(bigArray.size().toNumber(), 2, 'size should be 2');
                t.equal(bigArray.dataSize().toNumber(), 10, 'data size should be 10');
                t.ok(!bigArray.isEmpty(), 'array should not be empty');
                t.ok(!bigArray.isFull(), 'array should not be full');
                t.ok(!bigArray.isDataFull(), 'array should not be data full');
                t.equal(bigArray.get(0).toString(), 'hello', 'should have hello');
                t.equal(bigArray.get(1).toString(), 'world', 'should have world');
                bigArray.flush();
                bigArray.close();
                next();
            },
            function read(next) {
                var bigArray = new BigArray(testDir, 'persistence_test');
                t.equal(bigArray.arrayTailIndex.toNumber(), 0, 'tail index should be 0');
                t.equal(bigArray.arrayHeadIndex.toNumber(), 2, 'head index should be 2');
                t.equal(bigArray.size().toNumber(), 2, 'size should be 2');
                t.equal(bigArray.dataSize().toNumber(), 10, 'data size should be 10');
                t.ok(!bigArray.isEmpty(), 'array should not be empty');
                t.ok(!bigArray.isFull(), 'array should not be full');
                t.ok(!bigArray.isDataFull(), 'array should not be data full');
                t.equal(bigArray.get(0).toString(), 'hello', 'should have hello');
                t.equal(bigArray.get(1).toString(), 'world', 'should have world');
                next();
            }
        ], function (err) {
            t.ok(!err, err && err.stack);
            t.end();
        });

	});

    t.test('removeBeforeIndexTest', function (t) {
        t.timeoutAfter(20000);

        var bigArray = new BigArray(testDir, "remove_before_index_test");

        var loop = 500;
        for(var i = 0; i < loop; i++) {
            bigArray.append(new Buffer('' + i));
        }

        var half = loop / 2;
        var last;
        Async.series([
            function removeHalf(next) {
                t.equal(bigArray.get(half).toString(), half + '');
                t.equal(bigArray.get(half + 1).toString(), half + 1 + '');
                bigArray.removeBeforeIndex(half, next);
            },
            function removeHalf(next) {
                t.equal(bigArray.arrayTailIndex.toNumber(), half);
                t.equal(bigArray.size().toNumber(), half);
                t.equal(bigArray.get(half).toString(), half + '');
                t.equal(bigArray.get(half + 1).toString(), (half + 1) + '');
                t.equal(bigArray.get(half + 2).toString(), (half + 2) + '');
                try {
                    bigArray.get(half - 1);
                    t.fail("IndexOutOfBoundsException should be thrown here");
                } catch (IndexOutOfBoundsException) {
                }
                next();
            },
            function removeLast(next) {
                last = loop - 1;
                bigArray.removeBeforeIndex(last, next);
            },
            function removeLast(next) {
                t.equal(bigArray.arrayTailIndex.toNumber(), last);
                t.equal(bigArray.size().toNumber(), 1);
                t.equal(last + '', bigArray.get(last).toString());
                try {
                    bigArray.get(last - 1);
                    t.fail("IndexOutOfBoundsException should be thrown here");
                } catch (IndexOutOfBoundsException) {
                }
                t.end();
            }
        ]);

    });

    t.test('removeBeforeIndexTestWithLoop', function (t) {
        var bigArray = new BigArray(testDir, 'big_loop_test');

        var i;
        var loop = 1000;
        var lastAppendTime = Date.now();
        for(i = 0; i < loop; i++) {
            bigArray.append(new Buffer('' + i));
            t.equal(bigArray.arrayTailIndex.toNumber(), 0);
            t.equal(bigArray.arrayHeadIndex.toNumber(), i + 1);
            t.equal(bigArray.size().toNumber(), i + 1);
            t.ok(!bigArray.isEmpty());
            t.ok(!bigArray.isFull());
        }

        try {
            bigArray.get(loop);
            t.fail('IndexOutOfBoundsException should be thrown here');
        } catch (IndexOutOfBoundsException) {
        }

        t.equal(bigArray.arrayTailIndex.toNumber(), 0);
        t.equal(bigArray.arrayHeadIndex.toNumber(), loop);
        t.equal(bigArray.size().toNumber(), loop);
        t.ok(!bigArray.isEmpty());
        t.ok(!bigArray.isFull());
        bigArray.flush();
        bigArray.close();

        // create a new instance on exiting array
        bigArray = new BigArray(testDir, 'big_loop_test');
        t.equal(bigArray.arrayTailIndex.toNumber(), 0);
        t.equal(bigArray.arrayHeadIndex.toNumber(), loop);
        t.equal(bigArray.size().toNumber(), loop);
        t.ok(!bigArray.isEmpty());
        t.ok(!bigArray.isFull());

        for(i = 0; i < 10; i++) {
            bigArray.append(new Buffer('' + i));
            t.equal(i + '', bigArray.get(loop + i).toString());
        }
        t.equal(bigArray.arrayTailIndex.toNumber(), 0);
        t.equal(bigArray.arrayHeadIndex.toNumber(), loop + 10);
        t.equal(bigArray.size().toNumber(), loop + 10);
        t.ok(!bigArray.isEmpty());
        t.ok(!bigArray.isFull());
        t.end();
    });

    t.test('loopTimingTest', function (t) {
        var bigArray = new BigArray(testDir, 'loop_timing_test');

        var i;
        var loop = 10000;
        var begin = Date.now();
        for(i = 0; i < loop; i++) {
            bigArray.append(new Buffer('' + i));
        }
        console.log('Time used to sequentially append ' + loop + ' items : ' + (Date.now() - begin)/1000 + ' seconds.');

        begin = Date.now();
        for(i = 0; i < loop; i++) {
            t.equal('' + i, bigArray.get(i).toString());
        }
        console.log('Time used to sequentially read ' + loop + ' items : ' + (Date.now() - begin)/1000 + ' seconds.');

        begin = Date.now();
        var list = [];
        for(i = 0; i < loop; i++) {
            list.push(i);
        }
        Utils.shuffle(list);
        console.log('Time used to shuffle ' + loop + ' items : ' + (Date.now() - begin)/1000 + ' seconds.');

        begin = Date.now();
        list.forEach(function forEach(index) {
            t.equal('' + index, bigArray.get(index).toString());
        });
        console.log('Time used to randomly read ' + loop + ' items : ' + (Date.now() - begin)/1000 + ' seconds.');
        t.end();
    });

    t.test('getBackFileSizeTest', function (t) {
        t.timeoutAfter(20000);
        var bigArray = new BigArray(testDir, 'get_back_file_size_test');
        // make it fast to run out of items per page for testing pruposes
        bigArray.INDEX_ITEMS_PER_PAGE_BITS = 10;
        bigArray.init();
        t.equal(bigArray.INDEX_PAGE_SIZE, 1024 * 32);

        var loop = 3000;

        Async.series([
            function getBackFileSize(next) {
                bigArray.getBackFileSize(function onSize(err, size) {
                    t.ok(!err, 'no error');
                    t.equal(size.toNumber(), 0);
                    next();
                });
            },
            function append(next) {
                bigArray.append(new Buffer('hello'));
                next();
            },
            function compare(next) {
                bigArray.getBackFileSize(function calc(err, backFileSize) {
                    t.ok(!err, err && err.stack);
                    t.equal(backFileSize.toNumber(),
                        bigArray.dataPageSize.add(bigArray.INDEX_PAGE_SIZE).toNumber(), 'after appending "hello"');
                    next();
                });
            },
            function append(next) {
                var randomString = Utils.randomString(256 * 1024);
                Async.timesSeries(loop, function iter(n, done) {
                    setImmediate(function append() {
                        bigArray.append(new Buffer(randomString));
                        done();
                    });
                    // cause some trouble, make it harder
                    if (n % 300 === 0) {
                        bigArray.metaPageFactory.close(0);
                    }
                }, next);
            },
            function compare(next) {
                bigArray.getBackFileSize(function calc(err, backFileSize) {
                    t.ok(!err, err && err.stack);
                    var expectedSize = bigArray.INDEX_PAGE_SIZE * 3 + bigArray.dataPageSize * 6;
                    t.equal(expectedSize, backFileSize.toNumber(), 'last append with random string');
                    next();
                });
            },
            bigArray.removeBeforeIndex.bind(bigArray, parseInt(loop / 2)),
            function compare(next) {
                bigArray.getBackFileSize(function calc(err, backFileSize) {
                    t.ok(!err, err && err.stack);
                    var expectedSize = bigArray.INDEX_PAGE_SIZE * 2 + bigArray.dataPageSize * 4;
                    t.equal(expectedSize, backFileSize.toNumber());
                    next();
                });
            },
            bigArray.removeAll.bind(bigArray),
            function compare(next) {
                bigArray.getBackFileSize(function onSize(err, size) {
                    t.ok(!err, err && err.stack);
                    t.equal(size.toNumber(), 0);
                    next();
                });
            }
        ], function onComplete(err) {
            t.ok(!err, err && err.stack);
            t.end();
        });

    });

    t.test('loopThroughMax', function (t) {
        t.timeoutAfter(5000);
        var bigArray = new BigArray({
            arrayDir: testDir,
            arrayName: 'loopThroughMax',
            MAX_INDEX: 20
        });

        var loop = 20;
        var lastTailIndex;
        var randomString = Utils.randomString(256);

        Async.series([
            function getBackFileSize(next) {
                bigArray.getBackFileSize(function onSize(err, size) {
                    if (err) {
                        return next(err);
                    }
                    t.equal(size.toNumber(), 0, 'size should be 0');
                    next();
                });
            },
            function append(next) {
                t.equal(bigArray.arrayTailIndex.toNumber(), 0, 'tail index should be 0');
                t.equal(bigArray.arrayHeadIndex.toNumber(), 0, 'head index should be 0');
                t.ok(bigArray.isEmpty(), 'initial array should be empty');
                t.ok(!bigArray.isFull(), 'initial array should not be full');
                for(var i = 0; i < 19; i++) {
                    bigArray.append(new Buffer(randomString));
                }
                t.equal(bigArray.arrayTailIndex.toNumber(), 0, 'tail index should be 0');
                t.equal(bigArray.arrayHeadIndex.toNumber(), 19, 'head index should be 19');
                t.ok(!bigArray.isEmpty(), 'array should not be empty');
                t.ok(bigArray.isFull(), 'array should be full'); // _FAIL
                t.equal(bigArray.dataSize().toNumber(), 19 * 256, 'data size for 18 elements');
                t.equal(bigArray.size().toNumber(), 19, 'index size for 20 elements');
                try {
                    bigArray.append(new Buffer(randomString));
                    t.fail('should have failed');
                }
                catch (err) {
                    t.pass('should have failed');
                }
                t.equal(randomString, bigArray.shift().toString(), 'should be equal');
                t.equal(18 * 256, bigArray.dataSize().toNumber(), 'data size for 19 elements'); // _FAIL
                t.equal(bigArray.size().toNumber(), 18, 'index size for 20 elements');
                t.equal(bigArray.arrayTailIndex.toNumber(), 1, 'tail index should be 0');
                t.equal(bigArray.arrayHeadIndex.toNumber(), 19, 'head index should be 0');
                t.ok(!bigArray.isEmpty(), 'array should not be empty');
                t.ok(!bigArray.isFull(), 'array should not be full');
                bigArray.append(new Buffer(randomString));
                t.equal(19 * 256, bigArray.dataSize().toNumber(), 'data size for 20 elements');
                t.equal(19, bigArray.size().toNumber(), 'index size for 20 elements');
                t.equal(bigArray.arrayTailIndex.toNumber(), 1);
                t.equal(bigArray.arrayHeadIndex.toNumber(), 0, 'head index should be 0');
                t.ok(!bigArray.isEmpty(), 'array should not be empty');
                t.ok(bigArray.isFull(), 'array should be full');
                next();
            },
            function compare(next) {

                bigArray.getBackFileSize(setImmediate.bind(null, function calc(err, size) {
                    if (err) {
                        return next(err);
                    }
                    t.equal(size.toNumber(),
                        bigArray.dataPageSize.add(bigArray.INDEX_PAGE_SIZE).toNumber(),
                        NodeUtils.format('the size (%s) should still be equal to %d',
                        size, bigArray.dataPageSize.add(bigArray.INDEX_PAGE_SIZE)));
                    next();
                }));
            },
        ], function onComplete(err) {
            t.ok(!err, err && err.stack || 'no error');
            t.end();
        });

    });

    t.test('loopThroughDataMax', function (t) {
        t.timeoutAfter(5000);
        var bigArray = new BigArray({
            arrayDir: testDir,
            arrayName: 'loopThroughDataMax',
            MAX_INDEX: 40, // max 33 index pages
            overrideMinDataPageSize: 128 * 1024,
            dataPageSize: 128 * 1024,
            maxDataSize: 1 // 1Mb
        });

        var loop = 32;
        var lastTailIndex;
        // at least 4 chunks per data page based o
        // given 128k for min data page size and 1Mb for max data size, we need 8 pages to reach the max
        // which requires 8 * 4 = 32 chunks of 30K each
        var randomString = Utils.randomString(30000);

        Async.series([
            function getBackFileSize(next) {
                bigArray.getBackFileSize(function onSize(err, size) {
                    if (err) {
                        return next(err);
                    }
                    t.equal(size.toNumber(), 0, 'size should be 0');
                    next();
                });
            },
            function maxOut(next) {
                t.equal(bigArray.getDataPageIndex(bigArray.arrayTailIndex).toNumber(), 0, 'data tail');
                t.equal(bigArray.getDataPageIndex(bigArray.arrayHeadIndex).toNumber(), 0, 'data head');

                for (var i = 0; i < loop; i++) {
                    bigArray.append(new Buffer(randomString));
                }
                t.equal(bigArray.getDataPageIndex(bigArray.arrayHeadIndex.sub(1)).toNumber(), 7, 'data head');
                t.ok(!bigArray.isEmpty(), 'array should not be empty');
                t.ok(!bigArray.isFull(), NodeUtils.format('array should not be full [tail:%s, head:%s]',
                    bigArray.arrayTailIndex, bigArray.arrayHeadIndex));
                t.ok(!bigArray.isDataFull(), NodeUtils.format('array should not be data full size: %s',
                    bigArray.dataSize()));

                try {
                    bigArray.append(new Buffer(randomString));
                    t.fail('should have failed');
                }
                catch (err) {
                    t.pass('expected to fail');
                }
                t.equal(bigArray.getDataPageIndex(bigArray.arrayTailIndex).toNumber(), 0, 'data tail');
                t.equal(bigArray.getDataPageIndex(bigArray.arrayHeadIndex.sub(1)).toNumber(), 7, 'data head should have moved');

                var size = bigArray.dataSize();
                // consume 5
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                t.equal(bigArray.dataSize().toNumber(), 876432, 'size should be 5 elements down');

                t.equal(bigArray.arrayTailIndex.toNumber(), 5, 'tail should have moved');
                t.equal(bigArray.arrayHeadIndex.toNumber(), 32, 'head should stay where it was');

                t.equal(bigArray.getDataPageIndex(bigArray.arrayTailIndex).toNumber(), 1, 'data tail move');
                t.equal(bigArray.getDataPageIndex(BigArray.prevIndex(bigArray.arrayHeadIndex, 40)).toNumber(), 7, 'data head stay');

                // move tail to page 2
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                t.equal(bigArray.arrayTailIndex.toNumber(), 10, 'index tail @10');
                // tail: 2 : 60000

                t.equal(bigArray.getDataPageIndex(bigArray.arrayTailIndex).toNumber(), 2, 'data tail move');
                t.equal(bigArray.getDataPageIndex(BigArray.prevIndex(bigArray.arrayHeadIndex, 40)).toNumber(), 7, 'data head stay');

                // new cycle
                // now add more data to let head cycle through max data
                bigArray.append(new Buffer(randomString));
                t.equal(bigArray.getDataPageIndex(BigArray.prevIndex(bigArray.arrayHeadIndex, 40)).toNumber(), 0, 'data head move');
                bigArray.append(new Buffer(randomString));
                bigArray.append(new Buffer(randomString));
                bigArray.append(new Buffer(randomString));
                t.equal(bigArray.getDataPageIndex(BigArray.prevIndex(bigArray.arrayHeadIndex, 40)).toNumber(), 0, 'data head move');

                // data page 1
                bigArray.append(new Buffer(randomString)); // 36
                t.equal(BigArray.prevIndex(bigArray.arrayHeadIndex, 40).toNumber(), 36, 'data head move');
                t.equal(bigArray.getDataPageIndex(BigArray.prevIndex(bigArray.arrayHeadIndex, 40)).toNumber(), 1, 'data head move');
                bigArray.append(new Buffer(randomString)); // 37
                bigArray.append(new Buffer(randomString)); // 38
                bigArray.append(new Buffer(randomString)); // 39 / data - 1:120000
                t.equal(bigArray.getDataPageIndex(BigArray.prevIndex(bigArray.arrayHeadIndex, 40)).toNumber(), 1, 'data head move');
                t.equal(BigArray.prevIndex(bigArray.arrayHeadIndex, 40).toNumber(), 39, 'data head move');

                // data page 2
                bigArray.append(new Buffer(randomString));
                t.equal(bigArray.getDataPageIndex(BigArray.prevIndex(bigArray.arrayHeadIndex, 40)).toNumber(), 2, 'data head move');
                t.equal(BigArray.prevIndex(bigArray.arrayHeadIndex, 40).toNumber(), 0, 'data head move');

                try {
                    bigArray.append(new Buffer(randomString)); // 2 : 60000
                    t.fail('should have failed');
                }
                catch (err) {
                    t.pass('expected to fail');
                }

                // move tail to 2:90000
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                bigArray.append(new Buffer(randomString)); // 2 : 60000
                t.equal(bigArray.shift().toString(), randomString, 'should match string written');

                // just loop
                for (i = 0; i < 500; i++) {
                    t.equal(bigArray.shift().toString(), randomString, 'should match string written');
                    bigArray.append(new Buffer(randomString)); // 2 : 60000
                }

                next();
            }
        ], function onComplete(err) {
            t.ok(!err, err && err.stack || 'no error');
            t.end();
        });

    });

    t.test('autoBacklog', function (t) {
        t.timeoutAfter(5000);
        var bigArray = new BigArray({
            arrayDir: testDir,
            arrayName: 'autoBacklog',
            MAX_INDEX: 40, // max 33 index pages
            overrideMinDataPageSize: 128 * 1024,
            maxDataSize: 1, // 1Mb
                dataPageSize: 128 * 1024,
            backlog: function autoBacklog(data) {
                backlog.push(data);
            }
        });

        var backlog = [];
        var loop = 34;
        var lastTailIndex;
        // at least 4 chunks per data page based o
        // given 128k for min data page size and 1Mb for max data size, we need 8 pages to reach the max
        // which requires 8 * 4 = 32 chunks of 30K each
        var randomString = Utils.randomString(30000);

        Async.series([
            function getBackFileSize(next) {
                bigArray.getBackFileSize(function onSize(err, size) {
                    if (err) {
                        return next(err);
                    }
                    t.equal(size.toNumber(), 0, 'size should be 0');
                    next();
                });
            },
            function maxOut(next) {
                for (var i = 0; i < loop; i++) {
                    bigArray.append(new Buffer(randomString));
                }
                t.ok(!bigArray.isEmpty(), 'array should not be empty');
                t.ok(!bigArray.isFull(), NodeUtils.format('array should not be full [tail:%s, head:%s]',
                    bigArray.arrayTailIndex, bigArray.arrayHeadIndex));
                t.ok(!bigArray.isDataFull(), NodeUtils.format('array should not be data full size: %s',
                    bigArray.dataSize()));

                bigArray.append(new Buffer(randomString));
                t.equal(backlog.length, 0, 'should have backlogged 10 chunks');
                setImmediate(function delay() {
                    t.equal(backlog.length, 10, 'should have backlogged 10 chunks');
                    next();
                });
            }
        ], function onComplete(err) {
            t.ok(!err, err && err.stack || 'no error');
            t.end();
        });

    });

    t.test('getItemLength', function (t) {
        var bigArray = new BigArray(testDir, "get_data_length_test");

		for (var i = 1; i <= 100; i++) {
			bigArray.append(new Buffer(Utils.randomString(i)));
		}

		for (i = 1; i <= 100; i++) {
			var length = bigArray.getItemLength(i - 1);
			t.equal(length.toNumber(), i);
		}

        t.end();
    });

    t.test('testInvalidDataPageSize', function (t) {
        try {
            var bigArray = new BigArray(testDir, "invalid_data_page_size", BigArray.MINIMUM_DATA_PAGE_SIZE - 1);
            t.fail("should throw invalid page size exception");
        } catch (IllegalArgumentException) {
            // expected
            t.end();
        }

    });

    t.test('testMinimumDataPageSize', function (t) {
        t.timeoutAfter(60 * 1000);
        var bigArray = new BigArray(testDir, "min_data_page_size", BigArray.MINIMUM_DATA_PAGE_SIZE);

        var randomString = Utils.randomString(BigArray.MINIMUM_DATA_PAGE_SIZE / (1024));

        for(var i = 0; i < 1024; i++) {
            bigArray.append(new Buffer(randomString));
        }

        Async.series([
            function getBackFileSize(next) {
                bigArray.getBackFileSize(function onSize(err, size) {
                    if (err) {
                        return next(err);
                    }

                    t.equal(size.toNumber(), 64 * 1024 * 1024);
                    next();
                });
            },
            function appendMore(next) {
                for(var i = 0; i < 1024 * 10; i++) {
                    bigArray.append(new Buffer(randomString));
                }
                next();
            },
            function getBackFileSize(next) {
                bigArray.getBackFileSize(function onSize(err, size) {
                    if (err) {
                        return next(err);
                    }

                    t.equal(size.toNumber(), 11 * 32 * 1024 * 1024 + 32 * 1024 * 1024);
                    next();
                });
            },
            function removeBeforeIndex(next) {
                bigArray.removeBeforeIndex(1024, next);
            },
            function getBackFileSize(next) {
                bigArray.getBackFileSize(function onSize(err, size) {
                    if (err) {
                        return next(err);
                    }

                    t.equal(size.toNumber(), 10 * 32 * 1024 * 1024 + 32 * 1024 * 1024);
                    next();
                });
            },
            function removeBeforeIndex(next) {
                bigArray.removeBeforeIndex(1024 * 2, next);
            },
            function getBackFileSize(next) {
                bigArray.getBackFileSize(function onSize(err, size) {
                    if (err) {
                        return next(err);
                    }

                    t.equal(size.toNumber(), 9 * 32 * 1024 * 1024 + 32 * 1024 * 1024);
                    next();
                });
            }
        ], function onComplete(err) {
            setImmediate(function () {
                t.ok(!err, err && err.stack || 'no error');
                t.end();
            });
        });
    });

    t.test('after', function (t) {
        rm('-rf', testDir);
        t.end();
    });

});
