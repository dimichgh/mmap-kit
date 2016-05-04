'use strict';

var Cp = require('child_process');
var NodeUtils = require('util');
var Fs = require('fs');
var Path = require('path');
var Test = require('tape');
var Async = require('async');
var Bignum = require('bignum');
var mkdir = require('shelljs').mkdir;
var rm = require('shelljs').rm;
var BigArray = require('../lib/big-array').BigArray;
var BigQueue = require('../lib/big-queue').BigQueue;
var Long = require('../lib/big-array').Long;
var Utils = require('./fixtures/utils');
var debug = require('debug')('mmap-kit/big-queue/test');

var testDir = Path.resolve(__dirname, '.tmp', 'bigqueue/unit');

Test(__filename, function (t) {

    t.test('before', function (t) {
        // make sure it is cleaned if not
        rm('-rf', Path.resolve(__dirname, '.tmp', 'bigqueue'));
        t.end();
    });

    t.test('simpleTest', function (t) {

        for(var i = 1; i <= 2; i++) {

            var bigQueue = new BigQueue(testDir, "simple_test");

            for(var j = 1; j <= 3; j++) {
                t.equal(bigQueue.size().toNumber(), 0, 'queue size should be 0');
                t.ok(bigQueue.isEmpty(), 'queue should be empty');

                t.equal(bigQueue.dequeue(), undefined, 'dequeue should return undefined');
                t.equal(bigQueue.peek(), undefined, 'peek should return undefined');


                bigQueue.enqueue(new Buffer('hello'));
                t.equal(bigQueue.size().toNumber(), 1, 'queue size should be 1');
                t.ok(!bigQueue.isEmpty(), 'queue should not be empty');
                t.equal(bigQueue.peek().toString(), 'hello', 'should peek "hello"');
                t.equal(bigQueue.dequeue().toString(), 'hello', 'should dequeue "hello"');
                t.equal(bigQueue.dequeue(), undefined, 'dequeue should return undefined');

                bigQueue.enqueue(new Buffer('world'));
                bigQueue.flush();
                t.ok(bigQueue.size().toNumber(), 1, 'queue size should be 1');
                t.ok(!bigQueue.isEmpty(), 'queue should not be empty');
                t.equal(bigQueue.dequeue().toString(), 'world', 'should dequeue "world"');
                t.equal(bigQueue.dequeue(), undefined, 'dequeue should return undefined');
            }

            bigQueue.close();
        }
        t.end();
    });

    t.test('bigLoopTest', function (t) {
        var bigQueue = new BigQueue(testDir, "big_loop_test");

        var data;
        var loop = 1000;
        for(var i = 0; i < loop; i++) {
            bigQueue.enqueue(new Buffer('' + i));
            t.ok(bigQueue.size().cmp(i + 1) === 0, 'queue size should be ' + (i + 1));
            t.ok(!bigQueue.isEmpty(), 'queue should not be empty');
            data = bigQueue.peek();
            t.equal(data.toString(), '0', 'first element should be 0');
        }

        t.equal(bigQueue.size().toNumber(), loop, 'final size of queue should be ' + loop);
        t.ok(!bigQueue.isEmpty(), 'queue should not be empty');
        t.equal(data.toString(), '0', 'first element should be 0');

        bigQueue.close();

        // create a new instance on exiting queue
        bigQueue = new BigQueue(testDir, "big_loop_test");
        t.equal(bigQueue.size().toNumber(), loop, 'final size of queue should be ' + loop);
        t.ok(!bigQueue.isEmpty(), 'queue should not be empty');

        for(i = 0; i < loop; i++) {
            data = bigQueue.dequeue();
            t.equal(data.toString(), '' + i, 'element[' + i + '] should be equal to ' + i);
            t.equal(bigQueue.size().toNumber(), loop - i - 1, 'queue size should be ' + (loop - i - 1));
        }

        t.ok(bigQueue.isEmpty(), 'queue should be empty');

        bigQueue.innerArray.gc();

        bigQueue.close();

        t.end();
    });

    t.test('loopTimingTest', function (t) {
        var bigQueue = new BigQueue(testDir, "loop_timing_test");

        var loop = 10000;
        var begin = Date.now();
        for(var i = 0; i < loop; i++) {
            bigQueue.enqueue(new Buffer('' + i));
        }
        var end = Date.now();
        var timeInSeconds = parseInt((end - begin) / 1000);
        console.log('Time used to enqueue ' + loop + ' items : ' + timeInSeconds + ' seconds.');

        begin = Date.now();
        for(i = 0; i < loop; i++) {
            t.equal(bigQueue.dequeue().toString(), '' + i, 'should be equal to ' + (i-1));
        }
        end = Date.now();
        timeInSeconds = parseInt((end - begin) / 1000);
        console.log('Time used to dequeue ' + loop + ' items : ' + timeInSeconds + ' seconds.');
        t.end();
    });

    t.test('testApplyForEachDoNotChangeTheQueueSync', function (t) {
        var bigQueue = new BigQueue(testDir, 'testApplyForEachDoNotChangeTheQueueSync', BigArray.MINIMUM_DATA_PAGE_SIZE);
        bigQueue.enqueue(new Buffer('1'));
        bigQueue.enqueue(new Buffer('2'));
        bigQueue.enqueue(new Buffer('3'));

        var count = 0;
        bigQueue.eachSync(function iter(el, i) {
            t.equal(el.toString(), '' + (i.toNumber() + 1), 'should be equal to ' + (i.toNumber() + 1));
            count++;
        });
        t.equal(3, count, 'should have iterated through 3 elements');

        t.equal(bigQueue.size().toNumber(), 3, 'queue size should be 3');

        t.equal(bigQueue.dequeue().toString(), '1', 'should dequeue 1');
        t.equal(bigQueue.dequeue().toString(), '2', 'should dequeue 2');
        t.equal(bigQueue.dequeue().toString(), '3', 'should dequeue 3');

        t.equal(bigQueue.size().toNumber(), 0, 'queue size should be 0');

        t.end();
    });

    t.test('testApplyForEachDoNotChangeTheQueueAsync', function (t) {
        t.timeoutAfter(1000);

        var bigQueue = new BigQueue(testDir, 'testApplyForEachDoNotChangeTheQueueAsync', BigArray.MINIMUM_DATA_PAGE_SIZE);
        bigQueue.enqueue(new Buffer('1'));
        bigQueue.enqueue(new Buffer('2'));
        bigQueue.enqueue(new Buffer('3'));

        var count = 0;
        bigQueue.each(function iter(el, i, next) {
            setImmediate(function asyncFn() {
                t.equal(el.toString(), '' + i.add(1).toNumber(), 'should be equal to ' + i.add(1).toNumber());
                count++;
                next();
            });
        }, function onComplete() {
            t.equal(3, count, 'should have iterated through 3 elements');

            t.equal(bigQueue.size().toNumber(), 3, 'queue size should be 3');

            t.equal(bigQueue.dequeue().toString(), '1', 'should dequeue 1');
            t.equal(bigQueue.dequeue().toString(), '2', 'should dequeue 2');
            t.equal(bigQueue.dequeue().toString(), '3', 'should dequeue 3');

            t.equal(bigQueue.size().toNumber(), 0, 'queue size should be 0');

            t.end();

        });
    });

    t.test('concurrentApplyForEachTest', function (t) {
        t.timeoutAfter(60000);
        var bigQueue = new BigQueue(testDir, "concurrentApplyForEachTest", BigArray.MINIMUM_DATA_PAGE_SIZE );

        var N = 10000;
        var pitem = 1;

        function publisher(done) {
            bigQueue.enqueue(new Buffer('' + pitem));
            pitem++;
            if (pitem < N) {
                setImmediate(publisher.bind(null, done));
            }
            else {
                done();
            }
        }

        var sitem = 0;
        var count = 0;
        function subscriber(done) {
            if (bigQueue.size() > 0) {
                var bytes = bigQueue.dequeue();
                var str = bytes.toString();
                var curr = parseInt(str);
                t.equal(sitem + 1, curr);
                sitem = curr;
            }
            if (count++ < N) {
                setImmediate(subscriber.bind(null, done));
            }
            else {
                done();
            }
        }

        Async.parallel([
            subscriber,
            publisher,
            function iterate(next) {
                Async.timesSeries(N/100, function () {
                    var count = 0;
                    var sb = '';
                    bigQueue.each(function iter(item, index, next) {
                        setImmediate(function () {
                            if (count < 20) {
                                sb += item.toString() + ',';
                            }
                            count++;
                            next();
                        });
                    }, function onComplete(err) {
                        err && console.log('conflict while iterating: ', err.message); // can try to read deleted element
                        console.log("[" + count + "] " + sb);
                        next();
                    });
                });
            }
        ], function onComplete() {
            t.end();
        });
    });

    t.test('multiProcess', function (t) {
        t.timeoutAfter(60000);
        var testDir = Path.resolve(__dirname, '.tmp', 'bigqueue/multi');
        var bigQueue = new BigQueue(testDir, "multi", BigArray.MINIMUM_DATA_PAGE_SIZE);

        var N = 1000;
        var publisher;
        var iter;

        var sitem = 0;
        var count = 0;
        function subscriber(done) {
            var size = bigQueue.size();
            if (size > 0) {
                var bytes = bigQueue.dequeue();
                var str = bytes.toString();
                var curr = parseInt(str);
                t.equal(sitem + 1, curr);
                sitem = curr;
                debug('subscriber item: %s, queue size: %d', curr, bigQueue.size());
                return setImmediate(subscriber.bind(null, done));
            }
            if (sitem < N - 1) {
                setTimeout(subscriber.bind(null, done), 500);
            }
            else {
                done();
            }
        }

        Async.parallel([
            function startPublisher(next) {
                publisher = Cp.fork(Path.resolve(__dirname,
                    'fixtures/big-queue/publisher.js'));
                publisher.on('message', function (msg) {
                    if (msg === 'online') {
                        return next();
                    }
                    debug(msg);
                });
            },
            function startIter(next) {
                iter = Cp.fork(Path.resolve(__dirname,
                    'fixtures/big-queue/iterator.js'));
                iter.on('message', function (msg) {
                    if (msg === 'online') {
                        return next();
                    }
                    debug(msg);
                });

            }
        ], function onReady() {
            publisher.send('start');
            iter.send('start');

            Async.parallel([
                function waitForPublisher(next) {
                    publisher.on('message', function (msg) {
                        if (msg === 'done') {
                            next();
                        }
                    });
                },
                function waitForIter(next) {
                    iter.on('message', function (msg) {
                        if (msg === 'done') {
                            next();
                        }
                    });
                },
                function delaySubscriber(next) {
                    // setTimeout(function () {
                        subscriber(next);
                    // }, 2000);
                }
            ], function onComplete() {
                publisher.kill();
                iter.kill();
                t.end();
            });
        });
    });

    t.test('concurrentAdd', function (t) {
        t.timeoutAfter(60000);
        var bigQueues = [];

        var events = [];
        function subscriber(done) {
            if (bigQueues.length) {
                var totalQueueSize = 0;
                bigQueues.forEach(function iter(bigQueue) {
                    var size = bigQueue.size();
                    if (size > 0) {
                        var bytes = bigQueue.dequeue();
                        var str = bytes.toString();
                        events.push(str);
                        t.pass('subscriber got ' + str + ', total events: ' +
                            events.length + ', queue size: ' + bigQueue.size().toNumber());
                        totalQueueSize += size-1;
                    }
                });
                if (totalQueueSize > 0) {
                    return setImmediate(subscriber.bind(null, done));
                }
            }
            if (events.length < 2000) {
                setTimeout(subscriber.bind(null, done), 500);
            }
            else {
                done();
            }
        }

        var publishers = [];

        function startPublisher(next) {
            var publisher = Cp.fork(Path.resolve(__dirname,
                'fixtures/concurrentAdd/publisher.js'));
            publisher.on('message', function (msg) {
                if (/^online:/.test(msg)) {
                    var pid = msg.split(':')[1];
                    var bigQueue = new BigQueue(testDir, 'concurrentAdd-' + pid, BigArray.MINIMUM_DATA_PAGE_SIZE);
                    bigQueue.pid = pid;
                    bigQueues.push(bigQueue);

                    return next();
                }
                debug(msg);
            });

            publishers.push(publisher);
        }

        Async.parallel([
            startPublisher,
            startPublisher,
            startPublisher
        ], function onReady() {
            var waitFor = [];
            publishers.forEach(function forEach(publisher) {
                publisher.send('start');
                waitFor.push(function waitForPublisher(publisher, next) {
                    publisher.on('message', function (msg) {
                        if (msg === 'done') {
                            next();
                        }
                    });
                }.bind(null, publisher));
            });

            waitFor.push(function delaySubscriber(next) {
                // setTimeout(function () {
                    subscriber(next);
                // }, 15000);
            });

            Async.parallel(waitFor, function onComplete() {
                t.equal(events.length, 3000);
                publishers.forEach(function forEach(publisher) {
                    publisher.kill();
                });
                t.end();
            });
        });
    });

    t.test('after', function (t) {
        rm('-rf', testDir);
        t.end();
    });

});
