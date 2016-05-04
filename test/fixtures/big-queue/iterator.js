'use strict';

var BigArray = require('../../../lib/big-array').BigArray;
var BigQueue = require('../../../lib/big-queue').BigQueue;
var Path = require('path');

var Async = require('async');

var testDir = Path.resolve(__dirname, '../../', '.tmp', 'bigqueue/multi');

var bigQueue = new BigQueue(testDir, "multi", BigArray.MINIMUM_DATA_PAGE_SIZE);

process.send('online');

process.on('SIGTERM', function() {
	process.send('exit');
	process.exit();
});

process.on('message', function (msg) {
    if (msg === 'start') {
        process.send('iterator started');
        Async.timesSeries(5, function (n, next) {
            var count = 0;
            var sb = '';
            process.send('iterating, queue size: ' + bigQueue.size());
            bigQueue.each(function iter(item, index, next) {
                setImmediate(function () {
                    if (count < 20) {
                        sb += item.toString() + ',';
                    }
                    count++;
                    next();
                });
            }, function onComplete() {
                process.send("[" + count + "] " + sb);
                setTimeout(next, 500);
            });
        }, function onComplete() {
            process.send('done');
        });
    }
});

setTimeout(function () {}, 10000);
