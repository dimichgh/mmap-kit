'use strict';

var BigArray = require('../../../lib/big-array').BigArray;
var BigQueue = require('../../../lib/big-queue').BigQueue;
var Path = require('path');

var testDir = Path.resolve(__dirname, '../../', '.tmp', 'bigqueue/unit');
var bigQueue = new BigQueue(testDir, 'concurrentAdd-' + process.pid, BigArray.MINIMUM_DATA_PAGE_SIZE);

var N = 1000;
var pitem = 1;

function publisher() {
    bigQueue.enqueue(new Buffer('' + process.pid + ':' + pitem));
    process.send('publishing ' + process.pid + ':' + pitem);
    pitem++;
    if (pitem <= N) {
        setImmediate(publisher);
    }
    else {
        process.send('done');
    }
}

process.send('online:' + process.pid);

process.on('SIGTERM', function() {
	process.send('exit');
	process.exit();
});

setTimeout(function () {}, 10000);

process.on('message', function (msg) {
    if (msg === 'start') {
        process.send('publisher started, pid: ' + process.pid);
        publisher();
    }
});
