'use strict';

var BigArray = require('../../../lib/big-array').BigArray;
var BigQueue = require('../../../lib/big-queue').BigQueue;
var Path = require('path');

var testDir = Path.resolve(__dirname, '../../', '.tmp', 'bigqueue/multi');
var bigQueue = new BigQueue(testDir, "multi", BigArray.MINIMUM_DATA_PAGE_SIZE);

var N = 1000;
var pitem = 1;

function publisher() {
    bigQueue.enqueue(new Buffer('' + pitem));
    process.send('publishing ' + pitem);
    pitem++;
    if (pitem < N) {
        setImmediate(publisher);
    }
    else {
        process.send('done');
    }
}

process.send('online');

process.on('SIGTERM', function() {
	process.send('exit');
	process.exit();
});

setTimeout(function () {}, 10000);

process.on('message', function (msg) {
    if (msg === 'start') {
        process.send('publisher started');
        publisher();
    }
});
