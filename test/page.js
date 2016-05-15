'use strict';

var Fs = require('fs');
var Path = require('path');

var MMap = require('mmap.js');
var Test = require('tape');
var Bignum = require('bignum');
var mkdir = require('shelljs').mkdir;
var rm = require('shelljs').rm;
var Async = require('async');

var createByteBuffer = require('../lib/byte-buffer').create;
var Page = require('../lib/page').Page;
var PageFactory = require('../lib/page-factory').PageFactory;

var fileName = 'test-page.dat';

var testDir = Path.resolve(__dirname, '.tmp', 'mapped_page_test');



Test(__filename, function (t) {

    var fd;
    var page;
    var buffer;

    t.test('before', function (t) {
        mkdir('-p', testDir);
        t.end();
    });

    t.test('testSingleThreadPage', function (t) {
        // t.plan(10004);
        t.timeoutAfter(10000);

        var pageSize = 1024 * 1024 * 32;
        var mappedPageFactory = new PageFactory(pageSize, Path.resolve(testDir, 'test_single_thread'), {
            ttl: 2000
        });

        var mappedPage = mappedPageFactory.acquirePage(0);
        var buffer = mappedPage.getLocal(0);
        t.equal(buffer.length, pageSize, 'buffer size should be equal to page size');
        t.equal(buffer.position, 0, 'buffer position should be set to 0');

        var i;
        for(i = 0; i < 10000; i++) {
            var hello = 'hello world';
            var length = new Buffer(hello).length;
            mappedPage.getLocal(i * 20).putBuffer(new Buffer(hello));
            t.equals(mappedPage.getLocal(i * 20 , length).toString(), hello.toString());
        }

        buffer = createByteBuffer(16);
        buffer.putBigInt(Bignum(1));
        buffer.putBigInt(Bignum(2));
        buffer.putBigLong(Bignum(3));
        for(i = 0; i < 10000; i++) {
            var buf = buffer.flip();
            mappedPage.getLocal(i * 20).putBuffer(buf);
        }
        for(i = 0; i < 10000; i++) {
            var buff = mappedPage.getLocal(i * 20);
            t.equal(buff.getBigInt().toString(10), '1');
            t.equal(buff.getBigInt().toString(10), '2');
            t.equal(buff.getBigLong().toString(10), '3');
        }
        t.end();


    });

    t.test('testMultiThreadsPage', function (t) {
        t.timeoutAfter(4000);

        var pageSize = 1024 * 1024 * 32;
        var mappedPageFactory = new PageFactory(pageSize, Path.resolve(testDir, 'test_multi_threads'), {
            ttl: 2000
        });

        var threadNum = 2; //100;
   		var pageNumLimit = 5; //50;

        Async.times(threadNum, function everyTime(id, next) {
            runWorker(t, id, mappedPageFactory, pageNumLimit, function onResult(err, result) {
                t.ok(!err, err && err.stack);

                var localBufferList = result.localBufferList;
                var pageSet = result.pageSet;

                t.equal(localBufferList.length, pageNumLimit);
           		t.equal(pageSet.length, pageNumLimit);

                // verify thread locality
               for(var i = 0; i < localBufferList.length; i++) {
                   for(var j = i + 1; j < localBufferList.length; j++) {
                       t.ok(localBufferList[i] !== localBufferList[j], 'should not be equal');
                   }
               }
               next();
            });
        }, function (err) {
            t.end();
        });
    });

    t.test('after', function (t) {
        rm('-rf', testDir);
        t.end();
    });

});

function runWorker(t, id, pageFactory, pageNumLimit, callback) {

    var result = {
        pageSet: [],
        localBufferList: []
    };
    Async.times(pageNumLimit, function forEveryPage(i, next) {

        var page;
        Async.series([
            function getPage(next) {
                setTimeout(function () {
                    page = pageFactory.acquirePage(i);
                    next();
                }, parseInt(Math.random() * 100));
            },
            function process(next) {
                setTimeout(function () {
                    result.pageSet.push(page);
                    result.localBufferList.push(page.getLocal(0));
                    next();
                }, parseInt(Math.random() * 100));
            },
            function loop(next) {
                var startPosition = id * 2048;

                for(var j = 0; j < 100; j++) {
                    var helloj = "hello world " + j;
                    var length = new Buffer(helloj).length;
                    page.getLocal(startPosition + j * 20).putBuffer(new Buffer(helloj));
                    t.equals(page.getLocal(startPosition + j * 20 , length).toString(), helloj);
                }

                var buffer = createByteBuffer(16);
                buffer.putBigInt(Bignum(1));
                buffer.putBigInt(Bignum(2));
                buffer.putBigLong(Bignum(3));
                for(j = 0; j < 100; j++) {
                    var buf = buffer.flip();
                    page.getLocal(startPosition + j * 20).putBuffer(buf);
                }
                for(j = 0; j < 100; j++) {
                    var buff = page.getLocal(startPosition + j * 20);
                    t.equal(buff.getBigInt().toString(10), '1');
                    t.equal(buff.getBigInt().toString(10), '2');
                    t.equal(buff.getBigLong().toString(10), '3');
                }
                next();
            }
        ], next);
    }, function onComplete(err) {
        callback(err, result);
    });

}
