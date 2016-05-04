'use strict';

var Fs = require('fs');
var Path = require('path');
var Test = require('tape');
var Bignum = require('bignum');
var mkdir = require('shelljs').mkdir;
var rm = require('shelljs').rm;
var Async = require('async');
var Utils = require('./fixtures/utils');

var Page = require('../lib/page').Page;
var PageFactory = require('../lib/page-factory').PageFactory;
var fileName = 'test-page.dat';

var testDir = Path.resolve(__dirname, '.tmp', 'page-factory');

Test(__filename, function (t) {

    t.test('before', function (t) {
        rm('-rf', testDir);
        t.end();
    });

    t.test('testGetBackPageFileSet', function (t) {
        t.plan(22);
        t.timeoutAfter(2000);
        var root = Path.resolve(testDir, 'test_get_backpage_fileset');
        var mappedPageFactory = new PageFactory(1024, root);
        var i;

        for (i = 0; i < 10; i++) {
            var page = mappedPageFactory.acquirePage(i);
            t.ok(page, 'should create page for index ' + i);
        }

        mappedPageFactory.getBackPageFiles(function (err, fileSet) {
            t.ok(!err, 'no error');
            t.equal(fileSet.length, 10, 'should create 10 page files');

            for(i = 0; i < 10; i++ ) {
                var fileName = Path.resolve(root, PageFactory.PAGE_FILE_NAME + '-' + i +
                    PageFactory.PAGE_FILE_SUFFIX);
                t.ok(fileSet.indexOf(fileName) !== -1, 'should create file ' + fileName);
            }

            t.end();
        });
    });

    t.test('testGetBackPageFileSize', function (t) {
        t.plan(2);
        t.timeoutAfter(2000);

        var mappedPageFactory = new PageFactory(1024 * 1024,
            Path.resolve(testDir, 'test_get_backpage_filesize'));

        for (var i = 0; i < 100; i++) {
            mappedPageFactory.acquirePage(i);
        }
        mappedPageFactory.getBackPageFileSize(function (err, size) {
            t.ok(!err, err && err.stack);

            t.equal(size.toString(), Bignum(1024 * 1024 * 100).toString());
        });
    });

    var mappedPageFactory;

    t.test('testSingleThread', function (t) {
        t.plan(229);
        t.timeoutAfter(10000);

        mappedPageFactory = new PageFactory(1024 * 1024,
            Path.resolve(testDir, 'test_single_thread'), {
                ttl: 2000
            });

        var start;

        var page = mappedPageFactory.acquirePage(0);
        t.ok(page, 'should get page');

        var page0 = mappedPageFactory.acquirePage(0);
        t.equal(page, page0);

        var page1 = mappedPageFactory.acquirePage(1);
        t.notEqual(page0, page1);

        mappedPageFactory.releasePage(0); // release first acquire
        mappedPageFactory.releasePage(0); // release second acquire

        Async.waterfall([
            function delay(next) {
                setTimeout(next, 2200); // let page0 expire
            },
            function getPage2(next) {
                t.pass('## getPage2');
                // trigger mark&sweep and purge old page0
                mappedPageFactory.acquirePage(2);
                // create a new page0
                page = mappedPageFactory.acquirePage(0);
                t.notEqual(page, page0);
                next();
            },
            function delay(next) {
                // let the async cleaner do the job
                setTimeout(next, 1000);
            },
            function validate(next) {
                t.pass('## validate');
                t.ok(!page.isClosed());
                t.ok(page0.isClosed());
                next();
            },
            function acquire100pages(next) {
                t.pass('## acquire100pages');
                for (var i = 0; i < 100; i++) {
                    var p = mappedPageFactory.acquirePage(i);
                    t.ok(p, 'should aquire page ' + i);
                }
                next();
            },
            function validate(next) {
                t.pass('## validate');
                t.equal(mappedPageFactory.getCacheSize(), 100);
                mappedPageFactory.getExistingBackFileIndexes(function onComplete(err, indexSet) {
                    t.ok(!err, err && err.stack);
                    t.equal(indexSet.length, 100);
                    indexSet = indexSet.map(function map(index) {
                        return index.toNumber();
                    });
                    for (var i = 0; i < 100; i++) {
                        t.ok(indexSet.indexOf(i) !== -1, 'should have index ' + i);
                    }
                    next();
                });
            },
            function deletePage0(next) {
                t.pass('## deletePage0');
                mappedPageFactory.deletePage(0, function onComplete(err) {
                    t.equal(mappedPageFactory.getCacheSize(), 99);
                    var indexSet = mappedPageFactory.getExistingBackFileIndexes(function onComplete(err, indexSet) {
                        t.ok(!err, err && err.stack);
                        t.equal(indexSet.length, 99);
                        next();
                    });
                });
            },
            function deletePage1(next) {
                t.pass('## deletePage1');
                mappedPageFactory.deletePage(1, function onComplete(err) {
                    t.equal(mappedPageFactory.getCacheSize(), 98);
                    var indexSet = mappedPageFactory.getExistingBackFileIndexes(function onComplete(err, indexSet) {
                        t.ok(!err, err && err.stack);
                        t.equal(indexSet.length, 98);
                        next();
                    });
                });
            },
            function deletePage50(next) {
                t.pass('## deletePage50');
                Async.timesSeries(50, function iter(n, next) {
                    if (n < 2) {
                        return next();
                    }
                    mappedPageFactory.deletePage(n, next);
                }, function onComplete(err) {
                    t.equal(mappedPageFactory.getCacheSize(), 50);
                    var indexSet = mappedPageFactory.getExistingBackFileIndexes(function onComplete(err, indexSet) {
                        t.equal(indexSet.length, 50);
                        next();
                    });
                });
            },
            function deleteAllPages(next) {
                t.pass('## deleteAllPages');
                mappedPageFactory.deleteAllPages(next);
            },
            function validate(next) {
                t.pass('## validate');
                t.equal(mappedPageFactory.getCacheSize(), 0);
                mappedPageFactory.getExistingBackFileIndexes(function (err, indexSet) {
                    t.ok(!err, err && err.stack);
                    t.equal(indexSet.length, 0);
                    next();
                });
            }
        ], function complete(err) {
            if (err) {
                t.fail(err);
            }
            t.end();
        });

    });

    t.test('testMultiThreads', function (t) {
        t.timeoutAfter(10000);

        var pageNumLimit = 100;
        var threadNum = 100;

        var mappedPageFactory = new PageFactory(1024 * 1024,
            Path.resolve(testDir, 'test_multi_threads'), {
                ttl: 2000
            });

        var sharedMap1 = testAndGetSharedMap(t, mappedPageFactory, threadNum, pageNumLimit);
        t.equal(mappedPageFactory.getCacheSize(), pageNumLimit, 'cache size should be equal to ' + pageNumLimit);

        var sharedMap2 = testAndGetSharedMap(t, mappedPageFactory, threadNum, pageNumLimit);
        t.equal(mappedPageFactory.getCacheSize(), pageNumLimit, 'cache size should be equal to ' + pageNumLimit);

        // pages in two maps should be same since they are all cached
        compareResults(t, sharedMap1, sharedMap2, true);
        sharedMap1.forEach(function forEachThreadResult(result) {
            result.forEach(function forEachPage(page) {
                t.ok(!page.isClosed(), 'page should not be closed');
            });
        });

        Async.waterfall([
            function sleep(next) {
                setTimeout(next, 2500);
            },
            function triggerMarkAndSweep(next) {
                mappedPageFactory.cache.prune();
                var page = mappedPageFactory.acquirePage(pageNumLimit + 1);
                t.equal(mappedPageFactory.getCacheSize(), 1, 'cache size should be 1');
                next();
            },
            function testAndGetSharedMap3(next) {
                var results = testAndGetSharedMap(t, mappedPageFactory, threadNum, pageNumLimit);
                t.equal(mappedPageFactory.getCacheSize(), pageNumLimit + 1, 'cache size should increase');
                compareResults(t, sharedMap1, results, false);
                sharedMap1.forEach(function forEachThreadResult(result) {
                    result.forEach(function forEachPage(page) {
                        t.ok(page.isClosed(), 'first cycle page should be closed');
                    });
                });
                results.forEach(function forEachThreadResult(result) {
                    result.forEach(function forEachPage(page) {
                        t.ok(!page.isClosed(), 'page should not be closed');
                    });
                });
                next();
            },
        ], function onComplete() {
            t.end();
        });
    });

    // t.test('deletePagesByIndex', function (t) {
    //     t.fail('should test deleting the range of pages');
    // });

    t.test('after', function (t) {
        t.pass('## clean up');
        rm('-rf', testDir);
        t.end();
    });

});

function compareResults(t, resultsA, resultsB, compResult) {
    resultsA.forEach(function forEachResultA(resultA, index) {
        var resultB = resultsB[index];
        resultA.forEach(function forEachPageA(pageA, i) {
            var pageB = resultB[i];
            t.equal(compResult, pageA === pageB, 'pageA should be ' + (compResult ? '' : 'not ') + 'equal to pageB');
        });
    });
}

function testAndGetSharedMap(t, pageFactory, threadNum, pageNumLimit) {
    // not real threads
    var results = [];
    for (var i = 0; i < threadNum; i++) {
        results.push(workerGetPages(t, pageFactory, pageNumLimit));
    }
    var pages = results[0];
    pages.forEach(function iter(page) {
        t.ok(!page.isClosed());
    });
    t.equal(pages.length, pageNumLimit, 'pages number should be equal to pageNumLimit ('+ pageNumLimit +')');
    results.slice(1).forEach(function forEach(pageArray) {
        t.equal(pageArray.length, pages.length, 'arrays should be of the same size');
        pages.forEach(function iter(page, index) {
            t.ok(page === pageArray[index], 'pages should be the same');
        });
    });

    return results;
}

function workerGetPages(t, pageFactory, pageNumLimit) {
    var pageNumList = [];
    for(var i = 0; i < pageNumLimit; i++) {
        pageNumList.push(i);
    }
    Utils.shuffle(pageNumList);

    var pages = [];

    pageNumList.forEach(function forEach(index) {
        var page = pageFactory.acquirePage(index);
        pages[index] = page;
        pageFactory.releasePage(index);
    });
    return pages;
}
