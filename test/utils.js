'use strict';

var Test = require('tape');
var Utils = require('../lib/utils');

Test(__filename, function (t) {

    t.test('should get normal callback', function (t) {
        t.plan(3);
        t.timeoutAfter(500);

        var count = 0;
        var fn = Utils.timeoutFunction(asyncFn, 100);

        fn(function (err, msg) {
            t.ok(!err, 'should not get error');
            t.equal(msg, 'hello');
        });

        function asyncFn(callback) {
            setTimeout(function () {
                callback(null, 'hello');
                t.pass('callback was called');
            }, 50);
        }
    });

    t.test('should timeout callback', function (t) {
        t.plan(2);
        t.timeoutAfter(500);

        var count = 0;
        var fn = Utils.timeoutFunction(asyncFn, 50);

        fn(function (err, msg) {
            t.ok(err, 'should get error');
        });

        function asyncFn(callback) {
            setTimeout(function () {
                callback(null, 'hello');
                t.pass('callback was called');
            }, 100);
        }
    });

});
