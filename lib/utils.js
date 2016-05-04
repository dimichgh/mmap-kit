'use strict';

var Assert = require('assert');
var Fs = require('fs');
var Path = require('path');

module.exports.timeoutFunction = function timeoutFunction(fn, timeout) {
    Assert.ok(timeout, 'timeout is missing');
    return function timedFn() {

        var callback = arguments[arguments.length - 1];
        arguments[arguments.length - 1] = _newCallback;

        setTimeout(function timed() {
            var err = new Error('Timeout error');
            err.code = 'ETIMEDOUT';
            _newCallback(err);
        }, timeout);

        return fn.apply(null, arguments);

        function _newCallback() {
            callback.apply(null, arguments);
            callback = function noop() {};
        }
    };
};

module.exports.isError = function isError(obj) {
    return obj instanceof Error;
};

module.exports.once = function once(fn) {
    return function once() {
        var ret = fn.apply(null, arguments);
        fn = function noop() {};
        return ret;
    };
};

function tryMkdir(dir) {
    if (!Fs.existsSync(dir)) {
        try {
            Fs.mkdirSync(dir, parseInt('0777', 8));
        }
        catch (err) {
            if (Fs.existsSync(dir)) {
                // someone already created
                return;
            }
            throw err;
        }
    }
}

function mkdirSyncRecursive(dir) {
    var baseDir = Path.dirname(dir);

    // Base dir exists, no recursion necessary
    if (Fs.existsSync(baseDir)) {
        tryMkdir(dir);
        return;
    }

    // Base dir does not exist, go recursive
    mkdirSyncRecursive(baseDir);

    // Base dir created, can create dir
    tryMkdir(dir);
}
module.exports.mkdir = mkdirSyncRecursive;
