'use strict';

var Test = require('tape');
var Bignum = require('bignum');

var ByteBuffer = require('..').ByteBuffer;
function toArray(buffer) {
    var arr = [];
    for (var i = 0; i < buffer.length; i++) {
        arr[i] = buffer[i];
    }
    return arr;
}

Test(__filename, function (t) {
    var buffer = new Buffer(1 + 4 + 8);
    buffer.fill(0);
    var wbuffer = ByteBuffer.extend(buffer);

    t.test('write', function (t) {
        t.equal(0, wbuffer.position);

        t.equal(buffer, wbuffer);
        t.equal(13, wbuffer.length);

        wbuffer.putUInt8(1);
        t.deepEqual([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            toArray(wbuffer), 'should have 1 byte');
        t.equal(1, wbuffer.position, 'should change offset to 1');
        wbuffer.putBigInt(Bignum(0x01020304));
        t.equal(5, wbuffer.position, 'should change offset to 5');
        t.deepEqual([1, 1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0],
            toArray(wbuffer), 'should put big int');
        wbuffer.putBigLong(Bignum('05060708090a0b0c', 16));
        t.equal(13, wbuffer.position, 'should change offset to 13');
        t.deepEqual([1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xA, 0xB, 0xC],
            toArray(wbuffer), 'should put big long');

        t.end();
    });

    t.test('read', function (t) {

        wbuffer.position = 0;

        t.equal(1, wbuffer.getUInt8(), 'should read byte');
        t.equal(1, wbuffer.position, 'should change offset to 1');
        t.equal(wbuffer.getBigInt().toString(16), '01020304', 'should read big int');
        t.equal(5, wbuffer.position, 'should change offset to 5');
        t.equal('05060708090a0b0c', wbuffer.getBigLong().toString(16), 'should read big long');
        t.equal(13, wbuffer.position, 'should change offset to 13');

        t.end();
    });
});
