'use strict';

var Fs = require('fs');
var MMap = require('mmap.js');
var Test = require('tape');
var Bignum = require('bignum');

var Page = require('..').Page;
var fileName = 'test-page.dat';

Test(__filename, function (t) {

    var fd;
    var page;
    var buffer;

    t.test('before', function (t) {

        Fs.existsSync(fileName) && Fs.unlinkSync(fileName);
        Fs.writeFileSync(fileName, (new Buffer(MMap.PAGE_SIZE)).fill(0));
        fd = Fs.openSync(fileName, 'r+');
        var buffer = MMap.alloc(
            MMap.PAGE_SIZE,
            MMap.PROT_READ | MMap.PROT_WRITE,
            MMap.MAP_SHARED,
            fd,
            0);

        page = new Page(buffer, fileName, 0);

        t.end();
    });

    t.test('update', function (t) {

        buffer = page.getLocal();
        buffer.fill(0);

        buffer.putBigLong(Bignum('0102030405060708', 16));
        t.equal(8, buffer.position);

        buffer.position = 0;
        var bigLong = buffer.getBigLong();
        t.equal(bigLong.toString(16), '0102030405060708', 'buffer should have long');

        t.end();
    });

    t.test('share', function (t) {
        var buffer2 = createPage().getLocal();

        var bigLong = buffer2.getBigLong();
        t.equal('0102030405060708', bigLong.toString(16), 'buffer should have long');

        buffer2.putBigInt(Bignum('0a0b0c0d', 16));
        t.equal(12, buffer2.position, 'should not have long and int');

        t.equal(buffer.getBigInt().toString(16), '0a0b0c0d', 'first buffer should have int');

        var buffer3 = createPage().getLocal();
        t.equal(buffer3.getBigLong().toString(16), '0102030405060708', 'buffer should have long');
        t.equal(buffer3.getBigInt().toString(16), '0a0b0c0d', 'buffer should have long');

        t.end();
    });

    t.test('flush', function (t) {

        page.close();
        t.end();
    });

    t.test('close', function (t) {
        page.close();
        t.end();
    });

    t.test('after', function (t) {
        t.end();
    });
});

function createPage() {
    var fd = Fs.openSync(fileName, 'r+');
    var buffer = MMap.alloc(
        MMap.PAGE_SIZE,
        MMap.PROT_READ | MMap.PROT_WRITE,
        MMap.MAP_SHARED,
        fd,
        0);
    Fs.closeSync(fd);

    return new Page(buffer, fileName, 0);
}
