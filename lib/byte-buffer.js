'use strict';

var Bignum = require('bignum');

var READ_METHODS = {
    readUInt8: 1,
    readUInt16LE: 2,
    readUInt16BE: 2,
    readUInt32LE: 4,
    readUInt32BE: 4,
    readInt8: 1,
    readInt16LE: 2,
    readInt16BE: 2,
    readInt32LE: 4,
    readInt32BE: 4,
    readFloatLE: 4,
    readFloatBE: 4,
    readDoubleLE: 8,
    readDoubleBE: 8
};

var WRITE_METHODS = {
    writeUInt8: 1,
    writeUInt16LE: 2,
    writeUInt16BE: 2,
    writeUInt32LE: 4,
    writeUInt32BE: 4,
    writeInt8: 1,
    writeInt16LE: 2,
    writeInt16BE: 2,
    writeInt32LE: 4,
    writeInt32BE: 4,
    writeFloatLE: 4,
    writeFloatBE: 4,
    writeDoubleLE: 8,
    writeDoubleBE: 8
};

module.exports.create = function create(size) {
    return module.exports.extend(new Buffer(size));
};

module.exports.extend = function extend(buffer) {
    if (buffer.position !== undefined) {
        // already extended
        return buffer;
    }
    buffer.position = 0;
    Object.keys(READ_METHODS).forEach(function forEach(name) {
        var newName = 'get' + name.substring(4);
        buffer[newName] = createReadFunction(name, READ_METHODS[name]);
    });
    Object.keys(WRITE_METHODS).forEach(function forEach(name) {
        var newName = 'put' + name.substring(5);
        buffer[newName] = createWriteFunction(name, WRITE_METHODS[name]);
    });
    buffer.putBuffer = function putBuffer(buffer) {
        buffer.copy(this);
        this.position += buffer.length;
    };
    // attach bignum
    buffer.putBigLong = function putBigLong(val) {
        val.toBuffer({
            endian : 'big',
            size : 8
        }).copy(this, this.position);
        this.position += 8;
    };
    buffer.putBigInt = function putBigInt(val) {
        val.toBuffer({
            endian : 'big',
            size : 4
        }).copy(this, this.position);
        this.position += 4;
    };
    buffer.getBigLong = function getBigLong() {
        var val = Bignum.fromBuffer(this.slice(this.position, this.position + 8), {
            endian: 'big',
            size : 8
        });
        this.position += 8;
        return val;
    };
    buffer.getBigInt = function getBigInt() {
        var val = Bignum.fromBuffer(this.slice(this.position, this.position + 4), {
            endian: 'big',
            size : 4
        });
        this.position += 4;
        return val;
    };
    buffer.flip = function flip() {
        return this.slice(0, this.position);
    };
    return buffer;
};

function createReadFunction(name, size) {
    return function _getFromBuffer() {
        var val = this[name](this.position);
        this.position += size;
        return val;
    };
}

function createWriteFunction(name, size) {
    return function _putFromBuffer(value) {
        this[name](value, this.position);
        this.position += size;
    };
}
