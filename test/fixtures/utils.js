'use strict';

var AB = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';

module.exports.shuffle = function shuffle(a) {
    var j, x, i;
    for (i = a.length; i; i -= 1) {
        j = Math.floor(Math.random() * i);
        x = a[i - 1];
        a[i - 1] = a[j];
        a[j] = x;
    }
};

module.exports.randomString = function randomString(len) {
    var sb = '';
    for (var i = 0; i < len; i++) {
        sb += AB.charAt(parseInt(Math.random() * AB.length)) ;
    }
    return sb;
};
