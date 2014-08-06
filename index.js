var stream = require('stream');
var util = require('util');

function GetKeyOfValue (db, getopts) {
    if (!this instanceof GetKeyOfValue) {
        return new GetKeyOfValue(db, getopts);
    }
    stream.Transform.call(this, {objectMode: true});

    this.db = db;
    this.getopts = getopts;
    this.last = null;
}

util.inherits(GetKeyOfValue, stream.Transform);

(function () {

    this._transform = function (chunk, e, next) {
        this.last = chunk.key;
        var self = this;
        var key = chunk.value;
        if (this.getopts.keyValue) {
            key = key[this.getopts.keyValue];
        }
        this.db.get(key, this.getopts, function (err, value) {
            chunk.key = key;
            chunk.value = value;
            self.push(chunk);
            next();
        });
    };

}).call(GetKeyOfValue.prototype);

//==================

function OnEach(onEach) {
    this.onEach = onEach;
    stream.Writable.call(this, {objectMode: true});
}

util.inherits(OnEach, stream.Writable);

(function () {
    this._write = function (chunk, encoding, next) {
        this.onEach(chunk, next);
    };
}).call(OnEach.prototype);

module.exports = {
    createPrefixReadStream: function (db, prefix, opts) {
        opts = opts || {};
        opts.start = opts.end = prefix;
        if (opts.reverse) {
            opts.start += '!';
            opts.end += '~';
        } else {
            opts.start += '';
            opts.end += '~';
        }
        return db.createReadStream(opts);
    },

    GetKeyOfValue: GetKeyOfValue,
    OnEach: OnEach,
};
