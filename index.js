var stream = require('stream');
var util = require('util');

function GetKeyOfValue (db, getopts) {
    if (!this instanceof GetKeyOfValue) {
        return new GetKeyOfValue(db, getopts);
    }
    stream.Transform.call(this, {objectMode: true});

    this.db = db;
    this.getopts = getopts;
}

util.inherits(GetKeyOfValue, stream.Transform);

(function () {

    this._transform = function (chunk, e, next) {
        this.db.get(chunk.value, this.getopts, function (err, value) {
            chunk.key = chunk.value;
            chunk.value = value;
            this.push(chunk);
            next();
        }.bind(this));
    };

}).call(GetKeyOfValue.prototype);

module.exports = {
    createPrefixReadStream: function (db, prefix, opts) {
        opts = opts || {};
        opts.start = opts.end = prefix;
        if (opts.reverse) {
            opts.start += '~';
        } else {
            opts.end += '~';
        }
        return db.createReadStream(opts);
    },

    GetKeyOfValue: GetKeyOfValue,
};
