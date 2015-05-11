var RBTree = require('bintrees').RBTree;
var EventEmitter = require("events").EventEmitter;
var util = require("util");
var relvarUtils = require("./utils");

function Relvar(spec, uniqueKey) {
	EventEmitter.call(this);

	// Ensure uniqueKey is valid
	relvarUtils.validateUniqueKey(uniqueKey, spec);

	// Set basic values
	this.spec = spec;
	this.uniqueKey = uniqueKey;
	this.index = new RBTree(this.compare.bind(this));
}
util.inherits(Relvar, EventEmitter);

Relvar.prototype.compare = function(a, b) {
	var i, prop;
	for(i = 0; i < this.uniqueKey.length; i++) {
		prop = this.uniqueKey[i];
		if(a[prop] < b[prop]) {
			return -1;
		}
		if(a[prop] > b[prop]) {
			return 1;
		}
	}
	return 0;
};

Relvar.prototype.validate = function(row) {
	for(var key in this.spec) {
		if(row.hasOwnProperty(key)) {
			if(this.spec[key] === Number) {
				if(typeof row[key] !== "number") {
					return false;
				}
			} else if(this.spec[key] === String) {
				if(typeof row[key] !== "string") {
					return false;
				}
			} else if(this.spec[key] === Boolean) {
				if(typeof row[key] !== "boolean") {
					return false;
				}
			} else {
				if(!(row[key] instanceof this.spec[key])) {
					return false;
				}
			}
		} else {
			return false;
		}
	}
	return true;
};

Relvar.prototype.array = function() {
	var rows = [];
	this.index.each(function(row) {
		rows.push(row);
	});
	return rows;
};

Relvar.prototype.insert = function(rows, cb) {
	var invalidRows;
	invalidRows = 0;

	// Validate all rows
	rows.forEach(function(row) {
		if(!this.validate(row)) {
			invalidRows += 1;
		}
	}.bind(this));

	// If any invalid rows, throw an error
	// Otherwise insert the rows on the next tick
	if(invalidRows > 0) {
		var err = new Error("could not insert "+invalidRows+" rows");
		throw err;
	} else {
		process.nextTick(function() {
			rows.forEach(function(row) {
				this.index.insert(row);
			}.bind(this));
			this.emit("insert", rows);
			if(cb) {cb();}
		}.bind(this));
		return this;
	}
};

Relvar.prototype.update = function(keys, rows, cb) {
	var replacedRows, newRows, row;

	replacedRows = [];
	newRows = [];
	keys.forEach(function(key, index) {
		row = this.index.find(key);
		if(row) {
			replacedRows.push(row);
			newRows.push(rows[index]);
		}
	}.bind(this));

	if(replacedRows.length > 0) {
		process.nextTick(function() {
			replacedRows.forEach(function(row, index) {
				this.index.remove(row);
				this.index.insert(newRows[index]);
			}.bind(this));
			this.emit("update", replacedRows, newRows);
			if(cb) {cb();}
		}.bind(this));
	}
	return this;
};

Relvar.prototype.remove = function(keys, cb) {
	var removedRows, row;

	removedRows = [];
	keys.forEach(function(key) {
		var row = this.index.find(key);
		if(row) {
			removedRows.push(row);
		}
	}.bind(this));

	if(removedRows.length > 0) {
		process.nextTick(function() {
			removedRows.forEach(function(row) {
				this.index.remove(row);
			}.bind(this));
			this.emit("remove", removedRows);
			if(cb) {cb();}
		}.bind(this));
	}
	return this;
};


module.exports = Relvar;