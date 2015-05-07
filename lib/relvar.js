var EventEmitter = require("events").EventEmitter;
var util = require("util");

function Relvar(spec) {
	EventEmitter.call(this);
	this.spec = spec;
	this.rows = [];
}
util.inherits(Relvar, EventEmitter);

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

Relvar.prototype.insert = function() {
	var rows = Array.prototype.slice.apply(arguments);
	var invalidRows = 0;

	rows.forEach(function(row) {
		if(!this.validate(row)) {
			invalidRows += 1;
		}
	}.bind(this));

	if(invalidRows > 0) {
		var err = new Error("could not insert "+invalidRows+" rows");
		throw err;
	} else {
		process.nextTick(function() {
			this.emit("insert", rows);
		}.bind(this));
		this.rows = this.rows.concat(rows);
		return this;
	}
};

function shallowCopy_(obj) {
	var newObj = {};
	for(var key in obj) {
		if(obj.hasOwnProperty(key)) {
			newObj[key] = obj[key];
		}
	}
	return newObj;
}

function applyExtensions_(rows, properties) {
	return rows.map(function(row) {
		var prop, rowCopy;
		rowCopy = shallowCopy_(row);
		for(prop in properties) {
			if(properties[prop].val) {
				rowCopy[prop] = properties[prop].val;
			} else if(properties[prop].func) {
				rowCopy[prop] = properties[prop].func(rowCopy);
			}
		}
		return rowCopy;
	});
}

function extend(relvar, properties) {
	var spec, prop, r;

	// @TODO ensure no conflict in property names

	// Create new spec
	spec = {};
	for(prop in relvar.spec) {
		spec[prop] = relvar.spec[prop];
	}
	for(prop in properties) {
		spec[prop] = properties[prop].type;
	}
	
	// Define how to handle changes in relvar
	r = new Relvar(spec);
	relvar.on("insert", function(rows) {
		var extendedRows = applyExtensions_(rows, properties);
		r.insert.apply(r, extendedRows);
	});
	var extendedRows = applyExtensions_(relvar.rows, properties);
	r.rows = extendedRows;
	return r;
}

function union(relvarA, relvarB) {
	var r;

	// @TODO ensure specs match

	r = new Relvar(relvarA.spec);
	relvarA.on("insert", function(rows) {
		r.insert.apply(r, rows);
	});
	relvarB.on("insert", function(rows) {
		r.insert.apply(r, rows);
	});
	r.insert.apply(r, relvarA.rows);
	r.insert.apply(r, relvarB.rows);
	return r;
}

function difference(relvarA, relvarB) {
	
}

function project(relvar, properties) {

}

function join(relvarLeft, relvarRight) {
	
}

exports.Relvar = Relvar;
exports.extend = extend;
exports.union = union;
exports.difference = difference;
exports.project = project;
exports.join = join;