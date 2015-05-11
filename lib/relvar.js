var EventEmitter = require("events").EventEmitter;
var util = require("util");

function Relvar(spec, uniqueKey) {
	// Ensure uniqueKey is valid
	if(!validUniqueKey_(uniqueKey)) {
		throw new Error("invalid unique key: "+JSON.stringify(uniqueKey));
	}

	// Check that uniqueKey is subset of properties
	var props = getPropertiesForSpec(spec);
	if(!arrayIsSubset_(uniqueKey, props)) {
		throw new Error("uniqueKey is not subset of properties");
	}

	EventEmitter.call(this);
	this.spec = spec;
	this.uniqueKey = uniqueKey;
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

// relvar.insert(val1, val2, ..., valN)
// relvar.insert(val1, val2, ..., valN, cb)
Relvar.prototype.insert = function() {
	var rows, invalidRows, cb;
	rows = Array.prototype.slice.apply(arguments);
	invalidRows = 0;

	// Check if there's a callback
	if(typeof rows[rows.length - 1] == "function") {
		cb = rows.pop();
	}

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
			this.rows = this.rows.concat(rows);
			this.emit("insert", rows);
			if(cb) {cb();}
		}.bind(this));
		return this;
	}
};

Relvar.prototype.update = function() {
	var rows, cb, updatedRows, updates;
	rows = Array.prototype.slice.apply(arguments);

	// Check if there's a callback
	if(typeof rows[rows.length - 1] == "function") {
		cb = rows.pop();
	}

	updatedRows = [];
	updates = [];
	this.rows.forEach(function(row, index) {
		rows.forEach(function(updatedRow, updatedIndex) {
			if(hasSameUniqueKey_(this.uniqueKey), updatedRow, row) {
				updates.push({orig: index, updated: updatedIndex});
				updatedRows.push(updatedRow);
			}
		}.bind(this));
	}.bind(this));

	if(updates.length > 0) {
		process.nextTick(function() {
			updates.forEach(function(update) {
				this.rows[update.orig] = rows[update.updated];
			}.bind(this));
			this.emit("update", updatedRows);
			if(cb) {cb();}
		}.bind(this));
	}
	return this;
};

Relvar.prototype.remove = function() {
	var keys, cb, removedRows, removedIndices;
	keys = Array.prototype.slice.apply(arguments);

	// Check if there's a callback
	if(typeof keys[keys.length - 1] == "function") {
		cb = keys.pop();
	}

	removedRows = [];
	removedIndices = [];
	this.rows.forEach(function(row, index) {
		keys.forEach(function(key) {
			if(hasSameUniqueKey_(this.uniqueKey), key, row) {
				removedRows.push(row);
				removedIndices.push(index);
			}
		}.bind(this));
	}.bind(this));

	if(removedRows.length > 0) {
		process.nextTick(function() {
			removedIndices.forEach(function(index) {
				this.rows = this.rows.splice(index, 1);
			}.bind(this));
			this.emit("remove", removedRows);
			if(cb) {cb();}
		}.bind(this));
	}
	return this;
};

function getPropertiesForSpec(spec) {
	var properties = [];
	for(var prop in spec) {
		if(spec.hasOwnProperty(prop)) {
			properties.push(prop);
		}
	}
	return properties;
}

function hasSameUniqueKey_(key, a, b) {
	for(var i = 0; i < key.length; i++) {
		var prop = key[i];
		if(a[prop] !== b[prop]) {
			return false;
		}
	}
	return true;
}

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

function project_(rows, properties) {
	return rows.map(function(row) {
		var projectedRow = {};
		properties.forEach(function(prop) {
			projectedRow[prop] = row[prop];
		});
		return projectedRow;
	});
}

function validUniqueKey_(key) {
	return key.length && key.length > 0;
}

function arrayIsSubset_(a, b) {
	var missing = 0;
	a.forEach(function(val) {
		var index = b.indexOf(val);
		if(index === -1) {
			missing += 1;
		}
	});
	return (missing === 0);
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
	r = new Relvar(spec, relvar.uniqueKey);
	relvar.on("insert", function(rows) {
		var extendedRows = applyExtensions_(rows, properties);
		r.insert.apply(r, extendedRows);
	});
	relvar.on("update", function(rows) {
		var extendedRows = applyExtensions_(rows, properties);
		r.update.apply(r, extendedRows);
	});
	relvar.on("remove", function(rows) {
		// No need to extend since uniqueKey is the same
		r.remove.apply(r, rows);
	});
	var extendedRows = applyExtensions_(relvar.rows, properties);
	r.rows = extendedRows;
	return r;
}

// @TODO factor out shared code with extend
function project(relvar, properties, optUniqueKey) {
	var spec, prop, r, uniqueKey;

	// Create new spec
	spec = {};
	properties.forEach(function(prop) {
		if(!relvar.spec.hasOwnProperty(prop)) {
			throw new Error("property '"+prop+"' not in Relvar spec");
		}
		spec[prop] = relvar.spec[prop];
	});

	// Choose a uniqueKey
	uniqueKey = optUniqueKey || relvar.uniqueKey;
	
	// Define how to handle changes in relvar
	r = new Relvar(spec, uniqueKey);
	relvar.on("insert", function(rows) {
		var projectedRows = project_(rows, properties);
		r.insert.apply(r, projectedRows);
	});
	relvar.on("update", function(rows) {
		var projectedRows = project_(rows, properties);
		r.update.apply(r, projectedRows);
	});
	relvar.on("remove", function(rows) {
		// No need to project since uniqueKey is the same
		r.remove.apply(r, rows);
	});
	var projectedRows = project_(relvar.rows, properties);
	r.rows = projectedRows;
	return r;
}

function union(relvarA, relvarB) {
	var r;

	// @TODO ensure specs/uniqueKeys match

	r = new Relvar(relvarA.spec, relvarA.uniqueKey);
	relvarA.on("insert", function(rows) {
		r.insert.apply(r, rows);
	});
	relvarA.on("update", function(rows) {
		r.update.apply(r, rows);
	});
	relvarA.on("remove", function(rows) {
		r.remove.apply(r, rows);
	});
	relvarB.on("insert", function(rows) {
		r.insert.apply(r, rows);
	});
	relvarB.on("update", function(rows) {
		r.update.apply(r, rows);
	});
	relvarB.on("remove", function(rows) {
		r.remove.apply(r, rows);
	});
	r.rows = relvarA.rows.concat(relvarB.rows);
	return r;
}

function difference(relvarA, relvarB) {
	// Question: can we tell two rows are the same by uniqueKey?
	// Answer: no, since they may have different values and
	// we aren't merging them
	
	var r;

	r = new Relvar(relvarA.spec, relvarA.uniqueKey);
	relvarA.on("insert", function(rows) {

	});
	relvarA.on("update", function(rows) {

	});
	relvarA.on("remove", function(rows) {

	});

	relvarB.on("insert", function(rows) {

	});
	relvarB.on("update", function(rows) {

	});
	relvarB.on("remove", function(rows) {

	});
}

function join(relvarLeft, relvarRight) {
	
}

exports.Relvar = Relvar;
exports.extend = extend;
exports.union = union;
exports.difference = difference;
exports.project = project;
exports.join = join;