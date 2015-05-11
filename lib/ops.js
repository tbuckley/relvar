var Relvar = require('./relvar');
var utils = require('./utils');

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
		var extendedRows = utils.applyExtensionsAll(rows, properties);
		r.insert(extendedRows);
	});
	relvar.on("update", function(keys, rows) {
		var extendedKeys = utils.applyExtensionsAll(keys, properties);
		var extendedRows = utils.applyExtensionsAll(rows, properties);
		r.update(extendedKeys, extendedRows);
	});
	relvar.on("remove", function(rows) {
		// No need to extend since uniqueKey is the same
		r.remove(rows);
	});

	// Initialize relvar
	relvar.index.each(function(row) {
		r.index.insert(utils.applyExtensions(row, properties));
	});
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
		var projectedRows = utils.projectAll(rows, properties);
		r.insert(projectedRows);
	});
	relvar.on("update", function(keys, rows) {
		var projectedKeys = utils.projectAll(keys, properties);
		var projectedRows = utils.projectAll(rows, properties);
		r.update(projectedKeys, projectedRows);
	});
	relvar.on("remove", function(rows) {
		// No need to project since it's the same
		r.remove.apply(r, rows);
	});

	// Initialize relvar
	relvar.index.each(function(row) {
		r.index.insert(utils.project(row, properties));
	});
	return r;
}

function union(relvarA, relvarB) {
	var r;

	// @TODO ensure specs/uniqueKeys match

	r = new Relvar(relvarA.spec, relvarA.uniqueKey);

	[relvarA, relvarB].forEach(function(base) {
		base.on("insert", function(rows) {
			r.insert(rows);
		});
		base.on("update", function(keys, rows) {
			r.update(keys, rows);
		});
		base.on("remove", function(rows) {
			r.remove(rows);
		});
		base.index.each(function(row) {
			r.index.insert(row);
		});
	});

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

exports.extend = extend;
exports.union = union;
exports.difference = difference;
exports.project = project;
exports.join = join;