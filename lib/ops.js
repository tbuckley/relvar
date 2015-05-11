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

		// Initialize relvar
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
	
	// Some ops can be made more efficient through 
	// RBTree.lowerBound()/RBTree.upperBound()

	var r;

	r = new Relvar(relvarA.spec, relvarA.uniqueKey);
	relvarA.on("insert", function(rows) {
		// Add any rows that aren't in B
		var filteredRows = [];
		rows.forEach(function(row) {
			if(!relvarB.index.find(row)) {
				filteredRows.push(row);
			}
		});
		r.insert(filteredRows);
	});
	relvarA.on("update", function(keys, rows) {
		var updateKeys = [], updateRows = [], insert = [], remove = [];
		// Update any rows that are in result where key is the same
		// Add any rows where key is in B but row is not
		// Remove any rows where key is not it B but row is
		keys.forEach(function(key, index) {
			var row = rows[index];
			var bHasKey = relvarB.index.find(key);
			var bHasRow = relvarB.index.find(row);
			if(bHasKey && !bHasRow) {
				// Insert
				insert.push(row);
			} else if(!bHasKey && bHasRow) {
				// Remove
				remove.push(key);
			} else if(!bHasKey && !bHasRow) {
				// Update
				updateKeys.push(key);
				updateRows.push(row);
			}
		});
		if(insert.length > 0) {
			r.insert(insert);
		}
		if(remove.length > 0) {
			r.remove(remove);
		}
		if(updateKeys.length > 0) {
			r.update(updateKeys, updateRows);
		}
	});
	relvarA.on("remove", function(rows) {
		// Remove any rows that aren't in B
		var filteredRows = [];
		rows.forEach(function(row) {
			if(!relvarB.index.find(row)) {
				filteredRows.push(row);
			}
		});
		r.remove(filteredRows);
	});

	relvarB.on("insert", function(rows) {
		// Remove any rows that are in A
		var filteredRows = [];
		rows.forEach(function(row) {
			if(relvarA.index.find(row)) {
				filteredRows.push(row);
			}
		});
		r.remove(filteredRows);
	});
	relvarB.on("update", function(keys, rows) {
		// Add any rows in A that are in keys
		// Remove any rows in A that are in rows
		var insert = [], remove = [];
		keys.each(function(key, index) {
			var row = rows[index];
			var aKey = relvarA.index.find(key);
			var aRow = relvarA.index.find(row);
			if(aKey) {
				remove.push(aKey);
			}
			if(aRow) {
				insert.push(aRow);
			}
		});
		if(insert.length > 0) {
			r.insert(insert);
		}
		if(remove.length > 0) {
			r.remove(remove);
		}
	});
	relvarB.on("remove", function(rows) {
		// Add any rows that are in A
		var filteredRows = [];
		rows.forEach(function(row) {
			if(relvarA.index.find(row)) {
				filteredRows.push(row);
			}
		});
		r.add(filteredRows);
	});

	// Initialize relvar
	var diff = utils.rbtreeDifference(relvarA.index, relvarB.index, r.compare.bind(r));
	diff.forEach(function(row) {
		r.index.insert(row);
	});
	return r;
}

function join(relvarLeft, relvarRight) {
	
}

exports.extend = extend;
exports.union = union;
exports.difference = difference;
exports.project = project;
exports.join = join;